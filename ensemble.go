package ensemblekv

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	_ "log"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donomii/clusterF/syncmap"
)

// This is ensemblekv, a key-value store that uses multiple sub-stores to store data. It is designed to be used with large data sets that do not fit into other kv stores, while still being able to use the features of those kv stores.

type KvLike interface {
	Get(key []byte) ([]byte, error)                                                     // Get retrieves a value for a given key.  If the key does not exist, it returns nil and an error.
	Put(key []byte, val []byte) error                                                   // Put stores a key-value pair.  If the key already exists, it overwrites the value.
	Exists(key []byte) bool                                                             // Exists checks if a key exists in the store.  Returns true if the key exists, false otherwise.
	Delete(key []byte) error                                                            // Delete removes a key-value pair from the store.  Behaviour might vary by store.
	Size() int64                                                                        // Size retains backend-dependent units; use StoreMetrics for explicit counts and bytes.
	Flush() error                                                                       // Flush ensures all data is written to disk.  Behaviour might vary by store.
	Close() error                                                                       // Close closes the store and releases any resources.  The store becomes unusable after this call.
	MapFunc(f func([]byte, []byte) error) (map[string]bool, error)                      // MapFunc applies a function to all key-value pairs in the store.  The function should return an error if it fails.  Some stores may call f multiple times for the same key, with each call being a different past value (usually caused by overwrites in an LSM store).  Returns a map of keys that were processed, where the value indicates if the key exists (true) or was deleted (false).
	MapPrefixFunc(prefix []byte, f func([]byte, []byte) error) (map[string]bool, error) // MapPrefixFunc applies a function to all key-value pairs in the store that have a given prefix.  The function should return an error if it fails.  Some stores may call f multiple times for the same key, with each call being a different past value (usually caused by overwrites in an LSM store).  Returns a map of keys that were processed, where the value indicates if the key exists (true) or was deleted (false).
	DumpIndex() error                                                                   // Prints the index to stdout, for debuggins
	KeyHistory(key []byte) ([][]byte, error)                                            // Returns array of all values for a given key
	Keys() [][]byte                                                                     // Returns array of all keys
}

type DefaultOps struct {
}

func (d *DefaultOps) DumpIndex() error {
	return nil
}

func (d *DefaultOps) KeyHistory(key []byte) ([][]byte, error) {

	return [][]byte{}, nil
}

// HashFunc is the type of function used to hash keys for consistent hashing.
type HashFunc func(data []byte) uint64

// EnsembleKv represents the ensemble key-value store.
type EnsembleKv struct {
	DefaultOps
	directory   string
	substores   *syncmap.SyncMap[int, KvLike]
	flushStore  *syncmap.SyncMap[int, bool]
	hashFunc    HashFunc
	N           int64
	maxKeys     int64
	maxBlock    int64
	totalKeys   atomic.Int64 // Total number of keys across all substores
	generation  int64
	createStore CreatorFunc
	filesize    int64
	running     atomic.Bool

	compactMu        sync.Mutex
	substoreLocks    *syncmap.SyncMap[int, *sync.Mutex]
	compactingStores *syncmap.SyncMap[int, *compactionState]
	unavailable      *syncmap.SyncMap[int, error]
	countErrors      *syncmap.SyncMap[int, error]
	metadataMu       sync.Mutex
	stopWorker       chan struct{}
	workerDone       chan struct{}
}

func NewEnsembleKv(directory string, N, maxBlock, maxKeys, filesie int64, createStore CreatorFunc) (*EnsembleKv, error) {
	store := &EnsembleKv{
		directory:        directory,
		N:                N,
		maxBlock:         maxBlock,
		maxKeys:          maxKeys,
		createStore:      createStore,
		hashFunc:         defaultHashFunc,
		filesize:         filesie,
		substores:        syncmap.NewSyncMap[int, KvLike](),
		flushStore:       syncmap.NewSyncMap[int, bool](),
		substoreLocks:    syncmap.NewSyncMap[int, *sync.Mutex](),
		compactingStores: syncmap.NewSyncMap[int, *compactionState](),
		unavailable:      syncmap.NewSyncMap[int, error](),
		countErrors:      syncmap.NewSyncMap[int, error](),
		stopWorker:       make(chan struct{}),
		workerDone:       make(chan struct{}),
	}

	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, err
	}

	if err := store.loadMetadata(); err != nil {
		fmt.Printf("Ensemble: Error loading shard metadata: %v\n", err)
		return nil, err
	}

	if store.N <= 0 {
		return nil, fmt.Errorf("Ensemble substore count must be positive, received %d", store.N)
	}
	if err := store.setup(); err != nil {
		return nil, errors.Join(err, store.closeSubstores())
	}
	if err := store.saveMetadata(); err != nil {
		return nil, errors.Join(err, store.closeSubstores())
	}

	store.running.Store(true)
	go store.flushWorker()

	return store, nil
}

func (s *EnsembleKv) flushWorker() {
	// Periodically flush the substores to disk
	defer close(s.workerDone)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopWorker:
			return
		case <-ticker.C:
			s.flushStore.Range(func(id int, _ bool) bool {
				err := s.runInSubstoreWithErr(id, func(substore KvLike) error {
					if dirty, _ := s.flushStore.Load(id); !dirty {
						return nil
					}
					if err := substore.Flush(); err != nil {
						return err
					}
					s.flushStore.Store(id, false)
					return nil
				})
				if err != nil && s.running.Load() {
					logEnsembleFlushError(id, err)
				}
				return true
			})
		}
	}
}

// setup initializes the store by creating the directory structure and substores.
func (s *EnsembleKv) setup() error {
	s.totalKeys.Store(0)
	genPath := filepath.Join(s.directory, fmt.Sprintf("%d", s.generation))
	if err := os.MkdirAll(genPath, os.ModePerm); err != nil {
		return err
	}

	for i := int64(0); i < s.N; i++ {
		s.substoreLocks.Store(int(i), &sync.Mutex{})
		subPath := filepath.Join(genPath, fmt.Sprintf("%d", i))

		// Cleanup logic for interrupted compactions
		if err := s.recoverCompaction(subPath); err != nil {
			return fmt.Errorf("recover substore %d: %w", i, err)
		}

		substore, err := s.createStore(subPath, s.maxBlock, s.filesize)
		if err != nil {
			return err
		}
		s.substores.Store(int(i), substore)
		count, err := StoreCount(substore)
		if err != nil {
			return fmt.Errorf("count existing keys in substore %d: %w", i, err)
		}
		if count < 0 || s.totalKeys.Load() > math.MaxInt64-count {
			return fmt.Errorf("invalid key count from substore %d: received %d after %d keys; expected a nonnegative total through %d", i, count, s.totalKeys.Load(), int64(math.MaxInt64))
		}
		s.totalKeys.Add(count)
	}

	return nil
}

func defaultHashFunc(data []byte) uint64 {
	bi := big.NewInt(0)
	h := md5.New()
	h.Write(data)
	hexstr := hex.EncodeToString(h.Sum(nil))
	bi.SetString(hexstr, 16)

	value := bi.Int64()
	if value < 0 {
		value = value * -1
	}
	return uint64(value)
}

// hashToIndex maps a hash value to an index of a substore.
func (s *EnsembleKv) hashToIndex(hash uint64) int {
	return int(hash) % int(s.N)
}

func (s *EnsembleKv) runInSubstore(id int, f func(substore KvLike)) bool {
	lock, ok := s.substoreLocks.Load(id)
	if !ok {
		return false
	}
	lock.Lock()
	defer lock.Unlock()
	if !s.running.Load() {
		return false
	}
	if _, failed := s.unavailable.Load(id); failed {
		return false
	}
	if substore, ok := s.substores.Load(id); ok {
		f(substore)
		return true
	}
	return false
}

func (s *EnsembleKv) runInSubstoreWithErr(id int, f func(substore KvLike) error) error {
	lock, ok := s.substoreLocks.Load(id)
	if !ok {
		return fmt.Errorf("lock for substore %d not found", id)
	}
	lock.Lock()
	defer lock.Unlock()
	if !s.running.Load() {
		return fmt.Errorf("store is closed")
	}
	if err, failed := s.unavailable.Load(id); failed {
		return fmt.Errorf("substore %d is unavailable: %w", id, err)
	}
	if substore, ok := s.substores.Load(id); ok {
		return f(substore)
	}
	return fmt.Errorf("substore %d not found", id)
}

func (s *EnsembleKv) KeyHistory(key []byte) ([][]byte, error) {
	if !s.running.Load() {
		return nil, fmt.Errorf("store is closed")
	}

	// Combined history from all substores
	allHistory := make([][]byte, 0)
	var historyMu sync.Mutex

	// Get the current hash and index for this key
	hash := s.hashFunc(key)
	index := s.hashToIndex(hash)

	// Check the natural home for this key
	s.runInSubstore(index, func(substore KvLike) {
		history, err := substore.KeyHistory(key)
		if err == nil && len(history) > 0 {
			historyMu.Lock()
			allHistory = append(allHistory, history...)
			historyMu.Unlock()
		}
	})

	// Also check all other substores in case the key was previously in a different substore
	s.substores.Range(func(i int, substore KvLike) bool {
		if i == index {
			return true // Already checked
		}
		s.runInSubstore(i, func(substore KvLike) {
			history, err := substore.KeyHistory(key)
			if err == nil && len(history) > 0 {
				historyMu.Lock()
				allHistory = append(allHistory, history...)
				historyMu.Unlock()
			}
		})
		return true
	})

	return allHistory, nil
}

func (s *EnsembleKv) Keys() [][]byte {
	if !s.running.Load() {
		return nil
	}

	var keys [][]byte
	var keysMu sync.Mutex

	// Get keys from substores
	s.substores.Range(func(i int, substore KvLike) bool {
		s.runInSubstore(i, func(substore KvLike) {
			subKeys := substore.Keys()
			keysMu.Lock()
			keys = append(keys, subKeys...)
			keysMu.Unlock()
		})
		return true
	})

	return keys
}

// Get retrieves a value for a given key from the appropriate substore.
func (s *EnsembleKv) Get(key []byte) ([]byte, error) {
	if !s.running.Load() {
		return nil, fmt.Errorf("store is closed")
	}
	hash := s.hashFunc(key)
	index := s.hashToIndex(hash)

	var result []byte
	err := s.runInSubstoreWithErr(index, func(substore KvLike) error {
		var err error
		result, err = substore.Get(key)
		return err
	})
	return result, err
}

func clampString(in string, length int) string {
	if len(in) > length {
		return in[:length]
	}
	return in
}

// Put stores a key-value pair in the appropriate substore.

func (s *EnsembleKv) Put(key []byte, val []byte) error {
	if !s.running.Load() {
		return fmt.Errorf("store is closed")
	}
	hash := s.hashFunc(key)
	index := s.hashToIndex(hash)

	return s.runInSubstoreWithErr(index, func(substore KvLike) error {
		state, compacting := s.compactingStores.Load(index)
		if err := s.countMutation(index, substore, key, func() error { return substore.Put(key, val) }); err != nil {
			if compacting {
				state.failure = errors.Join(state.failure, err)
			}
			return err
		}
		if compacting && state.failure == nil {
			if err := state.keys.set(key, compactionCopied); err != nil {
				state.failure = err
				return err
			}
			if err := state.target.Put(key, val); err != nil {
				state.failure = fmt.Errorf("dual-write failed for substore %d, key %x: %w", index, key, err)
				return state.failure
			}
		}
		return nil
	})
}

// Delete removes a key-value pair from the appropriate substore.
func (s *EnsembleKv) Delete(key []byte) error {
	if !s.running.Load() {
		return fmt.Errorf("store is closed")
	}

	hash := s.hashFunc(key)
	index := s.hashToIndex(hash)

	return s.runInSubstoreWithErr(index, func(substore KvLike) error {
		state, compacting := s.compactingStores.Load(index)
		if err := s.countMutation(index, substore, key, func() error { return substore.Delete(key) }); err != nil {
			if compacting {
				state.failure = errors.Join(state.failure, err)
			}
			return err
		}
		if compacting && state.failure == nil {
			previous, err := state.keys.get(key)
			if err == nil {
				err = state.keys.set(key, compactionDeleted)
			}
			if err != nil {
				state.failure = err
				return err
			}
			if previous == compactionCopied {
				if err := state.target.Delete(key); err != nil {
					state.failure = fmt.Errorf("dual-delete failed for substore %d, key %x: %w", index, key, err)
					return state.failure
				}
			}
		}
		return nil
	})
}

// Exists checks if a key exists in the appropriate substore.
func (s *EnsembleKv) Exists(key []byte) bool {
	if !s.running.Load() {
		return false
	}

	hash := s.hashFunc(key)
	index := s.hashToIndex(hash)

	var exists bool
	s.runInSubstore(index, func(substore KvLike) {
		exists = substore.Exists(key)
	})
	return exists
}

// Flush calls Flush on all substores.
func (s *EnsembleKv) Flush() error {
	if !s.running.Load() {
		return fmt.Errorf("store is closed")
	}
	var result error
	for id := 0; id < int(s.N); id++ {
		err := s.runInSubstoreWithErr(id, func(substore KvLike) error {
			if err := substore.Flush(); err != nil {
				return err
			}
			s.flushStore.Store(id, false)
			return nil
		})
		if err != nil {
			result = errors.Join(result, fmt.Errorf("flush substore %d: %w", id, err))
		}
	}
	return errors.Join(result, s.saveMetadata())
}

// Close closes all substores.
func (s *EnsembleKv) Close() error {
	s.compactMu.Lock()
	defer s.compactMu.Unlock()
	if !s.running.Swap(false) {
		return nil
	}
	close(s.stopWorker)
	<-s.workerDone
	return errors.Join(s.closeSubstores(), s.saveMetadata())
}

func (s *EnsembleKv) closeSubstores() error {
	var result error
	s.substores.Range(func(id int, substore KvLike) bool {
		lock, _ := s.substoreLocks.Load(id)
		lock.Lock()
		defer lock.Unlock()
		if err := substore.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("close substore %d: %w", id, err))
		}
		return true
	})
	return result
}

// Metadata represents the persistent state of the Store.
type Metadata struct {
	Generation int64 `json:"generation"`
	N          int64 `json:"n"`
	TotalKeys  int64 `json:"totalKeys"`
}

// saveMetadata writes the current store metadata to a file.
func (s *EnsembleKv) saveMetadata() error {
	s.metadataMu.Lock()
	defer s.metadataMu.Unlock()
	metadata := Metadata{
		Generation: s.generation,
		N:          s.N,
		TotalKeys:  s.totalKeys.Load(),
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	metadataPath := filepath.Join(s.directory, "metadata.json")
	mode := os.FileMode(0644)
	if info, err := os.Stat(metadataPath); err == nil {
		mode = info.Mode().Perm()
	} else if !os.IsNotExist(err) {
		return err
	}
	file, err := os.CreateTemp(s.directory, ".ensemble-metadata-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if err := file.Chmod(mode); err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), metadataPath); err != nil {
		return err
	}
	return syncCompactionPath(s.directory)
}

// loadMetadata reads the store metadata from a file.
func (s *EnsembleKv) loadMetadata() error {
	metadataPath := filepath.Join(s.directory, "metadata.json")
	data, err := ioutil.ReadFile(metadataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No metadata exists, likely new store
		}
		return err
	}
	var metadata Metadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return err
	}
	s.generation = metadata.Generation
	s.N = metadata.N
	s.totalKeys.Store(metadata.TotalKeys)
	return nil
}

// Implementation of the MapFunc method for the Store. This method applies a function to each key-value pair in all substores.

func (s *EnsembleKv) MapFunc(f func(key []byte, value []byte) error) (map[string]bool, error) {
	// Snapshot keys first to avoid holding lock during callback
	keys := s.Keys() // Keys() handles its own locking

	visited := make(map[string]bool)

	for _, key := range keys {
		// Get value for each key (thread-safe, handles hashing internally)
		val, err := s.Get(key)
		if err != nil {
			// Key might have been deleted concurrently
			continue
		}

		visited[string(key)] = true

		// Call user function without holding ensemble lock
		if err := f(key, val); err != nil {
			return visited, err
		}
	}

	return visited, nil
}

// Size sums the legacy backend-dependent Size results; use Count or SizeBytes for explicit units.
func (s *EnsembleKv) Size() int64 {

	var total int64
	var sizeMu sync.Mutex
	s.substores.Range(func(i int, substore KvLike) bool {
		s.runInSubstore(i, func(substore KvLike) {
			size := substore.Size()
			sizeMu.Lock()
			total += size
			sizeMu.Unlock()
		})
		return true
	})
	return total
}

func (s *EnsembleKv) MapPrefixFunc(prefix []byte, f func([]byte, []byte) error) (map[string]bool, error) {
	// Snapshot keys first
	keys := s.Keys() // Keys() handles its own locking

	visited := make(map[string]bool)

	for _, key := range keys {
		if !bytes.HasPrefix(key, prefix) {
			continue
		}

		val, err := s.Get(key)
		if err != nil {
			continue
		}

		visited[string(key)] = true

		if err := f(key, val); err != nil {
			return visited, err
		}
	}

	return visited, nil
}

func (s *EnsembleKv) ConcurrentCompact() error {
	s.compactMu.Lock()
	defer s.compactMu.Unlock()
	if !s.running.Load() {
		return fmt.Errorf("store is closed")
	}
	for id := 0; id < int(s.N); id++ {
		if err := s.compactSubstore(id); err != nil {
			return fmt.Errorf("compact substore %d: %w", id, err)
		}
	}
	return nil
}
