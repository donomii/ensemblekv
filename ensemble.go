package ensemblekv

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	_ "log"
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
	Size() int64                                                                        // Size returns the size, in bytes, used by the store.
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
	compactingStores *syncmap.SyncMap[int, KvLike]
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
		compactingStores: syncmap.NewSyncMap[int, KvLike](),
	}

	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, err
	}

	if err := store.loadMetadata(); err != nil {
		fmt.Printf("Ensemble: Error loading shard metadata: %v\n", err)
		return nil, err
	}

	if err := store.setup(); err != nil {
		return nil, err
	}

	store.saveMetadata()

	go store.flushWorker()
	store.running.Store(true)

	return store, nil
}

func (s *EnsembleKv) flushWorker() {
	// Periodically flush the substores to disk
	for {
		time.Sleep(10 * time.Second)
		if !s.running.Load() {
			continue
		}
		s.flushStore.Range(func(i int, needsFlush bool) bool {
			if needsFlush {
				if substore, ok := s.substores.Load(i); ok {
					if err := substore.Flush(); err != nil {
						fmt.Printf("Error flushing substore %d: %v\n", i, err)
					} else {
						s.flushStore.Store(i, false)
					}
				}
			}
			return true
		})
	}
}

// setup initializes the store by creating the directory structure and substores.
func (s *EnsembleKv) setup() error {
	genPath := filepath.Join(s.directory, fmt.Sprintf("%d", s.generation))
	if err := os.MkdirAll(genPath, os.ModePerm); err != nil {
		return err
	}

	for i := int64(0); i < s.N; i++ {
		s.substoreLocks.Store(int(i), &sync.Mutex{})
		subPath := filepath.Join(genPath, fmt.Sprintf("%d", i))

		// Cleanup logic for interrupted compactions
		compactPath := subPath + ".compacting"
		oldPath := subPath + ".old"

		if _, err := os.Stat(compactPath); err == nil {
			os.RemoveAll(compactPath)
		}

		if _, err := os.Stat(oldPath); err == nil {
			// Check if primary exists and can be opened
			primaryExists := false
			if _, err := os.Stat(subPath); err == nil {
				primaryExists = true
			}

			if primaryExists {
				// Try to open it
				testStore, err := s.createStore(subPath, s.maxBlock, s.filesize)
				if err == nil {
					testStore.Close()
					os.RemoveAll(oldPath)
				} else {
					// Primary exists but is broken, revert from old
					os.RemoveAll(subPath)
					os.Rename(oldPath, subPath)
				}
			} else {
				// Primary does not exist, revert from old
				os.Rename(oldPath, subPath)
			}
		}

		substore, err := s.createStore(subPath, s.maxBlock, s.filesize)
		if err != nil {
			return err
		}
		s.substores.Store(int(i), substore)
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

// Adjusted Put method to track the total number of keys and trigger rebalance if needed.
func (s *EnsembleKv) Put(key []byte, val []byte) error {
	if !s.running.Load() {
		return fmt.Errorf("store is closed")
	}
	hash := s.hashFunc(key)
	index := s.hashToIndex(hash)

	return s.runInSubstoreWithErr(index, func(substore KvLike) error {
		if err := substore.Put(key, val); err != nil {
			return err
		}

		// Dual-write if compacting
		if compactSide, ok := s.compactingStores.Load(index); ok {
			if err := compactSide.Put(key, val); err != nil {
				return fmt.Errorf("dual-write failed for substore %d: %w", index, err)
			}
		}

		// Increment total key count
		s.totalKeys.Add(1)
		s.flushStore.Store(index, true)
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
		if err := substore.Delete(key); err != nil {
			return err
		}

		// Dual-delete if compacting
		if compactSide, ok := s.compactingStores.Load(index); ok {
			if err := compactSide.Delete(key); err != nil {
				return fmt.Errorf("dual-delete failed for substore %d: %w", index, err)
			}
		}

		// Decriment total key count
		s.totalKeys.Add(-1)
		s.flushStore.Store(index, true)
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

	var firstErr error
	var errMu sync.Mutex
	s.substores.Range(func(i int, substore KvLike) bool {
		s.runInSubstore(i, func(substore KvLike) {
			if err := substore.Flush(); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		})
		return true
	})
	return firstErr
}

// Close closes all substores.
func (s *EnsembleKv) Close() error {
	s.running.Store(false)

	var firstErr error
	var errMu sync.Mutex
	s.substores.Range(func(i int, substore KvLike) bool {
		s.runInSubstore(i, func(substore KvLike) {
			if err := substore.Close(); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		})
		return true
	})
	return firstErr
}

// Metadata represents the persistent state of the Store.
type Metadata struct {
	Generation int64 `json:"generation"`
	N          int64 `json:"n"`
	TotalKeys  int64 `json:"totalKeys"`
}

// saveMetadata writes the current store metadata to a file.
func (s *EnsembleKv) saveMetadata() error {
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
	return ioutil.WriteFile(metadataPath, data, 0644)
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

// Size returns the total number of keys in the store.
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

	genPath := filepath.Join(s.directory, fmt.Sprintf("%d", s.generation))

	for i := 0; i < int(s.N); i++ {
		subPath := filepath.Join(genPath, fmt.Sprintf("%d", i))
		compactPath := subPath + ".compacting"
		oldPath := subPath + ".old"

		// Create temporary substore
		compactStore, err := s.createStore(compactPath, s.maxBlock, s.filesize)
		if err != nil {
			return fmt.Errorf("failed to create compact store for substore %d: %w", i, err)
		}

		// Register compact store for dual-writes
		var originalStore KvLike
		err = s.runInSubstoreWithErr(i, func(substore KvLike) error {
			s.compactingStores.Store(i, compactStore)
			originalStore = substore
			return nil
		})
		if err != nil {
			compactStore.Close()
			return err
		}

		// Iterate over keys from original store
		keys := originalStore.Keys()
		for _, key := range keys {
			// Check key existence in target store -> exists -> next key
			if compactStore.Exists(key) {
				continue
			}

			// lock both source and target (via the substore lock)
			copyErr := s.runInSubstoreWithErr(i, func(substore KvLike) error {
				// check key existence in target store -> exists -> next key
				if compactStore.Exists(key) {
					return nil
				}

				// check key existence in source -> not exists -> next key
				val, err := substore.Get(key)
				if err != nil {
					return nil
				}

				// copy key and value from source to target
				if err := compactStore.Put(key, val); err != nil {
					s.compactingStores.Delete(i)
					compactStore.Close()
					os.RemoveAll(compactPath)
					return fmt.Errorf("compact copy failed for substore %d: %w", i, err)
				}
				return nil
			})
			if copyErr != nil {
				return copyErr
			}
		}

		// Swap sequence
		swapErr := s.runInSubstoreWithErr(i, func(substore KvLike) error {
			// Close both
			substore.Close()
			compactStore.Close()
			s.compactingStores.Delete(i)

			// Rename primary to old
			if err := os.Rename(subPath, oldPath); err != nil {
				// Try to restore original state by re-opening (best effort)
				newOriginal, reOpenErr := s.createStore(subPath, s.maxBlock, s.filesize)
				if reOpenErr == nil {
					s.substores.Store(i, newOriginal)
				}
				os.RemoveAll(compactPath)
				if reOpenErr != nil {
					return fmt.Errorf("failed to rename original substore %d to .old: %v, and failed to re-open original: %w", i, err, reOpenErr)
				}
				return fmt.Errorf("failed to rename original substore %d to .old: %w", i, err)
			}

			// Rename compacting to primary
			if err := os.Rename(compactPath, subPath); err != nil {
				// Try to recover original if rename failed
				os.Rename(oldPath, subPath)
				newOriginal, reOpenErr := s.createStore(subPath, s.maxBlock, s.filesize)
				if reOpenErr == nil {
					s.substores.Store(i, newOriginal)
				}
				if reOpenErr != nil {
					return fmt.Errorf("failed to rename compacting substore %d to original: %v, and failed to re-open original: %w", i, err, reOpenErr)
				}
				return fmt.Errorf("failed to rename compacting substore %d to original: %w", i, err)
			}

			// Open new substore
			newStore, err := s.createStore(subPath, s.maxBlock, s.filesize)
			if err != nil {
				// This is a bad state, but we have the .old backup.
				// Attempt to revert if possible
				os.Rename(subPath, compactPath) // move bad new back
				os.Rename(oldPath, subPath)     // restore old
				newOriginal, reOpenErr := s.createStore(subPath, s.maxBlock, s.filesize)
				if reOpenErr == nil {
					s.substores.Store(i, newOriginal)
				}
				if reOpenErr != nil {
					return fmt.Errorf("failed to open new substore %d after swap: %v, and failed to revert to original: %w.  Good luck!", i, err, reOpenErr)
				}
				return fmt.Errorf("failed to open new substore %d after swap: %w", i, err)
			}

			s.substores.Store(i, newStore)
			return nil
		})
		if swapErr != nil {
			return swapErr
		}

		// Final cleanup of .old
		os.RemoveAll(oldPath)
	}

	return nil
}
