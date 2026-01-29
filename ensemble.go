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
}

func NewEnsembleKv(directory string, N, maxBlock, maxKeys, filesie int64, createStore CreatorFunc) (*EnsembleKv, error) {
	store := &EnsembleKv{
		directory:   directory,
		N:           N,
		maxBlock:    maxBlock,
		maxKeys:     maxKeys,
		createStore: createStore,
		hashFunc:    defaultHashFunc,
		filesize:    filesie,
		substores:   syncmap.NewSyncMap[int, KvLike](),
		flushStore:  syncmap.NewSyncMap[int, bool](),
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
		subPath := filepath.Join(genPath, fmt.Sprintf("%d", i))
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

func (s *EnsembleKv) KeyHistory(key []byte) ([][]byte, error) {
	fmt.Printf("Ensemble(%p).KeyHistory: lock-free\n", s)
	if !s.running.Load() {
		return nil, fmt.Errorf("store is closed")
	}

	// Combined history from all substores
	allHistory := make([][]byte, 0)

	// Get the current hash and index for this key
	hash := s.hashFunc(key)
	index := s.hashToIndex(hash)

	// Check the natural home for this key
	if substore, ok := s.substores.Load(index); ok {
		history, err := substore.KeyHistory(key)
		if err == nil && len(history) > 0 {
			allHistory = append(allHistory, history...)
		}
	}

	// Also check all other substores in case the key was previously in a different substore
	s.substores.Range(func(i int, substore KvLike) bool {
		if i == index {
			return true // Already checked
		}
		history, err := substore.KeyHistory(key)
		if err == nil && len(history) > 0 {
			allHistory = append(allHistory, history...)
		}
		return true
	})

	return allHistory, nil
}

func (s *EnsembleKv) Keys() [][]byte {
	fmt.Printf("Ensemble(%p).Keys: lock-free\n", s)
	if !s.running.Load() {
		return nil
	}

	var keys [][]byte

	// Get keys from substores
	s.substores.Range(func(i int, substore KvLike) bool {
		subKeys := substore.Keys()
		keys = append(keys, subKeys...)
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
	if substore, ok := s.substores.Load(index); ok {
		return substore.Get(key)
	}
	return nil, fmt.Errorf("substore %d not found", index)
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
	if substore, ok := s.substores.Load(index); ok {
		if err := substore.Put(key, val); err != nil {
			return err
		}

		// Increment total key count
		s.totalKeys.Add(1)
		s.flushStore.Store(index, true)
		return nil
	}
	return fmt.Errorf("substore %d not found", index)
}

// Delete removes a key-value pair from the appropriate substore.
func (s *EnsembleKv) Delete(key []byte) error {
	fmt.Printf("Ensemble(%p).Delete: lock-free\n", s)
	if !s.running.Load() {
		return fmt.Errorf("store is closed")
	}

	hash := s.hashFunc(key)
	index := s.hashToIndex(hash)

	if substore, ok := s.substores.Load(index); ok {
		if err := substore.Delete(key); err != nil {
			return err
		}
		// Decriment total key count
		s.totalKeys.Add(-1)
		s.flushStore.Store(index, true)
		return nil
	}
	return fmt.Errorf("substore %d not found", index)
}

// Exists checks if a key exists in the appropriate substore.
func (s *EnsembleKv) Exists(key []byte) bool {
	fmt.Printf("Ensemble(%p).Exists: lock-free\n", s)
	if !s.running.Load() {
		return false
	}

	hash := s.hashFunc(key)
	index := s.hashToIndex(hash)
	if substore, ok := s.substores.Load(index); ok {
		return substore.Exists(key)
	}
	return false
}

// Flush calls Flush on all substores.
func (s *EnsembleKv) Flush() error {
	fmt.Printf("Ensemble(%p).Flush: lock-free\n", s)
	if !s.running.Load() {
		return fmt.Errorf("store is closed")
	}

	var firstErr error
	s.substores.Range(func(i int, substore KvLike) bool {
		if err := substore.Flush(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
		return true
	})
	return firstErr
}

// Close closes all substores.
func (s *EnsembleKv) Close() error {
	fmt.Printf("Ensemble(%p).Close: lock-free\n", s)
	s.running.Store(false)

	var firstErr error
	s.substores.Range(func(i int, substore KvLike) bool {
		if err := substore.Close(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
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
	fmt.Printf("Ensemble(%p).Size: lock-free\n", s)

	var total int64
	s.substores.Range(func(i int, substore KvLike) bool {
		total += substore.Size()
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
