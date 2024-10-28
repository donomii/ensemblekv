package ensemblekv

import (
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


)

// This is ensemblekv, a key-value store that uses multiple sub-stores to store data. It is designed to be used with large data sets that do not fit into other kv stores, while still being able to use the features of those kv stores.

type KvLike interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, val []byte) error
	Exists(key []byte) bool
	Delete(key []byte) error
	Size() int64
	Flush() error
	Close() error
	MapFunc(f func([]byte, []byte) error) (map[string]bool, error)
	DumpIndex() error
}

type DefaultOps struct {
}

func (d *DefaultOps) DumpIndex() error {
	return nil
}



// HashFunc is the type of function used to hash keys for consistent hashing.
type HashFunc func(data []byte) uint64

// EnsembleKv represents the ensemble key-value store.
type EnsembleKv struct {
	DefaultOps
	directory   string
	substores   []KvLike
	hashFunc    HashFunc
	N           int
	maxKeys     int
	maxBlock    int
	totalKeys   int // Total number of keys across all substores
	mutex       sync.Mutex
	generation  int
	createStore func(path string, blockSize int) (KvLike, error)
}

func NewEnsembleKv(directory string, N int, maxBlock int, maxKeys int, createStore func(path string, blockSize int) (KvLike, error)) (*EnsembleKv, error) {
	store := &EnsembleKv{
		directory:   directory,
		N:           N,
		maxBlock:    maxBlock,
		maxKeys:     maxKeys,
		createStore: createStore,
		hashFunc:    defaultHashFunc,
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

	return store, nil
}

// setup initializes the store by creating the directory structure and substores.
func (s *EnsembleKv) setup() error {
	genPath := filepath.Join(s.directory, fmt.Sprintf("%d", s.generation))
	if err := os.MkdirAll(genPath, os.ModePerm); err != nil {
		return err
	}

	for i := 0; i < s.N; i++ {
		subPath := filepath.Join(genPath, fmt.Sprintf("%d", i))
		substore, err := s.createStore(subPath, s.maxBlock)
		if err != nil {
			return err
		}
		s.substores = append(s.substores, substore)
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
	return int(hash) % len(s.substores)
}

// Get retrieves a value for a given key from the appropriate substore.
func (s *EnsembleKv) Get(key []byte) ([]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	hash := s.hashFunc(key)
	index := s.hashToIndex(hash)
	//fmt.Printf("Getting key %v from substore %d\n", clampString(string(key), 100), index) //FIXME make debug log
	return s.substores[index].Get(key)
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
	s.mutex.Lock()
	defer s.mutex.Unlock()

	hash := s.hashFunc(key)
	index := s.hashToIndex(hash)
	//fmt.Printf("Putting key %v in substore %d.  %v keys in total\n", clampString(string(key), 100), index, s.totalKeys) //FIXME make debug log
	if err := s.substores[index].Put(key, val); err != nil {
		return err
	}

	// Increment total key count
	s.totalKeys = s.totalKeys + 1

	// Check if rebalance is needed
	if s.totalKeys/s.N >= s.maxKeys {
		fmt.Printf("Substores hav grown past key limit of %d\n", s.maxKeys) //FIXME make debug log
		if err := s.rebalance(); err != nil {
			return err
		}
	}

	return nil
}

// Delete removes a key-value pair from the appropriate substore.
func (s *EnsembleKv) Delete(key []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	hash := s.hashFunc(key)
	index := s.hashToIndex(hash)

	// Decriment total key count
	s.totalKeys = s.totalKeys - 1
	return s.substores[index].Delete(key)
}

// Exists checks if a key exists in the appropriate substore.
func (s *EnsembleKv) Exists(key []byte) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	hash := s.hashFunc(key)
	index := s.hashToIndex(hash)
	return s.substores[index].Exists(key)
}

// Flush calls Flush on all substores.
func (s *EnsembleKv) Flush() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, substore := range s.substores {
		if err := substore.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// Close closes all substores.
func (s *EnsembleKv) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, substore := range s.substores {
		if err := substore.Close(); err != nil {
			return err
		}
	}
	return nil
}

// rebalance redistributes keys across a new generation of substores when the current generation reaches capacity.
func (s *EnsembleKv) rebalance() error {
	//fmt.Printf("Rebalancing store with %d keys\n", s.totalKeys)
	//go fmt.Printf("Rebalancing store complete with %v keys\n", s.totalKeys)

	// Increment generation and calculate new N
	s.generation++
	s.N *= 2

	// Create new generation directory
	newGenPath := filepath.Join(s.directory, fmt.Sprintf("%d", s.generation))
	if err := os.MkdirAll(newGenPath, os.ModePerm); err != nil {
		return err
	}

	//fmt.Printf("Creating new generation %d with %d substores\n", s.generation, s.N)
	// Create new substores
	newSubstores := make([]KvLike, s.N)
	for i := 0; i < s.N; i++ {
		var err error
		subPath := filepath.Join(newGenPath, fmt.Sprintf("%d", i))
		newSubstores[i], err = s.createStore(subPath, s.maxBlock)
		if err != nil {
			return err
		}

	}

	//fmt.Printf("Rebalancing %d keys\n", s.totalKeys)
	// Re-distribute keys
	for _, substore := range s.substores {
		_, err := substore.MapFunc(func(key, value []byte) error {
			hash := s.hashFunc(key)
			newIndex := int(hash) % s.N
			//fmt.Printf("Rebalancing key %v to substore %d\n", string(key), newIndex)
			return newSubstores[newIndex].Put(key, value)
		})
		if err != nil {
			return err
		}
	}

	//fmt.Printf("Rebalancing complete\n")

	//fmt.Printf("Closing old substores\n")
	// Close and remove old substores
	oldGenPath := filepath.Join(s.directory, fmt.Sprintf("%d", s.generation-1))
	for _, substore := range s.substores {
		//fmt.Printf("Closing substore\n")
		if err := substore.Close(); err != nil {
			return fmt.Errorf("Error closing substore: %v\n", err)
		}
	}

	//fmt.Printf("Removing old generation %d\n", s.generation-1)
	//fmt.Printf("Removing old generation %v\n", oldGenPath)
	if err := os.RemoveAll(oldGenPath); err != nil {
		fmt.Printf("Error removing old generation: %v\n", err)
		return err
	}

	// Update substores to new generation
	s.substores = newSubstores

	// Save metadata after rebalancing
	return s.saveMetadata()
}

// Metadata represents the persistent state of the Store.
type Metadata struct {
	Generation int `json:"generation"`
	N          int `json:"n"`
	TotalKeys  int `json:"totalKeys"`
}

// saveMetadata writes the current store metadata to a file.
func (s *EnsembleKv) saveMetadata() error {
	metadata := Metadata{
		Generation: s.generation,
		N:          s.N,
		TotalKeys:  s.totalKeys,
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
	s.totalKeys = metadata.TotalKeys
	return nil
}

// Implementation of the MapFunc method for the Store. This method applies a function to each key-value pair in all substores.

func (s *EnsembleKv) MapFunc(f func(key []byte, value []byte) error) (map[string]bool, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return lockFreeMapFunc(s.substores, f)
}

// Implementation of the MapFunc method for the Store. This method applies a function to each key-value pair in all substores.

func lockFreeMapFunc(substores []KvLike, f func(key []byte, value []byte) error) (map[string]bool, error) {

	// Iterate over each substore
	for _, substore := range substores {
		// Use the MapFunc of each substore, passing the function 'f'
		_, err := substore.MapFunc(func(key []byte, value []byte) error {
			// Call the function 'f' for each key-value pair
			return f(key, value)
		})
		if err != nil {
			return nil, err // Return error if any occurs
		}
	}

	return nil, nil // Return nil if the operation completes successfully on all substores
}

// Size returns the total number of keys in the store.
func (s *EnsembleKv) Size() int64 {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    
    var total int64
    for _, substore := range s.substores {
        total += substore.Size()
    }
    return total
}
