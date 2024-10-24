package ensemblekv


import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"strings"
)

const (
	dataPrefix      = "__data__:"      // Prefix for normal keys
	tombstonePrefix = "__tombstone__:" // Prefix for tombstone keys
)

type Linelsm struct {
	DefaultOps
	directory       string
	currentDir      int               // Current active directory number
	stores          []KvLike          // Open stores (newest first for reads)
	mutex           sync.Mutex        // Mutex for thread safety
	maxBlock        int               // Max block size per store
	maxKeysPerStore int               // Max keys allowed per store before rotation
	currentKeyCount int               // Current number of keys in the active store
	createStore     func(path string, blockSize int) (KvLike, error)
}

// NewLinelsm initializes the linelsm database.
func NewLinelsm(directory string, maxBlock, maxKeysPerStore int, createStore func(path string, blockSize int) (KvLike, error)) (*Linelsm, error) {
	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, err
	}

	lsm := &Linelsm{
		directory:       directory,
		maxBlock:        maxBlock,
		maxKeysPerStore: maxKeysPerStore,
		createStore:     createStore,
	}

	if err := lsm.loadMetadata(); err != nil {
		return nil, err
	}

	if err := lsm.openCurrentStore(); err != nil {
		return nil, err
	}

	return lsm, nil
}
// Exists checks if a key exists, respecting tombstones.
func (l *Linelsm) Exists(key []byte) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	tombstoneKey := tombstonePrefix + string(key)
	dataKey := dataPrefix + string(key)

	// Check for a tombstone in any store (newest to oldest)
	for _, store := range l.stores {
		if store.Exists([]byte(tombstoneKey)) {
			return false // Key is tombstoned
		}
	}

	// If no tombstone, search for the data in all stores
	for _, store := range l.stores {
		if store.Exists([]byte(dataKey)) {
			return true
		}
	}

	return false // Key not found in any store
}
// Put stores a key-value pair and triggers store rotation if needed.
func (l *Linelsm) Put(key, value []byte) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	dataKey := dataPrefix + string(key)
	if err := l.stores[0].Put([]byte(dataKey), value); err != nil {
		return err
	}

	// Increment the key count and check if rotation is needed
	l.currentKeyCount++
	if l.currentKeyCount >= l.maxKeysPerStore {
		if err := l.rotateStore(); err != nil {
			return err
		}
	}

	return nil
}

// Delete marks a key as deleted by storing a tombstone in the active store.
func (l *Linelsm) Delete(key []byte) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	tombstoneKey := tombstonePrefix + string(key)
	if err := l.stores[0].Put([]byte(tombstoneKey), nil); err != nil {
		return err
	}

	// Increment the key count and check for rotation
	l.currentKeyCount++
	if l.currentKeyCount >= l.maxKeysPerStore {
		return l.rotateStore()
	}

	return nil
}

// rotateStore creates a new store when the current one reaches the threshold.
func (l *Linelsm) rotateStore() error {
	fmt.Printf("Rotating store. Creating new store %d\n", l.currentDir+1)
	l.currentDir++
	l.currentKeyCount = 0 // Reset key count for the new store

	if err := l.openCurrentStore(); err != nil {
		return err
	}
	return l.saveMetadata()
}

// openCurrentStore opens the latest store directory and adds it to the store list.
func (l *Linelsm) openCurrentStore() error {
	dirPath := filepath.Join(l.directory, fmt.Sprintf("%d", l.currentDir))
	store, err := l.createStore(dirPath, l.maxBlock)
	if err != nil {
		return err
	}
	l.stores = append([]KvLike{store}, l.stores...) // Newest store first
	return nil
}

// Get retrieves a key's value, checking for tombstones in all stores.
func (l *Linelsm) Get(key []byte) ([]byte, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	tombstoneKey := tombstonePrefix + string(key)
	dataKey := dataPrefix + string(key)

	// Check for a tombstone in all stores (from newest to oldest)
	for _, store := range l.stores {
		if store.Exists([]byte(tombstoneKey)) {
			return nil, fmt.Errorf("key not found (tombstoned)")
		}
	}

	// If no tombstone is found, search for the data in all stores
	for _, store := range l.stores {
		value, err := store.Get([]byte(dataKey))
		if err == nil {
			return value, nil
		}
	}

	return nil, fmt.Errorf("key not found")
}

// saveMetadata writes the current state (active directory) to metadata.
func (l *Linelsm) saveMetadata() error {
	data, err := json.Marshal(l.currentDir)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(l.directory, "metadata.json"), data, 0644)
}

// loadMetadata loads the metadata to determine the latest store directory.
func (l *Linelsm) loadMetadata() error {
	metadataPath := filepath.Join(l.directory, "metadata.json")
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		if os.IsNotExist(err) {
			l.currentDir = 500000 // Start with directory 500000 if no metadata exists
			return nil
		}
		return err
	}
	return json.Unmarshal(data, &l.currentDir)
}

// Close closes all open stores.
func (l *Linelsm) Close() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	for _, store := range l.stores {
		if err := store.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Flush ensures all changes are persisted to disk, collecting any errors along the way.
func (l *Linelsm) Flush() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	var flushErrors []string

	for i, store := range l.stores {
		if err := store.Flush(); err != nil {
			flushErrors = append(flushErrors, fmt.Sprintf("store %d: %v", i, err))
		}
	}

	if len(flushErrors) > 0 {
		return fmt.Errorf("flush errors: %s", strings.Join(flushErrors, "; "))
	}

	return nil
}



// MapFunc applies the provided function to each key-value pair in all stores
// and returns a map of all non-tombstoned keys.
func (l *Linelsm) MapFunc(f func(key, value []byte) error) (map[string]bool, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	visitedTombstones := make(map[string]bool) // Track tombstoned keys
	resultKeys := make(map[string]bool)        // Store valid keys

	for _, store := range l.stores {
		_, err := store.MapFunc(func(k, v []byte) error {
			keyStr := string(k)

			// Handle tombstone keys
			if strings.HasPrefix(keyStr, tombstonePrefix) {
				visitedTombstones[strings.TrimPrefix(keyStr, tombstonePrefix)] = true
				return nil
			}

			// Handle normal data keys
			originalKey := strings.TrimPrefix(keyStr, dataPrefix)

			// Skip keys that have been tombstoned
			if visitedTombstones[originalKey] {
				return nil
			}

			// Add the key to the result map
			resultKeys[originalKey] = true

			// Apply the provided function to the key-value pair
			return f([]byte(originalKey), v)
		})
		if err != nil {
			return nil, fmt.Errorf("map function failed: %w", err)
		}
	}

	return resultKeys, nil
}