package ensemblekv

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	maxStoreSize = 64 * 1024 * 1024 // 64MB before splitting
	prefixLen    = 1                 // Number of hex chars to use for prefixing (1 = 16 subdirs)
)

// TreeLSM represents a hierarchical LSM-tree store
type TreeLSM struct {
	DefaultOps
	directory   string
	currentStore KvLike
	subStores   map[string]*TreeLSM  // Prefix -> Store mapping
	parent      *TreeLSM
	prefix      string
	isReadOnly  bool
	createStore func(path string, blockSize int) (KvLike, error)
	blockSize   int
	mutex       sync.RWMutex
}

// NewTreeLSM creates a new TreeLSM store
func NewTreeLSM(
	directory string,
	blockSize int,
	createStore func(path string, blockSize int) (KvLike, error),
) (*TreeLSM, error) {
	store := &TreeLSM{
		directory:   directory,
		subStores:   make(map[string]*TreeLSM),
		createStore: createStore,
		blockSize:   blockSize,
	}

	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Create or open the current store
	current, err := createStore(filepath.Join(directory, "current"), blockSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}
	store.currentStore = current

	// Load existing substores if any
	if err := store.loadSubStores(); err != nil {
		return nil, fmt.Errorf("failed to load substores: %w", err)
	}

	return store, nil
}

// loadSubStores scans for and loads existing substores
func (t *TreeLSM) loadSubStores() error {
	entries, err := os.ReadDir(t.directory)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() && len(entry.Name()) == prefixLen && isHexString(entry.Name()) {
			subStore, err := NewTreeLSM(
				filepath.Join(t.directory, entry.Name()),
				t.blockSize,
				t.createStore,
			)
			if err != nil {
				return fmt.Errorf("failed to load substore %s: %w", entry.Name(), err)
			}
			subStore.prefix = entry.Name()
			subStore.parent = t
			t.subStores[entry.Name()] = subStore
		}
	}

	return nil
}

// getPrefixForKey extracts the prefix for a key
func getPrefixForKey(key []byte) string {
	if len(key) == 0 {
		return "0"
	}
	return hex.EncodeToString(key)[:prefixLen]
}

// isHexString checks if a string is valid hex
func isHexString(s string) bool {
	_, err := hex.DecodeString(s)
	return err == nil
}

// Put stores a key-value pair
func (t *TreeLSM) Put(key, value []byte) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.isReadOnly {
		prefix := getPrefixForKey(key)
		subStore, exists := t.subStores[prefix]
		if !exists {
			return fmt.Errorf("no substore found for prefix %s", prefix)
		}
		return subStore.Put(key, value)
	}

	// Check if we need to split
	if t.currentStore.Size() > maxStoreSize {
		if err := t.split(); err != nil {
			return fmt.Errorf("failed to split store: %w", err)
		}
		// After splitting, we're read-only, so recursively put
		return t.Put(key, value)
	}

	return t.currentStore.Put(key, value)
}

// Get retrieves a value for a key
func (t *TreeLSM) Get(key []byte) ([]byte, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	// First check substores if they exist
	prefix := getPrefixForKey(key)
	if subStore, exists := t.subStores[prefix]; exists {
		value, err := subStore.Get(key)
		if err == nil {
			return value, nil
		}
	}

	// Then check current store
	value, err := t.currentStore.Get(key)
	if err == nil {
		return value, nil
	}

	// Finally check parent if we have one
	if t.parent != nil {
		return t.parent.Get(key)
	}

	return nil, fmt.Errorf("key not found")
}

// split creates substores and redistributes data
func (t *TreeLSM) split() error {
	// Create subdirectories for each hex prefix
	for i := 0; i < 16; i++ {
		prefix := fmt.Sprintf("%x", i)
		subDir := filepath.Join(t.directory, prefix)
		
		subStore, err := NewTreeLSM(subDir, t.blockSize, t.createStore)
		if err != nil {
			return fmt.Errorf("failed to create substore %s: %w", prefix, err)
		}
		
		subStore.prefix = prefix
		subStore.parent = t
		t.subStores[prefix] = subStore
	}

	// Redistribute existing data
	_, err := t.currentStore.MapFunc(func(key, value []byte) error {
		prefix := getPrefixForKey(key)
		subStore := t.subStores[prefix]
		return subStore.currentStore.Put(key, value)
	})
	if err != nil {
		return fmt.Errorf("failed to redistribute data: %w", err)
	}

	// Mark current store as read-only
	t.isReadOnly = true
	return nil
}

// Delete removes a key-value pair
func (t *TreeLSM) Delete(key []byte) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.isReadOnly {
		prefix := getPrefixForKey(key)
		subStore, exists := t.subStores[prefix]
		if !exists {
			return fmt.Errorf("no substore found for prefix %s", prefix)
		}
		return subStore.Delete(key)
	}

	return t.currentStore.Delete(key)
}

// Exists checks if a key exists
func (t *TreeLSM) Exists(key []byte) bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	// Check substores first
	prefix := getPrefixForKey(key)
	if subStore, exists := t.subStores[prefix]; exists {
		if subStore.Exists(key) {
			return true
		}
	}

	// Then current store
	if t.currentStore.Exists(key) {
		return true
	}

	// Finally parent
	if t.parent != nil {
		return t.parent.Exists(key)
	}

	return false
}

// Flush ensures all data is written to disk
func (t *TreeLSM) Flush() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if err := t.currentStore.Flush(); err != nil {
		return err
	}

	for _, subStore := range t.subStores {
		if err := subStore.Flush(); err != nil {
			return err
		}
	}

	return nil
}

// Close closes all stores
func (t *TreeLSM) Close() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if err := t.currentStore.Close(); err != nil {
		return err
	}

	for _, subStore := range t.subStores {
		if err := subStore.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Size returns the total number of keys
func (t *TreeLSM) Size() int64 {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	var total int64
	total += t.currentStore.Size()

	for _, subStore := range t.subStores {
		total += subStore.Size()
	}

	return total
}

// MapFunc applies a function to all key-value pairs
func (t *TreeLSM) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	// Start with current store
	visited, err := t.currentStore.MapFunc(f)
	if err != nil {
		return nil, err
	}

	// Then process substores
	for _, subStore := range t.subStores {
		subVisited, err := subStore.MapFunc(f)
		if err != nil {
			return nil, err
		}
		// Merge visited maps
		for k, v := range subVisited {
			visited[k] = v
		}
	}

	return visited, nil
}