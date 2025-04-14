package ensemblekv

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	prefixLen    = 1                 // Number of hex chars to use for prefixing (1 = 16 subdirs)
)

// TreeLSM represents a hierarchical LSM-tree store
type TreeLSM struct {
	DefaultOps
	directory   string
	currentStore KvLike
	subStores   map[string]*TreeLSM
	parent      *TreeLSM
	prefix      string
	isReadOnly  bool
	createStore CreatorFunc
	blockSize   int64
	fileSize    int64
	mutex       sync.RWMutex
	hashFunc    HashFunc // Added hash function
}

// hashKey hashes a key and returns the hex string
func (t *TreeLSM) hashKey(key []byte) string {
	// Use hash function to get uint64
	hashVal := t.hashFunc(key)
	
	// Convert to hex string
	return fmt.Sprintf("%016x", hashVal)
}

// getPrefixForKey now uses the hashed key value
func getPrefixForKey(hashedKey string) string {
	if len(hashedKey) == 0 {
		return "0"
	}
	return hashedKey[:prefixLen]
}

// NewTreeLSM creates a new TreeLSM store
func NewTreeLSM(
	directory string,
	blockSize int64,
	fileSize int64,
	createStore CreatorFunc,
) (*TreeLSM, error) {
	store := &TreeLSM{
		directory:   directory,
		subStores:   make(map[string]*TreeLSM),
		createStore: createStore,
		blockSize:   blockSize,
		fileSize:    fileSize,
		hashFunc:    defaultHashFunc, // Use the same hash function as EnsembleKV
	}

	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Create or open the current store
	current, err := createStore(filepath.Join(directory, "current"), blockSize, fileSize) //FIXME filesize
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

func (t *TreeLSM) KeyHistory(key []byte) ([][]byte, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	
	allHistory := make([][]byte, 0)
	
	// Try to get current store history
	history, err := t.currentStore.KeyHistory(key)
	if err == nil && len(history) > 0 {
		allHistory = append(allHistory, history...)
	}
	
	// Also check substores
	hashedKey := t.hashKey(key)
	prefix := getPrefixForKey(hashedKey)
	
	// Check the substore that should contain this key
	if subStore, exists := t.subStores[prefix]; exists {
		history, err := subStore.KeyHistory(key)
		if err == nil && len(history) > 0 {
			allHistory = append(allHistory, history...)
		}
	}
	
	// Also check other substores - key might have moved
	for subPrefix, subStore := range t.subStores {
		if subPrefix == prefix {
			continue // Already checked
		}
		
		history, err := subStore.KeyHistory(key)
		if err == nil && len(history) > 0 {
			allHistory = append(allHistory, history...)
		}
	}
	
	return allHistory, nil
}

// Put stores a key-value pair
func (t *TreeLSM) Put(key, value []byte) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.LockFreePut(key, value)
}
// LockFreePut stores a key-value pair without locking
func (t *TreeLSM) LockFreePut(key, value []byte) error {

	if len(value) > int(0.9*float64(t.fileSize)) {
		return fmt.Errorf("value size exceeds storage limit")
	}
	if len(key) > int(0.9*float64(t.fileSize)) {
		return fmt.Errorf("key size exceeds storage limit")
	}


	// Hash the key first
	hashedKey := t.hashKey(key)

	if t.isReadOnly {
		prefix := getPrefixForKey(hashedKey)
		subStore, exists := t.subStores[prefix]
		if !exists {
			return fmt.Errorf("no substore found for prefix %s", prefix)
		}
		return subStore.Put(key, value)
	}

	// Check if we need to split
	if t.currentStore.Size() + int64(len(value)) > int64(0.9*float64(t.fileSize)) {
		if err := t.split(); err != nil {
			return fmt.Errorf("failed to split store: %w", err)
		}
		// After splitting, we're read-only, so recursively put
		return t.LockFreePut(key, value)
	}

	return t.currentStore.Put(key, value)
}

// Get retrieves a value for a key
func (t *TreeLSM) Get(key []byte) ([]byte, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	// Hash the key first
	hashedKey := t.hashKey(key)

	// First check substores if they exist
	prefix := getPrefixForKey(hashedKey)
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
		
		subStore, err := NewTreeLSM(subDir, t.blockSize, t.fileSize,t.createStore)
		if err != nil {
			return fmt.Errorf("failed to create substore %s: %w", prefix, err)
		}
		
		subStore.prefix = prefix
		subStore.parent = t
		t.subStores[prefix] = subStore
	}

	// Redistribute existing data
	_, err := t.currentStore.MapFunc(func(key, value []byte) error {
		hashedKey := t.hashKey(key)
		prefix := getPrefixForKey(hashedKey)
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

	// Hash the key first
	hashedKey := t.hashKey(key)

	if t.isReadOnly {
		prefix := getPrefixForKey(hashedKey)
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

	// Hash the key first
	hashedKey := t.hashKey(key)

	// Check substores first
	prefix := getPrefixForKey(hashedKey)
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

// The rest of the methods (Flush, Close, Size, MapFunc) remain unchanged 
// as they don't need to deal with key hashing directly






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
				t.fileSize,
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


// isHexString checks if a string is valid hex
func isHexString(s string) bool {
	_, err := hex.DecodeString(s)
	return err == nil
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