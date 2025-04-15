package ensemblekv

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// TreeLSM represents a hierarchical LSM-tree store
type TreeLSM struct {
	DefaultOps
	directory   string  // Directory for the current store
	currentStore KvLike  // Current store, using the real kv store
	subStores   map[string]*TreeLSM // Substores for each prefix
	prefix      string	// Prefix for the current store
	isReadOnly  bool
	createStore CreatorFunc
	blockSize   int64	// Size of each block
	fileSize    int64  // MAximum file size for the current store
	level 	 int64 // Current level in the tree
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


// NewTreeLSM creates a new TreeLSM store
func NewTreeLSM(
	directory string,
	blockSize int64,
	fileSize int64,
	level int64,
	createStore CreatorFunc,

) (*TreeLSM, error) {
	store := &TreeLSM{
		directory:   directory,
		subStores:   make(map[string]*TreeLSM),
		createStore: createStore,
		blockSize:   blockSize,
		fileSize:    fileSize,
		level:	  level,
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

	// If there are any substores, set the current store to read-only
	if len(store.subStores) > 0 {
		store.isReadOnly = true
	}



	return store, nil
}

// getPrefixForKey now uses the hashed key value
func (t *TreeLSM)  getPrefixForKey(hashedKey string) string {
	if len(hashedKey) == 0 {
		return "0"
	}
	return hashedKey[:t.level+1]
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
	prefix := t.getPrefixForKey(hashedKey)
	
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
	debugf("TreeLSM: Put: %s", t.hashKey(key))
	maxValueSize := int(0.9 * float64(t.fileSize))

	if len(value) > maxValueSize {
		return fmt.Errorf("value size exceeds storage limit")
	}
	if len(key) > maxValueSize {
		return fmt.Errorf("key size exceeds storage limit")
	}


	// Hash the key first
	hashedKey := t.hashKey(key)
	debugf("TreeLSM: Put: %s, hashedKey: %s", t.hashKey(key), hashedKey)

	if t.isReadOnly {
		prefix := t.getPrefixForKey(hashedKey)
		debugf("TreeLSM: Put: %s, prefix: %s", t.hashKey(key), prefix)
		subStore, exists := t.subStores[prefix]
		if !exists {
			debugf("TreeLSM: Put: %s, no substore found for prefix %s", t.hashKey(key), prefix)
			return fmt.Errorf("no substore found for prefix %s", prefix)
		}
		return subStore.Put(key, value)
	}

	// Check if we need to split
	if t.currentStore.Size() + int64(len(value)) > int64(0.9*float64(t.fileSize)) {
		debugf("Splitting store %s because value size %v is greater than 90%% of file size %v\n", t.directory, len(value), maxValueSize)
		debugf("Splitting store %s because value size %v is greater than 90%% of file size %v\n", t.directory, len(value), maxValueSize)
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
	debugf("TreeLSM: Get: %s", t.hashKey(key))
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	// Hash the key first
	hashedKey := t.hashKey(key)
	debugf("TreeLSM: Get: %s, hashedKey: %s", t.hashKey(key), hashedKey)

	// First check substores if they exist
	prefix := t.getPrefixForKey(hashedKey)
	if subStore, exists := t.subStores[prefix]; exists {
		debugf("TreeLSM: Get: %s, substore exists for prefix: %s", t.hashKey(key), prefix)
		value, err := subStore.Get(key)
		if err == nil {
			return value, nil
		}
	}

	debugf("TreeLSM: Get: %s, key not found in substores for prefix %s", t.hashKey(key), prefix)
	// Then check current store
	value, err := t.currentStore.Get(key)
	if err == nil {
		return value, nil
	}

	debugf("TreeLSM: Get: %s, key not found in current store", t.hashKey(key))
	return nil, fmt.Errorf("key not found")
}

// split creates substores and redistributes data
func (t *TreeLSM) split() error {
	debugf("Splitting store %s into substores\n", t.directory)
	// Create subdirectories for each hex prefix
	for i := 0; i < 16; i++ {
		p := fmt.Sprintf("%s%x", t.prefix, i)
		dir := fmt.Sprintf("substore_%x", p)
		subDir := filepath.Join(t.directory, dir)
		
		subStore, err := NewTreeLSM(subDir, t.blockSize, t.fileSize, t.level+1,t.createStore)
		if err != nil {
			return fmt.Errorf("failed to create substore %s: %w", dir, err)
		}
		
		subStore.prefix = p
		t.subStores[p] = subStore
	}

	// Redistribute existing data
	_, err := t.currentStore.MapFunc(func(key, value []byte) error {
		hashedKey := t.hashKey(key)
		prefix := t.getPrefixForKey(hashedKey)
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
		prefix := t.getPrefixForKey(hashedKey)
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
	prefix := t.getPrefixForKey(hashedKey)
	if subStore, exists := t.subStores[prefix]; exists {
		if subStore.Exists(key) {
			return true
		}
	}

	// Then current store
	if t.currentStore.Exists(key) {
		return true
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
		if entry.IsDir() &&  strings.HasPrefix(entry.Name(), "substore_") {
			prefix := strings.TrimPrefix(entry.Name(), "substore_")
			subStore, err := NewTreeLSM(
				filepath.Join(t.directory, entry.Name()),
				t.blockSize,
				t.fileSize,
				t.level+1,
				t.createStore,
			)
			if err != nil {
				return fmt.Errorf("failed to load substore %s: %w", entry.Name(), err)
			}
			subStore.prefix = prefix
			t.subStores[prefix] = subStore
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