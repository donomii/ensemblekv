package ensemblekv

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// TreeLSM represents a hierarchical LSM-tree store
type TreeLSM struct {
	DefaultOps
	directory    string              // Directory for the current store
	currentStore KvLike              // Current store, using the real kv store
	subStores    map[string]*TreeLSM // Substores for each prefix
	prefix       string              // Prefix for the current store
	isReadOnly   bool
	closed       bool
	splitFailure error
	createStore  CreatorFunc
	blockSize    int64 // Size of each block
	fileSize     int64 // MAximum file size for the current store
	level        int64 // Current level in the tree
	mutex        sync.RWMutex
	hashFunc     HashFunc // Added hash function
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
	return newTreeLSM(directory, blockSize, fileSize, level, createStore, "")
}

func newTreeLSM(directory string, blockSize, fileSize, level int64, createStore CreatorFunc, prefix string) (*TreeLSM, error) {
	store := &TreeLSM{
		directory:   directory,
		subStores:   make(map[string]*TreeLSM),
		createStore: createStore,
		blockSize:   blockSize,
		fileSize:    fileSize,
		level:       level,
		prefix:      prefix,
		hashFunc:    defaultHashFunc, // Use the same hash function as EnsembleKV
	}

	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}
	if _, err := inspectSplit(directory, store.splitLayout()); err != nil {
		return nil, err
	}

	// Create or open the current store
	current, err := openSplitCurrent(directory, blockSize, fileSize, createStore)
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}
	store.currentStore = current

	// Load existing substores if any
	if err := store.loadSubStores(); err != nil {
		return nil, errors.Join(fmt.Errorf("failed to load substores: %w", err), store.Close())
	}

	// If there are any substores, set the current store to read-only
	if len(store.subStores) > 0 {
		store.isReadOnly = true
	}

	return store, nil
}

// getPrefixForKey now uses the hashed key value
func (t *TreeLSM) getPrefixForKey(hashedKey string) string {
	if len(hashedKey) == 0 {
		return "0"
	}
	if t.level >= int64(len(hashedKey)) {
		return hashedKey
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

func (t *TreeLSM) Keys() [][]byte {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	var keys [][]byte
	if !t.isReadOnly {
		return t.currentStore.Keys()
	}
	for _, subStore := range t.subStores {
		subKeys := subStore.Keys()
		keys = append(keys, subKeys...)
	}
	return keys
}

// Put stores a key-value pair
func (t *TreeLSM) Put(key, value []byte) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.LockFreePut(key, value)
}

// LockFreePut stores a key-value pair without locking
func (t *TreeLSM) LockFreePut(key, value []byte) error {
	if err := t.available(); err != nil {
		return err
	}
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
	shouldSplit, err := splitNeeded(t.currentStore, key, value, int64(0.9*float64(t.fileSize)))
	if err != nil {
		return err
	}
	if shouldSplit && t.level < 16 {
		logTreeSplit(t.directory, maxValueSize, key, value)
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
	if err := t.available(); err != nil {
		return nil, err
	}

	// Hash the key first
	hashedKey := t.hashKey(key)
	debugf("TreeLSM: Get: %s, hashedKey: %s", t.hashKey(key), hashedKey)

	// First check substores if they exist
	prefix := t.getPrefixForKey(hashedKey)
	if subStore, exists := t.subStores[prefix]; exists {
		debugf("TreeLSM: Get: %s, substore exists for prefix: %s", t.hashKey(key), prefix)
		return subStore.Get(key)
	}
	if t.isReadOnly {
		return nil, fmt.Errorf("TreeLSM store %s is missing committed child %s", t.directory, prefix)
	}

	debugf("TreeLSM: Get: %s, key not found in substores for prefix %s", t.hashKey(key), prefix)
	// Then check current store
	return t.currentStore.Get(key)
}

// split creates substores and redistributes data
func (t *TreeLSM) split() (result error) {
	debugf("Splitting store %s into substores\n", t.directory)
	if err := t.available(); err != nil {
		return err
	}
	if t.isReadOnly {
		return fmt.Errorf("store %s already has a committed split", t.directory)
	}
	if t.level >= 16 {
		return fmt.Errorf("cannot split store %q beyond the 16-character hash", t.directory)
	}
	path, err := prepareSplit(t.directory, t.splitLayout(), t.currentStore)
	if err != nil {
		return err
	}
	children := make(map[string]*TreeLSM)
	defer func() {
		for _, child := range children {
			result = errors.Join(result, child.Close())
		}
	}()
	// Create subdirectories for each hex prefix
	for i := 0; i < 16; i++ {
		p := fmt.Sprintf("%s%x", t.prefix, i)
		dir := fmt.Sprintf("substore_%x", p)
		subDir := filepath.Join(path, dir)

		subStore, err := newTreeLSM(subDir, t.blockSize, t.fileSize, t.level+1, t.createStore, p)
		if err != nil {
			return fmt.Errorf("failed to create substore %s: %w", dir, err)
		}

		children[p] = subStore
	}

	// Redistribute existing data
	err = copySplit(t.currentStore, path, func(key []byte) KvLike {
		hashedKey := t.hashKey(key)
		prefix := t.getPrefixForKey(hashedKey)
		if child := children[prefix]; child != nil {
			return child.currentStore
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to redistribute data: %w", err)
	}
	for _, child := range children {
		if err := child.Flush(); err != nil {
			return err
		}
	}
	var closeErr error
	for _, child := range children {
		closeErr = errors.Join(closeErr, child.Close())
	}
	children = nil
	if closeErr != nil {
		return closeErr
	}
	published, err := publishSplit(t.directory)
	if err != nil {
		if published {
			t.splitFailure = err
		}
		return err
	}
	if err := t.loadSubStores(); err != nil {
		t.splitFailure = err
		return err
	}

	// Mark current store as read-only
	t.isReadOnly = true
	return nil
}

// Delete removes a key-value pair
func (t *TreeLSM) Delete(key []byte) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if err := t.available(); err != nil {
		return err
	}

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
	if t.available() != nil {
		return false
	}

	// Hash the key first
	hashedKey := t.hashKey(key)

	// Check substores first
	prefix := t.getPrefixForKey(hashedKey)
	if subStore, exists := t.subStores[prefix]; exists {
		return subStore.Exists(key)
	}
	if t.isReadOnly {
		return false
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
	path, err := inspectSplit(t.directory, t.splitLayout())
	if err != nil {
		return err
	}
	if path == "" {
		return nil
	}
	for partition := 0; partition < 16; partition++ {
		prefix := fmt.Sprintf("%s%x", t.prefix, partition)
		childPath := filepath.Join(path, splitChildName("tree", t.prefix, partition))
		subStore, err := newTreeLSM(childPath, t.blockSize, t.fileSize, t.level+1, t.createStore, prefix)
		if err != nil {
			return fmt.Errorf("failed to load substore %s: %w", childPath, err)
		}
		t.subStores[prefix] = subStore
	}

	return nil
}

// isHexString checks if a string is valid hex
func isHexString(s string) bool {
	return s != "" && strings.Trim(s, "0123456789abcdef") == ""
}

// Flush ensures all data is written to disk
func (t *TreeLSM) Flush() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if err := t.available(); err != nil {
		return err
	}

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

	if t.closed {
		return nil
	}
	t.closed = true
	err := t.currentStore.Close()
	for _, subStore := range t.subStores {
		err = errors.Join(err, subStore.Close())
	}
	return err
}

func (t *TreeLSM) splitLayout() splitLayout {
	return splitLayout{Version: 1, Kind: "tree", Prefix: t.prefix, Depth: t.level}
}

func (t *TreeLSM) available() error {
	if t.closed {
		return fmt.Errorf("TreeLSM store %s is closed", t.directory)
	}
	if t.splitFailure != nil {
		return fmt.Errorf("TreeLSM store %s needs reopening after split publication failed: %w", t.directory, t.splitFailure)
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
	visited := make(map[string]bool)
	for _, key := range t.Keys() {
		value, err := t.Get(key)
		if err != nil {
			return visited, err
		}
		visited[string(key)] = true
		if err := f(key, value); err != nil {
			return visited, err
		}
	}

	return visited, nil
}

func (t *TreeLSM) MapPrefixFunc(prefix []byte, f func([]byte, []byte) error) (map[string]bool, error) {
	keys := make(map[string]bool)
	_, err := t.MapFunc(func(key, value []byte) error {
		if strings.HasPrefix(string(key), string(prefix)) {
			keys[string(key)] = true
			return f(key, value)
		}
		return nil
	})

	return keys, err
}
