package ensemblekv

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	starMaxStoreSize = 64 * 1024 * 1024 // 64MB before splitting
)

// StarLSM represents a recursively splitting LSM-tree store
type StarLSM struct {
	DefaultOps
	directory    string
	currentStore KvLike
	subStores    map[string]*StarLSM
	parent       *StarLSM
	prefix       string
	depth        int64         // Current depth in the tree
	isReadOnly   bool
	createStore  CreatorFunc
	blockSize    int64
	fileSize     int64
	mutex        sync.RWMutex
	hashFunc     HashFunc
}

// NewStarLSM creates a new StarLSM store
func NewStarLSM(
	directory string,
	blockSize int64,
	fileSize int64,
	createStore CreatorFunc,
) (*StarLSM, error) {
	store := &StarLSM{
		directory:    directory,
		subStores:    make(map[string]*StarLSM),
		createStore:  createStore,
		blockSize:    blockSize,
		fileSize:     fileSize,
		hashFunc:     defaultHashFunc,
		depth:        0,
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

// hashKey generates a hex string hash of the key
func (s *StarLSM) hashKey(key []byte) string {
	hashVal := s.hashFunc(key)
	return fmt.Sprintf("%016x", hashVal)
}

// getPrefixAtDepth returns the prefix for a hash at the current depth
func (s *StarLSM) getPrefixAtDepth(hashedKey string, depth int64) string {
	if len(hashedKey) <= int(depth) {
		return hashedKey
	}
	return hashedKey[:depth+1]
}

func (s *StarLSM) KeyHistory(key []byte) ([][]byte, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	
	allHistory := make([][]byte, 0)
	
	// Try current store history
	history, err := s.currentStore.KeyHistory(key)
	if err == nil && len(history) > 0 {
		allHistory = append(allHistory, history...)
	}
	
	// Hash the key
	hashedKey := s.hashKey(key)
	prefix := s.getPrefixAtDepth(hashedKey, s.depth)

	// Check appropriate substore
	if subStore, exists := s.subStores[prefix]; exists {
		history, err := subStore.KeyHistory(key)
		if err == nil && len(history) > 0 {
			allHistory = append(allHistory, history...)
		}
	}
	
	// Also check other substores
	for subPrefix, subStore := range s.subStores {
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
func (s *StarLSM) Put(key, value []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	hashedKey := s.hashKey(key)

	if s.isReadOnly {
		prefix := s.getPrefixAtDepth(hashedKey, s.depth)
		subStore, exists := s.subStores[prefix]
		if !exists {
			return fmt.Errorf("no substore found for prefix %s at depth %d", prefix, s.depth)
		}
		return subStore.Put(key, value)
	}

	// Check if we need to split
	if s.currentStore.Size() > starMaxStoreSize {
		if err := s.split(hashedKey); err != nil {
			return fmt.Errorf("failed to split store: %w", err)
		}
		// After splitting, we're read-only, so recursively put
		return s.Put(key, value)
	}

	return s.currentStore.Put(key, value)
}

// split creates substores for the next level of hash precision
func (s *StarLSM) split(hashedKey string) error {
	nextDepth := s.depth + 1
	
	// Create subdirectories for each hex character at this depth
	for i := 0; i < 16; i++ {
		prefix := fmt.Sprintf("%s%x", s.prefix, i)
		subDir := filepath.Join(s.directory, fmt.Sprintf("%x", i))
		
		subStore, err := NewStarLSM(subDir, s.blockSize, s.fileSize, s.createStore)
		if err != nil {
			return fmt.Errorf("failed to create substore %s: %w", prefix, err)
		}
		
		subStore.prefix = prefix
		subStore.parent = s
		subStore.depth = nextDepth
		s.subStores[prefix] = subStore
	}

	// Redistribute existing data based on the next character in their hash
	_, err := s.currentStore.MapFunc(func(key, value []byte) error {
		keyHash := s.hashKey(key)
		prefix := s.getPrefixAtDepth(keyHash, nextDepth-1)
		subStore := s.subStores[prefix]
		return subStore.currentStore.Put(key, value)
	})
	if err != nil {
		return fmt.Errorf("failed to redistribute data: %w", err)
	}

	// Mark current store as read-only
	s.isReadOnly = true
	return nil
}

// Get retrieves a value for a key
func (s *StarLSM) Get(key []byte) ([]byte, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	hashedKey := s.hashKey(key)

	// Check substores first if they exist
	prefix := s.getPrefixAtDepth(hashedKey, s.depth)
	if subStore, exists := s.subStores[prefix]; exists {
		value, err := subStore.Get(key)
		if err == nil {
			return value, nil
		}
	}

	// Then check current store
	value, err := s.currentStore.Get(key)
	if err == nil {
		return value, nil
	}

	// Finally check parent if we have one
	if s.parent != nil {
		return s.parent.Get(key)
	}

	return nil, fmt.Errorf("key not found")
}

// Delete removes a key-value pair
func (s *StarLSM) Delete(key []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	hashedKey := s.hashKey(key)

	if s.isReadOnly {
		prefix := s.getPrefixAtDepth(hashedKey, s.depth)
		subStore, exists := s.subStores[prefix]
		if !exists {
			return fmt.Errorf("no substore found for prefix %s at depth %d", prefix, s.depth)
		}
		return subStore.Delete(key)
	}

	return s.currentStore.Delete(key)
}

// Exists checks if a key exists
func (s *StarLSM) Exists(key []byte) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	hashedKey := s.hashKey(key)

	// Check substores first
	prefix := s.getPrefixAtDepth(hashedKey, s.depth)
	if subStore, exists := s.subStores[prefix]; exists {
		if subStore.Exists(key) {
			return true
		}
	}

	// Then current store
	if s.currentStore.Exists(key) {
		return true
	}

	// Finally parent
	if s.parent != nil {
		return s.parent.Exists(key)
	}

	return false
}

// loadSubStores scans for and loads existing substores
func (s *StarLSM) loadSubStores() error {
	entries, err := os.ReadDir(s.directory)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() && len(entry.Name()) == 1 && isHexString(entry.Name()) {
			subStore, err := NewStarLSM(
				filepath.Join(s.directory, entry.Name()),
				s.blockSize,
				s.fileSize,
				s.createStore,

			)
			if err != nil {
				return fmt.Errorf("failed to load substore %s: %w", entry.Name(), err)
			}
			
			prefix := s.prefix + entry.Name()
			subStore.prefix = prefix
			subStore.parent = s
			subStore.depth = s.depth + 1
			s.subStores[prefix] = subStore
		}
	}

	return nil
}

// Flush ensures all data is written to disk
func (s *StarLSM) Flush() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := s.currentStore.Flush(); err != nil {
		return err
	}

	for _, subStore := range s.subStores {
		if err := subStore.Flush(); err != nil {
			return err
		}
	}

	return nil
}

// Close closes all stores
func (s *StarLSM) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := s.currentStore.Close(); err != nil {
		return err
	}

	for _, subStore := range s.subStores {
		if err := subStore.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Size returns the total number of keys
func (s *StarLSM) Size() int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var total int64
	total += s.currentStore.Size()

	for _, subStore := range s.subStores {
		total += subStore.Size()
	}

	return total
}

// MapFunc applies a function to all key-value pairs
func (s *StarLSM) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// Start with current store
	visited, err := s.currentStore.MapFunc(f)
	if err != nil {
		return nil, err
	}

	// Then process substores
	for _, subStore := range s.subStores {
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