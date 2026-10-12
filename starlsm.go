package ensemblekv

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	depth        int64 // Current depth in the tree
	isReadOnly   bool
	closed       bool
	splitFailure error
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
	return newStarLSM(directory, blockSize, fileSize, createStore, nil, "", 0)
}

func newStarLSM(directory string, blockSize, fileSize int64, createStore CreatorFunc, parent *StarLSM, prefix string, depth int64) (*StarLSM, error) {
	store := &StarLSM{
		directory:   directory,
		subStores:   make(map[string]*StarLSM),
		createStore: createStore,
		blockSize:   blockSize,
		fileSize:    fileSize,
		hashFunc:    defaultHashFunc,
		parent:      parent,
		prefix:      prefix,
		depth:       depth,
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
	store.isReadOnly = len(store.subStores) > 0

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

func (s *StarLSM) Keys() [][]byte {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var keys [][]byte
	if !s.isReadOnly {
		return s.currentStore.Keys()
	}
	for _, subStore := range s.subStores {
		subKeys := subStore.Keys()
		keys = append(keys, subKeys...)
	}
	return keys
}

// Put stores a key-value pair
func (s *StarLSM) Put(key, value []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.put(key, value)
}

func (s *StarLSM) put(key, value []byte) error {
	if err := s.available(); err != nil {
		return err
	}
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
	shouldSplit, err := splitNeeded(s.currentStore, key, value, starMaxStoreSize)
	if err != nil {
		return err
	}
	if shouldSplit && s.depth < 16 {
		if err := s.split(hashedKey); err != nil {
			return fmt.Errorf("failed to split store: %w", err)
		}
		// After splitting, we're read-only, so recursively put
		return s.put(key, value)
	}

	return s.currentStore.Put(key, value)
}

// split creates substores for the next level of hash precision
func (s *StarLSM) split(hashedKey string) (result error) {
	if err := s.available(); err != nil {
		return err
	}
	if s.isReadOnly {
		return fmt.Errorf("store %s already has a committed split", s.directory)
	}
	if s.depth >= 16 {
		return fmt.Errorf("cannot split store %q beyond the 16-character hash", s.directory)
	}
	nextDepth := s.depth + 1
	path, err := prepareSplit(s.directory, s.splitLayout(), s.currentStore)
	if err != nil {
		return err
	}
	children := make(map[string]*StarLSM)
	defer func() {
		for _, child := range children {
			result = errors.Join(result, child.Close())
		}
	}()

	// Create subdirectories for each hex character at this depth
	for i := 0; i < 16; i++ {
		prefix := fmt.Sprintf("%s%x", s.prefix, i)
		subDir := filepath.Join(path, fmt.Sprintf("%x", i))

		subStore, err := newStarLSM(subDir, s.blockSize, s.fileSize, s.createStore, s, prefix, nextDepth)
		if err != nil {
			return fmt.Errorf("failed to create substore %s: %w", prefix, err)
		}

		children[prefix] = subStore
	}

	// Redistribute existing data based on the next character in their hash
	err = copySplit(s.currentStore, path, func(key []byte) KvLike {
		keyHash := s.hashKey(key)
		prefix := s.getPrefixAtDepth(keyHash, nextDepth-1)
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
	published, err := publishSplit(s.directory)
	if err != nil {
		if published {
			s.splitFailure = err
		}
		return err
	}
	if err := s.loadSubStores(); err != nil {
		s.splitFailure = err
		return err
	}

	// Mark current store as read-only
	s.isReadOnly = true
	return nil
}

// Get retrieves a value for a key
func (s *StarLSM) Get(key []byte) ([]byte, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if err := s.available(); err != nil {
		return nil, err
	}

	hashedKey := s.hashKey(key)

	// Check substores first if they exist
	prefix := s.getPrefixAtDepth(hashedKey, s.depth)
	if subStore, exists := s.subStores[prefix]; exists {
		return subStore.Get(key)
	}
	if s.isReadOnly {
		return nil, fmt.Errorf("StarLSM store %s is missing committed child %s", s.directory, prefix)
	}

	// Then check current store
	return s.currentStore.Get(key)
}

// Delete removes a key-value pair
func (s *StarLSM) Delete(key []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if err := s.available(); err != nil {
		return err
	}

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
	if s.available() != nil {
		return false
	}

	hashedKey := s.hashKey(key)

	// Check substores first
	prefix := s.getPrefixAtDepth(hashedKey, s.depth)
	if subStore, exists := s.subStores[prefix]; exists {
		return subStore.Exists(key)
	}
	if s.isReadOnly {
		return false
	}

	// Then current store
	if s.currentStore.Exists(key) {
		return true
	}

	return false
}

// loadSubStores scans for and loads existing substores
func (s *StarLSM) loadSubStores() error {
	path, err := inspectSplit(s.directory, s.splitLayout())
	if err != nil {
		return err
	}
	if path == "" {
		return nil
	}
	for partition := 0; partition < 16; partition++ {
		prefix := fmt.Sprintf("%s%x", s.prefix, partition)
		childPath := filepath.Join(path, splitChildName("star", s.prefix, partition))
		subStore, err := newStarLSM(childPath, s.blockSize, s.fileSize, s.createStore, s, prefix, s.depth+1)
		if err != nil {
			return fmt.Errorf("failed to load substore %s: %w", childPath, err)
		}
		s.subStores[prefix] = subStore
	}

	return nil
}

// Flush ensures all data is written to disk
func (s *StarLSM) Flush() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if err := s.available(); err != nil {
		return err
	}

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

	if s.closed {
		return nil
	}
	s.closed = true
	err := s.currentStore.Close()
	for _, subStore := range s.subStores {
		err = errors.Join(err, subStore.Close())
	}
	return err
}

func (s *StarLSM) splitLayout() splitLayout {
	return splitLayout{Version: 1, Kind: "star", Prefix: s.prefix, Depth: s.depth}
}

func (s *StarLSM) available() error {
	if s.closed {
		return fmt.Errorf("StarLSM store %s is closed", s.directory)
	}
	if s.splitFailure != nil {
		return fmt.Errorf("StarLSM store %s needs reopening after split publication failed: %w", s.directory, s.splitFailure)
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
	visited := make(map[string]bool)
	for _, key := range s.Keys() {
		value, err := s.Get(key)
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

func (s *StarLSM) MapPrefixFunc(prefix []byte, f func([]byte, []byte) error) (map[string]bool, error) {
	keys := make(map[string]bool)
	_, err := s.MapFunc(func(key, value []byte) error {
		if strings.HasPrefix(string(key), string(prefix)) {
			keys[string(key)] = true
			return f(key, value)
		}
		return nil
	})

	return keys, err
}
