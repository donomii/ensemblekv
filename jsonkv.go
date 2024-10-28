package ensemblekv

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// JsonKV is a simple JSON-based key-value store
type JsonKV struct {
	DefaultOps
	filename string
	data     map[string][]byte
	mutex    sync.RWMutex
}

// NewJsonKV creates a new JSON key-value store
func JsonKVCreator(directory string, blockSize int) (KvLike, error) {
	store := &JsonKV{
		filename: filepath.Join(directory, "store.json"),
		data:     make(map[string][]byte),
	}

	// Ensure directory exists
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Load existing data if any
	if err := store.load(); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to load store: %w", err)
		}
	}

	return store, nil
}

// Helper function to encode bytes for JSON storage
func encodeBytes(b []byte) string {
	return string(b)
}

// Helper function to decode bytes from JSON storage
func decodeBytes(s string) []byte {
	return []byte(s)
}

// load reads the JSON file into memory
func (s *JsonKV) load() error {
	data, err := os.ReadFile(s.filename)
	if err != nil {
		return err
	}

	// Create temporary map for string-based storage
	tempMap := make(map[string]string)
	if err := json.Unmarshal(data, &tempMap); err != nil {
		return fmt.Errorf("failed to unmarshal data: %w", err)
	}

	// Convert string values back to bytes
	for k, v := range tempMap {
		s.data[k] = decodeBytes(v)
	}

	return nil
}

// save writes the in-memory data to JSON file
func (s *JsonKV) save() error {
	// Create temporary map for string-based storage
	tempMap := make(map[string]string)
	for k, v := range s.data {
		tempMap[k] = encodeBytes(v)
	}

	data, err := json.MarshalIndent(tempMap, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	return os.WriteFile(s.filename, data, 0644)
}

// Get retrieves a value for a key
func (s *JsonKV) Get(key []byte) ([]byte, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	value, exists := s.data[string(key)]
	if !exists {
		return nil, fmt.Errorf("key not found")
	}
	return value, nil
}

// Put stores a key-value pair
func (s *JsonKV) Put(key []byte, value []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.data[string(key)] = value
	return s.save()
}

// Delete removes a key-value pair
func (s *JsonKV) Delete(key []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.data, string(key))
	return s.save()
}

// Exists checks if a key exists
func (s *JsonKV) Exists(key []byte) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	_, exists := s.data[string(key)]
	return exists
}

// Size returns the number of stored keys
func (s *JsonKV) Size() int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return int64(len(s.data))
}

// Flush ensures all data is written to disk
func (s *JsonKV) Flush() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.save()
}

// Close flushes data and closes the store
func (s *JsonKV) Close() error {
	return s.Flush()
}

// MapFunc applies a function to all key-value pairs
func (s *JsonKV) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	visited := make(map[string]bool)
	for k, v := range s.data {
		visited[k] = true
		if err := f([]byte(k), v); err != nil {
			return visited, err
		}
	}
	return visited, nil
}