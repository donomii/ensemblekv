package ensemblekv

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// JsonKV is a simple JSON-based key-value store
type JsonKV struct {
	DefaultOps
	filename string
	data     map[string][]byte
	mutex    sync.RWMutex
}

type jsonKVEntry struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// NewJsonKV creates a new JSON key-value store
func JsonKVCreator(directory string, blockSize, fileSize int64) (KvLike, error) {
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

// load reads the JSON file into memory
func (s *JsonKV) load() error {
	data, err := os.ReadFile(s.filename)
	if err != nil {
		return err
	}

	tempMap := make(map[string][]byte)
	if bytes.HasPrefix(bytes.TrimSpace(data), []byte("[")) {
		var entries []jsonKVEntry
		if err := json.Unmarshal(data, &entries); err != nil {
			return fmt.Errorf("failed to unmarshal entries: %w", err)
		}
		for _, entry := range entries {
			key := string(entry.Key)
			if _, exists := tempMap[key]; exists {
				return fmt.Errorf("duplicate JSON store key %x", entry.Key)
			}
			tempMap[key] = entry.Value
		}
	} else {
		if err := json.Unmarshal(data, &tempMap); err != nil {
			return fmt.Errorf("failed to unmarshal data: %w", err)
		}
		if tempMap == nil {
			return fmt.Errorf("invalid JSON store: expected an object or an array of entries, got null")
		}
	}

	s.data = tempMap

	return nil
}

// save writes the in-memory data to JSON file
func (s *JsonKV) save() error {
	entries := make([]jsonKVEntry, 0, len(s.data))
	for key, value := range s.data {
		entries = append(entries, jsonKVEntry{Key: []byte(key), Value: value})
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	info, err := os.Stat(s.filename)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(s.filename), ".store-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if info != nil {
		if err := file.Chmod(info.Mode().Perm()); err != nil {
			return err
		}
	}
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), s.filename); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(s.filename))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

// KeyHistory returns all values for a key, even if they are deleted
func (s *JsonKV) KeyHistory(key []byte) ([][]byte, error) {
	// JsonKV only maintains current values, no history
	value, err := s.Get(key)
	if err != nil {
		return nil, err
	}

	return [][]byte{value}, nil
}

func (s *JsonKV) Keys() [][]byte {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	keys := make([][]byte, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, []byte(k))
	}
	return keys
}

// Get retrieves a value for a key
func (s *JsonKV) Get(key []byte) ([]byte, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	value, exists := s.data[string(key)]
	if !exists {
		return nil, fmt.Errorf("key not found")
	}
	return bytes.Clone(value), nil
}

// Put stores a key-value pair
func (s *JsonKV) Put(key []byte, value []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.data[string(key)] = bytes.Clone(value)
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
	keys := s.Keys()

	visited := make(map[string]bool)
	for _, k := range keys {
		val, err := s.Get(k)
		if err != nil {
			continue
		}
		visited[string(k)] = true
		if err := f(k, val); err != nil {
			return visited, err
		}
	}
	return visited, nil
}

func (s *JsonKV) MapPrefixFunc(prefix []byte, f func([]byte, []byte) error) (map[string]bool, error) {
	searchKeys := s.Keys()

	keys := make(map[string]bool)
	prefixStr := string(prefix)

	for _, kb := range searchKeys {
		k := string(kb)
		if strings.HasPrefix(k, prefixStr) {
			val, err := s.Get(kb)
			if err != nil {
				continue
			}
			keys[k] = true
			if err := f(kb, val); err != nil {
				return keys, err
			}
		}
	}
	return keys, nil
}
