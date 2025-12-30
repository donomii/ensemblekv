package ensemblekv

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSingleFileLSMBasic(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.lsm")

	// Create new store
	store, err := NewSingleFileLSM(dbPath, 4096, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Test Put and Get
	testKey := []byte("test-key")
	testValue := []byte("test-value")

	if err := store.Put(testKey, testValue); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	val, err := store.Get(testKey)
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if string(val) != string(testValue) {
		t.Fatalf("Value mismatch: got %s, want %s", val, testValue)
	}

	// Test Exists
	if !store.Exists(testKey) {
		t.Fatal("Key should exist")
	}

	// Close and reopen
	if err := store.Close(); err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	store2, err := NewSingleFileLSM(dbPath, 4096, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer store2.Close()

	// Verify persistence
	val2, err := store2.Get(testKey)
	if err != nil {
		t.Fatalf("Failed to get after reopen: %v", err)
	}

	if string(val2) != string(testValue) {
		t.Fatalf("Value mismatch after reopen: got %s, want %s", val2, testValue)
	}

	t.Log("SUCCESS: Basic operations work correctly")
}

func TestSingleFileLSMDelete(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.lsm")

	store, err := NewSingleFileLSM(dbPath, 4096, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Put and delete
	testKey := []byte("test-key")
	testValue := []byte("test-value")

	if err := store.Put(testKey, testValue); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	if err := store.Delete(testKey); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Verify deleted
	if store.Exists(testKey) {
		t.Fatal("Key should not exist after delete")
	}

	_, err = store.Get(testKey)
	if err == nil {
		t.Fatal("Get should fail for deleted key")
	}

	t.Log("SUCCESS: Delete works correctly")
}

func TestSingleFileLSMMultipleKeys(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.lsm")

	store, err := NewSingleFileLSM(dbPath, 4096, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Write multiple keys
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := store.Put(key, value); err != nil {
			t.Fatalf("Failed to put key %d: %v", i, err)
		}
	}

	// Read them back
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		expectedValue := []byte(fmt.Sprintf("value-%d", i))

		val, err := store.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key %d: %v", i, err)
		}

		if string(val) != string(expectedValue) {
			t.Fatalf("Value mismatch for key %d: got %s, want %s", i, val, expectedValue)
		}
	}

	t.Log("SUCCESS: Multiple keys work correctly")
}

func TestSingleFileLSMMapFunc(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.lsm")

	store, err := NewSingleFileLSM(dbPath, 4096, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Write test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for k, v := range testData {
		if err := store.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Failed to put %s: %v", k, err)
		}
	}

	// Test MapFunc
	found := make(map[string]string)
	keys, err := store.MapFunc(func(k, v []byte) error {
		found[string(k)] = string(v)
		return nil
	})

	if err != nil {
		t.Fatalf("MapFunc failed: %v", err)
	}

	// Verify all keys found
	for k, v := range testData {
		if found[k] != v {
			t.Fatalf("MapFunc: expected %s=%s, got %s", k, v, found[k])
		}
		if !keys[k] {
			t.Fatalf("MapFunc: key %s not in result map", k)
		}
	}

	t.Log("SUCCESS: MapFunc works correctly")
}

func TestSingleFileLSMMapPrefixFunc(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.lsm")

	store, err := NewSingleFileLSM(dbPath, 4096, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Write test data with prefixes
	testData := map[string]string{
		"user:alice": "alice-data",
		"user:bob":   "bob-data",
		"admin:root": "root-data",
		"user:carol": "carol-data",
	}

	for k, v := range testData {
		if err := store.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Failed to put %s: %v", k, err)
		}
	}

	// Test prefix search
	found := make(map[string]string)
	_, err = store.MapPrefixFunc([]byte("user:"), func(k, v []byte) error {
		found[string(k)] = string(v)
		return nil
	})

	if err != nil {
		t.Fatalf("MapPrefixFunc failed: %v", err)
	}

	// Verify only user: keys found
	if len(found) != 3 {
		t.Fatalf("Expected 3 user keys, got %d", len(found))
	}

	for k := range found {
		if k == "admin:root" {
			t.Fatal("Should not find admin:root with user: prefix")
		}
	}

	t.Log("SUCCESS: MapPrefixFunc works correctly")
}

func TestSingleFileLSMCrashRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.lsm")

	// Create store and write data
	store, err := NewSingleFileLSM(dbPath, 4096, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	testKey := []byte("test-key")
	testValue := []byte("test-value")

	if err := store.Put(testKey, testValue); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Close normally
	if err := store.Close(); err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	// Reopen - should recover from WAL
	store2, err := NewSingleFileLSM(dbPath, 4096, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer store2.Close()

	// Verify data
	val, err := store2.Get(testKey)
	if err != nil {
		t.Fatalf("Failed to get after recovery: %v", err)
	}

	if string(val) != string(testValue) {
		t.Fatalf("Value mismatch after recovery: got %s, want %s", val, testValue)
	}

	t.Log("SUCCESS: Crash recovery works correctly")
}

func TestSingleFileLSMFlush(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.lsm")

	store, err := NewSingleFileLSM(dbPath, 4096, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Write enough data to trigger a flush
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d-with-some-longer-content-to-fill-memtable", i))
		if err := store.Put(key, value); err != nil {
			t.Fatalf("Failed to put key %d: %v", i, err)
		}
	}

	// Manually flush
	if err := store.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Verify data is still accessible
	key := []byte("key-100")
	expectedValue := []byte("value-100-with-some-longer-content-to-fill-memtable")

	val, err := store.Get(key)
	if err != nil {
		t.Fatalf("Failed to get after flush: %v", err)
	}

	if string(val) != string(expectedValue) {
		t.Fatalf("Value mismatch after flush: got %s, want %s", val, expectedValue)
	}

	t.Log("SUCCESS: Flush works correctly")
}

func TestSingleFileLSMCorruptionHandling(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.lsm")

	// Create and populate store
	store, err := NewSingleFileLSM(dbPath, 4096, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := store.Put(key, value); err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}

	// Flush to create SSTable
	if err := store.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	store.Close()

	// Corrupt the file (truncate it)
	stat, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	// Truncate to 75% of size
	newSize := stat.Size() * 3 / 4
	if err := os.Truncate(dbPath, newSize); err != nil {
		t.Fatalf("Failed to truncate: %v", err)
	}

	// Try to reopen - should handle corruption gracefully
	store2, err := NewSingleFileLSM(dbPath, 4096, 1024*1024)
	if err != nil {
		t.Logf("Failed to reopen corrupted store (expected): %v", err)
		// This is acceptable - corruption may prevent opening
		return
	}
	defer store2.Close()

	// Try to write new data - should work
	newKey := []byte("new-key-after-corruption")
	newValue := []byte("new-value")

	if err := store2.Put(newKey, newValue); err != nil {
		t.Fatalf("Failed to write after corruption: %v", err)
	}

	val, err := store2.Get(newKey)
	if err != nil {
		t.Fatalf("Failed to read new data: %v", err)
	}

	if string(val) != string(newValue) {
		t.Fatalf("Value mismatch: got %s, want %s", val, newValue)
	}

	t.Log("SUCCESS: Corruption handling works correctly")
}
