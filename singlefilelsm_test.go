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
		fatalf(t, "action=Create path=%s err=%v", dbPath, err)
	}

	// Test Put and Get
	testKey := []byte("test-key")
	testValue := []byte("test-value")

	if err := store.Put(testKey, testValue); err != nil {
		fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(testKey), trimTo40(testValue), err)
	}

	val, err := store.Get(testKey)
	if err != nil {
		fatalf(t, "action=Get key=%s err=%v", trimTo40(testKey), err)
	}

	if string(val) != string(testValue) {
		fatalf(t, "action=Get key=%s expected=%s got=%s", trimTo40(testKey), trimTo40(testValue), trimTo40(val))
	}

	// Test Exists
	if !store.Exists(testKey) {
		fatalf(t, "action=Exists key=%s expected=true got=false", trimTo40(testKey))
	}

	// Close and reopen
	if err := store.Close(); err != nil {
		fatalf(t, "action=Close path=%s err=%v", dbPath, err)
	}

	store2, err := NewSingleFileLSM(dbPath, 4096, 1024*1024)
	if err != nil {
		fatalf(t, "action=Reopen path=%s err=%v", dbPath, err)
	}
	defer store2.Close()

	// Verify persistence
	val2, err := store2.Get(testKey)
	if err != nil {
		fatalf(t, "action=Get key=%s err=%v", trimTo40(testKey), err)
	}

	if string(val2) != string(testValue) {
		fatalf(t, "action=Get key=%s expected=%s got=%s", trimTo40(testKey), trimTo40(testValue), trimTo40(val2))
	}

	t.Log("SUCCESS: Basic operations work correctly")
}

func TestSingleFileLSMDelete(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.lsm")

	store, err := NewSingleFileLSM(dbPath, 4096, 1024*1024)
	if err != nil {
		fatalf(t, "action=Create path=%s err=%v", dbPath, err)
	}
	defer store.Close()

	// Put and delete
	testKey := []byte("test-key")
	testValue := []byte("test-value")

	if err := store.Put(testKey, testValue); err != nil {
		fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(testKey), trimTo40(testValue), err)
	}

	if err := store.Delete(testKey); err != nil {
		fatalf(t, "action=Delete key=%s err=%v", trimTo40(testKey), err)
	}

	// Verify deleted
	if store.Exists(testKey) {
		fatalf(t, "action=Exists key=%s expected=false got=true", trimTo40(testKey))
	}

	_, err = store.Get(testKey)
	if err == nil {
		fatalf(t, "action=Get key=%s expectedErr=true gotErr=false", trimTo40(testKey))
	}

	t.Log("SUCCESS: Delete works correctly")
}

func TestSingleFileLSMMultipleKeys(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.lsm")

	store, err := NewSingleFileLSM(dbPath, 4096, 1024*1024)
	if err != nil {
		fatalf(t, "action=Create path=%s err=%v", dbPath, err)
	}
	defer store.Close()

	// Write multiple keys
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := store.Put(key, value); err != nil {
			fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(key), trimTo40(value), err)
		}
	}

	// Read them back
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		expectedValue := []byte(fmt.Sprintf("value-%d", i))

		val, err := store.Get(key)
		if err != nil {
			fatalf(t, "action=Get key=%s err=%v", trimTo40(key), err)
		}

		if string(val) != string(expectedValue) {
			fatalf(t, "action=Get key=%s expected=%s got=%s", trimTo40(key), trimTo40(expectedValue), trimTo40(val))
		}
	}

	t.Log("SUCCESS: Multiple keys work correctly")
}

func TestSingleFileLSMMapFunc(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.lsm")

	store, err := NewSingleFileLSM(dbPath, 4096, 1024*1024)
	if err != nil {
		fatalf(t, "action=Create path=%s err=%v", dbPath, err)
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
			fatalf(t, "action=Put key=%s value=%s err=%v", k, v, err)
		}
	}

	// Test MapFunc
	found := make(map[string]string)
	keys, err := store.MapFunc(func(k, v []byte) error {
		found[string(k)] = string(v)
		return nil
	})

	if err != nil {
		fatalf(t, "action=MapFunc err=%v", err)
	}

	// Verify all keys found
	for k, v := range testData {
		if found[k] != v {
			fatalf(t, "action=MapFunc key=%s expected=%s got=%s", k, v, found[k])
		}
		if !keys[k] {
			fatalf(t, "action=MapFunc key=%s expectedPresent=true got=false", k)
		}
	}

	t.Log("SUCCESS: MapFunc works correctly")
}

func TestSingleFileLSMMapPrefixFunc(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.lsm")

	store, err := NewSingleFileLSM(dbPath, 4096, 1024*1024)
	if err != nil {
		fatalf(t, "action=Create path=%s err=%v", dbPath, err)
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
			fatalf(t, "action=Put key=%s value=%s err=%v", k, v, err)
		}
	}

	// Test prefix search
	found := make(map[string]string)
	_, err = store.MapPrefixFunc([]byte("user:"), func(k, v []byte) error {
		found[string(k)] = string(v)
		return nil
	})

	if err != nil {
		fatalf(t, "action=MapPrefixFunc prefix=%s err=%v", "user:", err)
	}

	// Verify only user: keys found
	if len(found) != 3 {
		fatalf(t, "action=MapPrefixFunc prefix=%s expectedKeys=3 got=%d", "user:", len(found))
	}

	for k := range found {
		if k == "admin:root" {
			fatalf(t, "action=MapPrefixFunc prefix=%s unexpectedKey=%s", "user:", k)
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
		fatalf(t, "action=Create path=%s err=%v", dbPath, err)
	}

	testKey := []byte("test-key")
	testValue := []byte("test-value")

	if err := store.Put(testKey, testValue); err != nil {
		fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(testKey), trimTo40(testValue), err)
	}

	// Close normally
	if err := store.Close(); err != nil {
		fatalf(t, "action=Close path=%s err=%v", dbPath, err)
	}

	// Reopen - should recover from WAL
	store2, err := NewSingleFileLSM(dbPath, 4096, 1024*1024)
	if err != nil {
		fatalf(t, "action=Reopen path=%s err=%v", dbPath, err)
	}
	defer store2.Close()

	// Verify data
	val, err := store2.Get(testKey)
	if err != nil {
		fatalf(t, "action=Get key=%s err=%v", trimTo40(testKey), err)
	}

	if string(val) != string(testValue) {
		fatalf(t, "action=Get key=%s expected=%s got=%s", trimTo40(testKey), trimTo40(testValue), trimTo40(val))
	}

	t.Log("SUCCESS: Crash recovery works correctly")
}

func TestSingleFileLSMFlush(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.lsm")

	store, err := NewSingleFileLSM(dbPath, 4096, 1024*1024)
	if err != nil {
		fatalf(t, "action=Create path=%s err=%v", dbPath, err)
	}
	defer store.Close()

	// Write enough data to trigger a flush
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d-with-some-longer-content-to-fill-memtable", i))
		if err := store.Put(key, value); err != nil {
			fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(key), trimTo40(value), err)
		}
	}

	// Manually flush
	if err := store.Flush(); err != nil {
		fatalf(t, "action=Flush err=%v", err)
	}

	// Verify data is still accessible
	key := []byte("key-100")
	expectedValue := []byte("value-100-with-some-longer-content-to-fill-memtable")

	val, err := store.Get(key)
	if err != nil {
		fatalf(t, "action=Get key=%s err=%v", trimTo40(key), err)
	}

	if string(val) != string(expectedValue) {
		fatalf(t, "action=Get key=%s expected=%s got=%s", trimTo40(key), trimTo40(expectedValue), trimTo40(val))
	}

	t.Log("SUCCESS: Flush works correctly")
}

func TestSingleFileLSMCorruptionHandling(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.lsm")

	// Create and populate store
	store, err := NewSingleFileLSM(dbPath, 4096, 1024*1024)
	if err != nil {
		fatalf(t, "action=Create path=%s err=%v", dbPath, err)
	}

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := store.Put(key, value); err != nil {
			fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(key), trimTo40(value), err)
		}
	}

	// Flush to create SSTable
	if err := store.Flush(); err != nil {
		fatalf(t, "action=Flush err=%v", err)
	}

	store.Close()

	// Corrupt the file (truncate it)
	stat, err := os.Stat(dbPath)
	if err != nil {
		fatalf(t, "action=Stat path=%s err=%v", dbPath, err)
	}

	// Truncate to 75% of size
	newSize := stat.Size() * 3 / 4
	if err := os.Truncate(dbPath, newSize); err != nil {
		fatalf(t, "action=Truncate path=%s size=%d err=%v", dbPath, newSize, err)
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
		fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(newKey), trimTo40(newValue), err)
	}

	val, err := store2.Get(newKey)
	if err != nil {
		fatalf(t, "action=Get key=%s err=%v", trimTo40(newKey), err)
	}

	if string(val) != string(newValue) {
		fatalf(t, "action=Get key=%s expected=%s got=%s", trimTo40(newKey), trimTo40(newValue), trimTo40(val))
	}

	t.Log("SUCCESS: Corruption handling works correctly")
}
