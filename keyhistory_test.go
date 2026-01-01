package ensemblekv

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestExtentKeyHistory tests the KeyHistory functionality for ExtentKeyValueStore
func TestExtentKeyHistory(t *testing.T) {
	t.Skip("REthinking key history")
	// Create a temp directory for the store
	dir := t.TempDir()
	storePath := filepath.Join(dir, "extent_test")

	// Create an ExtentKeyValueStore
	store, err := ExtentCreator(storePath, 4096, testFileCapacity)
	if err != nil {
		fatalf(t, "action=Create store=ExtentKV path=%s err=%v", storePath, err)
	}
	defer store.Close()

	// Define test key and values
	key := []byte("test_key_history")
	value1 := []byte("initial_value")
	value2 := []byte("updated_value")

	// Test sequence:
	// 1. Put initial value
	// 2. Get key history (should have 1 entry)
	// 3. Update value
	// 4. Get key history (should have 2 entries)
	// 5. Delete key
	// 6. Get key history (should have 0 entries - deleted keys aren't returned)

	// Step 1: Put initial value
	if err := store.Put(key, value1); err != nil {
		fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(key), trimTo40(value1), err)
	}

	store.Flush()

	// Step 2: Get key history
	history, err := store.KeyHistory(key)
	if err != nil {
		fatalf(t, "action=KeyHistory key=%s err=%v", trimTo40(key), err)
	}

	if len(history) != 1 {
		fatalf(t, "action=KeyHistory key=%s expectedEntries=1 got=%d", trimTo40(key), len(history))
	} else if !bytes.Equal(history[0], value1) {
		fatalf(t, "action=KeyHistory key=%s expected=%s got=%s", trimTo40(key), trimTo40(value1), trimTo40(history[0]))
	}

	// Step 3: Update value
	if err := store.Put(key, value2); err != nil {
		fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(key), trimTo40(value2), err)
	}

	// Step 4: Get key history
	history, err = store.KeyHistory(key)
	if err != nil {
		fatalf(t, "action=KeyHistory key=%s err=%v", trimTo40(key), err)
	}

	// Should have both values
	if len(history) != 2 {
		fatalf(t, "action=KeyHistory key=%s expectedEntries=2 got=%d", trimTo40(key), len(history))
	}

	// Check if values are in history
	foundValue1 := false
	foundValue2 := false
	for _, val := range history {
		if bytes.Equal(val, value1) {
			foundValue1 = true
		}
		if bytes.Equal(val, value2) {
			foundValue2 = true
		}
	}

	if !foundValue1 {
		fatalf(t, "action=KeyHistory key=%s missingValue=%s", trimTo40(key), trimTo40(value1))
	}
	if !foundValue2 {
		fatalf(t, "action=KeyHistory key=%s missingValue=%s", trimTo40(key), trimTo40(value2))
	}

	// Step 5: Delete key
	if err := store.Delete(key); err != nil {
		fatalf(t, "action=Delete key=%s err=%v", trimTo40(key), err)
	}

	// Step 6: Get key history
	history, err = store.KeyHistory(key)
	if err != nil {
		t.Logf("Error getting history after deletion: %v", err)
	}

	// After deletion, we should have no values in history (as specified)
	if len(history) != 0 {
		t.Logf("Note: Found %d history entries after deletion", len(history))
	}
}

// TestEnsembleKeyHistory tests the KeyHistory functionality for EnsembleKv
func TestEnsembleKeyHistory(t *testing.T) {
	t.Skip("REthinking key history")
	// Create a temp directory for the store
	dir := t.TempDir()
	storePath := filepath.Join(dir, "ensemble_test")

	// Create an EnsembleKv store
	store, err := EnsembleCreator(storePath, 4096, testFileCapacity, ExtentCreator)
	if err != nil {
		fatalf(t, "action=Create store=EnsembleKV path=%s err=%v", storePath, err)
	}
	defer store.Close()

	// Define test keys and values
	key1 := []byte("ensemble_key_1")
	key2 := []byte("ensemble_key_2")
	value1 := []byte("ensemble_value_1")
	value2 := []byte("ensemble_value_2")
	updatedValue := []byte("updated_ensemble_value")

	// Add multiple keys to spread across substores
	if err := store.Put(key1, value1); err != nil {
		fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(key1), trimTo40(value1), err)
	}

	if err := store.Put(key2, value2); err != nil {
		fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(key2), trimTo40(value2), err)
	}

	// Update a key
	if err := store.Put(key1, updatedValue); err != nil {
		fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(key1), trimTo40(updatedValue), err)
	}

	// Get history for key1
	history, err := store.KeyHistory(key1)
	if err != nil {
		fatalf(t, "action=KeyHistory key=%s err=%v", trimTo40(key1), err)
	}

	// Should have at least the updated value
	if len(history) == 0 {
		fatalf(t, "action=KeyHistory key=%s expectedEntries>0 got=0", trimTo40(key1))
	}

	// Check if updated value is in history
	foundUpdated := false
	for _, val := range history {
		if bytes.Equal(val, updatedValue) {
			foundUpdated = true
			break
		}
	}

	if !foundUpdated {
		fatalf(t, "action=KeyHistory key=%s missingValue=%s", trimTo40(key1), trimTo40(updatedValue))
	}
}

// TestMultipleStoreTypes tests KeyHistory across different store types
func TestMultipleStoreTypes(t *testing.T) {
	t.Skip("REthinking key history")
	baseDir := t.TempDir()

	storeCreators := []struct {
		name    string
		creator func(dir string) (KvLike, error)
	}{
		{
			name: "ExtentKV",
			creator: func(dir string) (KvLike, error) {
				return ExtentCreator(dir, 4096, testFileCapacity) //FIXME filesize
			},
		},
		{
			name: "JsonKV",
			creator: func(dir string) (KvLike, error) {
				return JsonKVCreator(dir, 4096, testFileCapacity) //FIXME filesize
			},
		},
		{
			name: "BoltDB",
			creator: func(dir string) (KvLike, error) {
				return BoltDbCreator(dir, 4096, testFileCapacity) //FIXME filesize
			},
		},
		{
			name: "EnsembleKV",
			creator: func(dir string) (KvLike, error) {
				return EnsembleCreator(dir, 4096, 10000000, ExtentCreator)
			},
		},
		{
			name: "TreeLSM",
			creator: func(dir string) (KvLike, error) {
				return NewTreeLSM(dir, 4096, testFileCapacity, 0, ExtentCreator)
			},
		},
		{
			name: "StarLSM",
			creator: func(dir string) (KvLike, error) {
				return NewStarLSM(dir, 4096, testFileCapacity, ExtentCreator)
			},
		},
	}

	for _, sc := range storeCreators {
		runFailfast(t, sc.name, func(t *testing.T) {
			storePath := filepath.Join(baseDir, sc.name)

			// Create store
			store, err := sc.creator(storePath)
			if err != nil {
				fatalf(t, "action=Create store=%s path=%s err=%v", sc.name, storePath, err)
			}
			defer store.Close()

			// Test key and values
			key := []byte("multi_store_test_key")
			value1 := []byte("initial value")
			value2 := []byte("updated value")

			// Put initial value
			if err := store.Put(key, value1); err != nil {
				fatalf(t, "action=Put store=%s key=%s value=%s err=%v", sc.name, trimTo40(key), trimTo40(value1), err)
			}

			// Get history - should have at least 1 value
			history, err := store.KeyHistory(key)
			if err != nil {
				fatalf(t, "action=KeyHistory store=%s key=%s err=%v", sc.name, trimTo40(key), err)
			}

			if len(history) == 0 {
				fatalf(t, "action=KeyHistory store=%s key=%s expectedEntries>0 got=0", sc.name, trimTo40(key))
			}

			// Check if initial value is in history
			foundInitial := false
			for _, val := range history {
				if bytes.Equal(val, value1) {
					foundInitial = true
					break
				}
			}

			if !foundInitial {
				fatalf(t, "action=KeyHistory store=%s key=%s missingValue=%s", sc.name, trimTo40(key), trimTo40(value1))
			}

			// Update value
			if err := store.Put(key, value2); err != nil {
				fatalf(t, "action=Put store=%s key=%s value=%s err=%v", sc.name, trimTo40(key), trimTo40(value2), err)
			}

			// Get history again
			history, err = store.KeyHistory(key)
			if err != nil {
				fatalf(t, "action=KeyHistory store=%s key=%s err=%v", sc.name, trimTo40(key), err)
			}

			// Check if updated value is in history
			foundUpdated := false
			for _, val := range history {
				if bytes.Equal(val, value2) {
					foundUpdated = true
					break
				}
			}

			if !foundUpdated {
				fatalf(t, "action=KeyHistory store=%s key=%s missingValue=%s", sc.name, trimTo40(key), trimTo40(value2))
			}
		})
	}
}

// TestRecoverFromCorruption tests recovery of historical values after simulated corruption
func TestRecoverFromCorruption(t *testing.T) {
	t.Skip("REthinking key history")
	// This test specifically targets ExtentKeyValueStore which can recover historical values
	dir := t.TempDir()
	storePath := filepath.Join(dir, "corruption_test")

	// Create the store
	store, err := ExtentCreator(storePath, 4096, testFileCapacity) //FIXME filesize
	if err != nil {
		fatalf(t, "action=Create store=ExtentKV path=%s err=%v", storePath, err)
	}

	// Define test key and values
	key := []byte("corrupt_test_key")
	value1 := []byte("first_value")
	value2 := []byte("second_value")
	value3 := []byte("third_value")

	// Write a sequence of values to create history
	if err := store.Put(key, value1); err != nil {
		fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(key), trimTo40(value1), err)
	}

	if err := store.Put(key, value2); err != nil {
		fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(key), trimTo40(value2), err)
	}

	if err := store.Put(key, value3); err != nil {
		fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(key), trimTo40(value3), err)
	}

	// Verify history before corruption
	history, err := store.KeyHistory(key)
	if err != nil {
		fatalf(t, "action=KeyHistory key=%s err=%v", trimTo40(key), err)
	}

	t.Logf("Found %d history entries before corruption", len(history))

	// Close the store
	store.Close()

	// Simulate corruption by truncating the keys.index file
	indexPath := filepath.Join(storePath, "keys.index")
	info, err := os.Stat(indexPath)
	if err != nil {
		fatalf(t, "action=Stat path=%s err=%v", indexPath, err)
	}

	// Truncate to simulate corruption - remove the last entry
	truncateSize := info.Size() - 8
	if err := os.Truncate(indexPath, truncateSize); err != nil {
		fatalf(t, "action=Truncate path=%s size=%d err=%v", indexPath, truncateSize, err)
	}

	t.Logf("Truncated index file from %d to %d bytes", info.Size(), truncateSize)

	// Try to reopen the store - this may fail due to corruption
	store, err = ExtentCreator(storePath, 4096, testFileCapacity) //FIXME filesize
	if err != nil {
		t.Logf("Note: Store reopened with error (expected with corruption): %v", err)
		// We'll still try to get history if possible
	}

	if store != nil {
		// Try to recover historical values
		history, err := store.KeyHistory(key)
		if err != nil {
			t.Logf("Error getting history after corruption: %v", err)
		}

		// Check what we recovered
		t.Logf("Recovered %d history entries after corruption", len(history))

		// We should at least recover some older values
		if len(history) > 0 {
			for i, val := range history {
				if bytes.Equal(val, value1) {
					t.Logf("Successfully recovered first value at position %d", i)
				} else if bytes.Equal(val, value2) {
					t.Logf("Successfully recovered second value at position %d", i)
				} else if bytes.Equal(val, value3) {
					t.Logf("Successfully recovered third value at position %d", i)
				}
			}
		}

		store.Close()
	}
}

// TestLargeHistory tests handling of a key with many historical values
func TestLargeHistory(t *testing.T) {
	t.Skip("REthinking key history")
	dir := t.TempDir()
	storePath := filepath.Join(dir, "large_history_test")

	// Create an ExtentKeyValueStore
	store, err := ExtentCreator(storePath, 4096, testFileCapacity) //FIXME filesize
	if err != nil {
		fatalf(t, "action=Create store=ExtentKV path=%s err=%v", storePath, err)
	}
	defer store.Close()

	// Define test key
	key := []byte("large_history_key")

	// Put many values
	valueCount := 100
	values := make([][]byte, valueCount)

	for i := 0; i < valueCount; i++ {
		values[i] = []byte(fmt.Sprintf("value_%d", i))
		if err := store.Put(key, values[i]); err != nil {
			fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(key), trimTo40(values[i]), err)
		}
	}

	// Get history
	history, err := store.KeyHistory(key)
	if err != nil {
		fatalf(t, "action=KeyHistory key=%s err=%v", trimTo40(key), err)
	}

	t.Logf("Requested %d values, found %d history entries", valueCount, len(history))

	// Check if we recovered a reasonable number of values
	if len(history) < valueCount/2 {
		fatalf(t, "action=KeyHistory key=%s expectedEntries>=%d got=%d", trimTo40(key), valueCount/2, len(history))
	}

	// Check some specific values
	for _, expectedVal := range []int{0, 10, 50, 99} {
		expectedValue := values[expectedVal]
		found := false

		for _, val := range history {
			if bytes.Equal(val, expectedValue) {
				found = true
				break
			}
		}

		if !found {
			fatalf(t, "action=KeyHistory key=%s missingValue=%s", trimTo40(key), trimTo40(expectedValue))
		}
	}
}
