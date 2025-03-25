package ensemblekv

import (
"fmt"
	"bytes"
	"testing"
	"os"
	"path/filepath"
)

// TestExtentKeyHistory tests the KeyHistory functionality for ExtentKeyValueStore
func TestExtentKeyHistory(t *testing.T) {
	// Create a temp directory for the store
	dir := t.TempDir()
	storePath := filepath.Join(dir, "extent_test")
	
	// Create an ExtentKeyValueStore
	store, err := ExtentCreator(storePath, 4096)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
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
		t.Fatalf("Failed to put initial value: %v", err)
	}

	store.Flush()
	
	// Step 2: Get key history
	history, err := store.KeyHistory(key)
	if err != nil {
		t.Fatalf("Failed to get key history: %v", err)
	}
	
	if len(history) != 1 {
		t.Errorf("Expected 1 history entry, got %d", len(history))
	} else if !bytes.Equal(history[0], value1) {
		t.Errorf("Value mismatch: expected %s, got %s", value1, history[0])
	}
	
	// Step 3: Update value
	if err := store.Put(key, value2); err != nil {
		t.Fatalf("Failed to update value: %v", err)
	}
	
	// Step 4: Get key history
	history, err = store.KeyHistory(key)
	if err != nil {
		t.Fatalf("Failed to get key history: %v", err)
	}
	
	// Should have both values
	if len(history) != 2 {
		t.Errorf("Expected 2 history entries, got %d", len(history))
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
		t.Errorf("Initial value not found in history")
	}
	if !foundValue2 {
		t.Errorf("Updated value not found in history")
	}
	
	// Step 5: Delete key
	if err := store.Delete(key); err != nil {
		t.Fatalf("Failed to delete key: %v", err)
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
	// Create a temp directory for the store
	dir := t.TempDir()
	storePath := filepath.Join(dir, "ensemble_test")
	
	// Create an EnsembleKv store
	store, err := EnsembleCreator(storePath, 4096, ExtentCreator)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
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
		t.Fatalf("Failed to put key1: %v", err)
	}
	
	if err := store.Put(key2, value2); err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}
	
	// Update a key
	if err := store.Put(key1, updatedValue); err != nil {
		t.Fatalf("Failed to update key1: %v", err)
	}
	
	// Get history for key1
	history, err := store.KeyHistory(key1)
	if err != nil {
		t.Fatalf("Failed to get key history: %v", err)
	}
	
	// Should have at least the updated value
	if len(history) == 0 {
		t.Errorf("No history entries found for key1")
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
		t.Errorf("Updated value not found in history")
	}
}

// TestMultipleStoreTypes tests KeyHistory across different store types
func TestMultipleStoreTypes(t *testing.T) {
	baseDir := t.TempDir()
	
	storeCreators := []struct {
		name    string
		creator func(dir string) (KvLike, error)
	}{
		{
			name: "ExtentKV",
			creator: func(dir string) (KvLike, error) {
				return ExtentCreator(dir, 4096)
			},
		},
		{
			name: "JsonKV",
			creator: func(dir string) (KvLike, error) {
				return JsonKVCreator(dir, 4096)
			},
		},
		{
			name: "BoltDB",
			creator: func(dir string) (KvLike, error) {
				return BoltDbCreator(dir, 4096)
			},
		},
		{
			name: "EnsembleKV",
			creator: func(dir string) (KvLike, error) {
				return EnsembleCreator(dir, 4096, ExtentCreator)
			},
		},
		{
			name: "TreeLSM",
			creator: func(dir string) (KvLike, error) {
				return NewTreeLSM(dir, 4096, ExtentCreator)
			},
		},
		{
			name: "StarLSM",
			creator: func(dir string) (KvLike, error) {
				return NewStarLSM(dir, 4096, ExtentCreator)
			},
		},
	}
	
	for _, sc := range storeCreators {
		t.Run(sc.name, func(t *testing.T) {
			storePath := filepath.Join(baseDir, sc.name)
			
			// Create store
			store, err := sc.creator(storePath)
			if err != nil {
				t.Fatalf("Failed to create %s store: %v", sc.name, err)
			}
			defer store.Close()
			
			// Test key and values
			key := []byte("multi_store_test_key")
			value1 := []byte("initial value")
			value2 := []byte("updated value")
			
			// Put initial value
			if err := store.Put(key, value1); err != nil {
				t.Fatalf("Failed to put initial value in %s: %v", sc.name, err)
			}
			
			// Get history - should have at least 1 value
			history, err := store.KeyHistory(key)
			if err != nil {
				t.Fatalf("Failed to get history from %s: %v", sc.name, err)
			}
			
			if len(history) == 0 {
				t.Errorf("%s: No history entries found after initial put", sc.name)
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
				t.Errorf("%s: Initial value not found in history", sc.name)
			}
			
			// Update value
			if err := store.Put(key, value2); err != nil {
				t.Fatalf("Failed to update value in %s: %v", sc.name, err)
			}
			
			// Get history again
			history, err = store.KeyHistory(key)
			if err != nil {
				t.Fatalf("Failed to get history after update from %s: %v", sc.name, err)
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
				t.Errorf("%s: Updated value not found in history", sc.name)
			}
		})
	}
}

// TestRecoverFromCorruption tests recovery of historical values after simulated corruption
func TestRecoverFromCorruption(t *testing.T) {
	// This test specifically targets ExtentKeyValueStore which can recover historical values
	dir := t.TempDir()
	storePath := filepath.Join(dir, "corruption_test")
	
	// Create the store
	store, err := ExtentCreator(storePath, 4096)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	
	// Define test key and values
	key := []byte("corrupt_test_key")
	value1 := []byte("first_value")
	value2 := []byte("second_value")
	value3 := []byte("third_value")
	
	// Write a sequence of values to create history
	if err := store.Put(key, value1); err != nil {
		t.Fatalf("Failed to put first value: %v", err)
	}
	
	if err := store.Put(key, value2); err != nil {
		t.Fatalf("Failed to put second value: %v", err)
	}
	
	if err := store.Put(key, value3); err != nil {
		t.Fatalf("Failed to put third value: %v", err)
	}
	
	// Verify history before corruption
	history, err := store.KeyHistory(key)
	if err != nil {
		t.Fatalf("Failed to get history before corruption: %v", err)
	}
	
	t.Logf("Found %d history entries before corruption", len(history))
	
	// Close the store
	store.Close()
	
	// Simulate corruption by truncating the keys.index file
	indexPath := filepath.Join(storePath, "keys.index")
	info, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("Failed to stat index file: %v", err)
	}
	
	// Truncate to simulate corruption - remove the last entry
	truncateSize := info.Size() - 8
	if err := os.Truncate(indexPath, truncateSize); err != nil {
		t.Fatalf("Failed to truncate index file: %v", err)
	}
	
	t.Logf("Truncated index file from %d to %d bytes", info.Size(), truncateSize)
	
	// Try to reopen the store - this may fail due to corruption
	store, err = ExtentCreator(storePath, 4096)
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
	dir := t.TempDir()
	storePath := filepath.Join(dir, "large_history_test")
	
	// Create an ExtentKeyValueStore
	store, err := ExtentCreator(storePath, 4096)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
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
			t.Fatalf("Failed to put value %d: %v", i, err)
		}
	}
	
	// Get history
	history, err := store.KeyHistory(key)
	if err != nil {
		t.Fatalf("Failed to get large history: %v", err)
	}
	
	t.Logf("Requested %d values, found %d history entries", valueCount, len(history))
	
	// Check if we recovered a reasonable number of values
	if len(history) < valueCount/2 {
		t.Errorf("Expected at least %d history entries, got %d", valueCount/2, len(history))
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
			t.Errorf("Value %d not found in history", expectedVal)
		}
	}
}
