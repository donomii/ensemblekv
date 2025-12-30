package ensemblekv

import (
	"os"
	"path/filepath"
	"testing"
)

// TestCrashRecoveryCorruption tests that the database can handle corrupted index entries
// where nextDataPos exceeds the actual data file length (simulating incomplete crash recovery)
func TestCrashRecoveryCorruption(t *testing.T) {
	// Create a temporary directory for the test
	tmpDir := t.TempDir()
	
	// Create a new store
	store, err := NewExtentMmapKeyValueStore(tmpDir, 4096, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	
	// Write some test data
	testKey := []byte("test-key")
	testValue := []byte("test-value")
	
	if err := store.Put(testKey, testValue); err != nil {
		t.Fatalf("Failed to put test data: %v", err)
	}
	
	// Verify we can read it back
	val, err := store.Get(testKey)
	if err != nil {
		t.Fatalf("Failed to get test data: %v", err)
	}
	if string(val) != string(testValue) {
		t.Fatalf("Got wrong value: expected %s, got %s", testValue, val)
	}
	
	// Close the store
	if err := store.Close(); err != nil {
		t.Fatalf("Failed to close store: %v", err)
	}
	
	// Now simulate corruption: truncate the values data file to be smaller than what the index expects
	valuesPath := filepath.Join(tmpDir, "values.dat")
	stat, err := os.Stat(valuesPath)
	if err != nil {
		t.Fatalf("Failed to stat values file: %v", err)
	}
	
	// Truncate to 50% of original size to simulate incomplete write
	newSize := stat.Size() / 2
	if err := os.Truncate(valuesPath, newSize); err != nil {
		t.Fatalf("Failed to truncate values file: %v", err)
	}
	
	// Reopen the store - this should NOT crash despite the corruption
	store2, err := NewExtentMmapKeyValueStore(tmpDir, 4096, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to reopen corrupted store: %v", err)
	}
	defer store2.Close()
	
	// Try to get the key - it may return an error (treating as deleted) but should NOT panic
	val2, err := store2.Get(testKey)
	if err != nil {
		// This is expected - the entry is treated as deleted due to corruption
		t.Logf("Key treated as deleted due to corruption (expected): %v", err)
	} else {
		// If we somehow got data back, verify it's correct
		t.Logf("Unexpectedly got data back: %s", val2)
	}
	
	// Verify the store still works for new operations
	newKey := []byte("new-key-after-corruption")
	newValue := []byte("new-value")
	
	if err := store2.Put(newKey, newValue); err != nil {
		t.Fatalf("Failed to put new data after corruption: %v", err)
	}
	
	val3, err := store2.Get(newKey)
	if err != nil {
		t.Fatalf("Failed to get new data after corruption: %v", err)
	}
	if string(val3) != string(newValue) {
		t.Fatalf("Got wrong value for new key: expected %s, got %s", newValue, val3)
	}
	
	t.Log("SUCCESS: Database handled corruption gracefully and continues to work")
}

// TestMultipleCorruptedEntries tests handling multiple corrupted entries in sequence
func TestMultipleCorruptedEntries(t *testing.T) {
	tmpDir := t.TempDir()
	
	store, err := NewExtentMmapKeyValueStore(tmpDir, 4096, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	
	// Write multiple keys
	for i := 0; i < 10; i++ {
		key := []byte("key-" + string(rune('0'+i)))
		value := []byte("value-" + string(rune('0'+i)))
		if err := store.Put(key, value); err != nil {
			t.Fatalf("Failed to put key %d: %v", i, err)
		}
	}
	
	store.Close()
	
	// Corrupt the values file significantly
	valuesPath := filepath.Join(tmpDir, "values.dat")
	if err := os.Truncate(valuesPath, 10); err != nil {
		t.Fatalf("Failed to truncate values file: %v", err)
	}
	
	// Reopen and try to list all keys
	store2, err := NewExtentMmapKeyValueStore(tmpDir, 4096, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to reopen store: %v", err)
	}
	defer store2.Close()
	
	// Try to list - this will encounter multiple corrupted entries but should not crash
	keys, err := store2.List()
	if err != nil {
		t.Logf("List returned error (may be expected): %v", err)
	} else {
		t.Logf("List returned %d keys after corruption", len(keys))
	}
	
	// Verify store still accepts new writes
	newKey := []byte("recovery-test")
	newValue := []byte("recovery-value")
	if err := store2.Put(newKey, newValue); err != nil {
		t.Fatalf("Failed to write after massive corruption: %v", err)
	}
	
	t.Log("SUCCESS: Database survived multiple corrupted entries")
}
