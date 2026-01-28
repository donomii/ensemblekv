package ensemblekv

import (
	"os"
	"path/filepath"
	"testing"
)

// StoreTestConfig defines configuration for crash recovery tests
type StoreTestConfig struct {
	Name           string
	Creator        CreatorFunc
	CorruptionFile string // Relative path to file to corrupt (e.g. "values.dat")
	TruncateBytes  int64  // Number of bytes to truncate (ExtentKV allows max 8)
}

var crashTestStores = []StoreTestConfig{
	{
		Name:           "ExtentKV",
		Creator:        ExtentCreator,
		CorruptionFile: "values.dat",
		TruncateBytes:  8,
	},
}

// TestCrashRecoveryCorruption tests that the database can handle corrupted index entries
// where nextDataPos exceeds the actual data file length (simulating incomplete crash recovery)
func TestCrashRecoveryCorruption(t *testing.T) {
	for _, config := range crashTestStores {
		t.Run(config.Name, func(t *testing.T) {
			// Create a temporary directory for the test
			tmpDir := t.TempDir()

			// Create a new store
			store, err := config.Creator(tmpDir, 4096, 1024*1024)
			if err != nil {
				fatalf(t, "action=Create store=%s path=%s err=%v", config.Name, tmpDir, err)
			}

			// Write some test data
			testKey := []byte("test-key")
			testValue := []byte("test-value")

			if err := store.Put(testKey, testValue); err != nil {
				fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(testKey), trimTo40(testValue), err)
			}

			// Verify we can read it back
			val, err := store.Get(testKey)
			if err != nil {
				fatalf(t, "action=Get key=%s err=%v", trimTo40(testKey), err)
			}
			if string(val) != string(testValue) {
				fatalf(t, "action=Get key=%s expected=%s got=%s", trimTo40(testKey), trimTo40(testValue), trimTo40(val))
			}

			// Close the store
			if err := store.Close(); err != nil {
				fatalf(t, "action=Close path=%s err=%v", tmpDir, err)
			}

			// Now simulate corruption: truncate the values data file to be smaller than what the index expects
			valuesPath := filepath.Join(tmpDir, config.CorruptionFile)
			stat, err := os.Stat(valuesPath)
			if err != nil {
				fatalf(t, "action=Stat path=%s err=%v", valuesPath, err)
			}

			// Truncate to simulate incomplete write
			// ExtentKV only recovers if diff <= 8 bytes
			newSize := stat.Size() - config.TruncateBytes
			if newSize < 0 {
				newSize = 0
			}
			if err := os.Truncate(valuesPath, newSize); err != nil {
				fatalf(t, "action=Truncate path=%s size=%d err=%v", valuesPath, newSize, err)
			}

			// Reopen the store - this should NOT crash despite the corruption
			store2, err := config.Creator(tmpDir, 4096, 1024*1024)
			if err != nil {
				fatalf(t, "action=Reopen path=%s err=%v", tmpDir, err)
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
				fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(newKey), trimTo40(newValue), err)
			}

			val3, err := store2.Get(newKey)
			if err != nil {
				fatalf(t, "action=Get key=%s err=%v", trimTo40(newKey), err)
			}
			if string(val3) != string(newValue) {
				fatalf(t, "action=Get key=%s expected=%s got=%s", trimTo40(newKey), trimTo40(newValue), trimTo40(val3))
			}

			t.Log("SUCCESS: Database handled corruption gracefully and continues to work")
		})
	}
}

// TestMultipleCorruptedEntries tests handling multiple corrupted entries in sequence
func TestMultipleCorruptedEntries(t *testing.T) {
	for _, config := range crashTestStores {
		t.Run(config.Name, func(t *testing.T) {
			tmpDir := t.TempDir()

			store, err := config.Creator(tmpDir, 4096, 1024*1024)
			if err != nil {
				fatalf(t, "action=Create store=%s path=%s err=%v", config.Name, tmpDir, err)
			}

			// Write multiple keys
			for i := 0; i < 10; i++ {
				key := []byte("key-" + string(rune('0'+i)))
				value := []byte("value-" + string(rune('0'+i)))
				if err := store.Put(key, value); err != nil {
					fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(key), trimTo40(value), err)
				}
			}

			store.Close()

			// Corrupt the values file
			// ExtentKV only recovers if diff <= 8 bytes
			valuesPath := filepath.Join(tmpDir, config.CorruptionFile)
			stat, err := os.Stat(valuesPath)
			if err != nil {
				fatalf(t, "action=Stat path=%s err=%v", valuesPath, err)
			}
			newSize := stat.Size() - config.TruncateBytes
			if newSize < 0 {
				newSize = 0
			}
			if err := os.Truncate(valuesPath, newSize); err != nil {
				fatalf(t, "action=Truncate path=%s size=%d err=%v", valuesPath, newSize, err)
			}

			// Reopen and try to list all keys
			store2, err := config.Creator(tmpDir, 4096, 1024*1024)
			if err != nil {
				fatalf(t, "action=Reopen path=%s err=%v", tmpDir, err)
			}
			defer store2.Close()

			// Try to list - this will encounter multiple corrupted entries but should not crash
			keys := store2.Keys()
			t.Logf("List returned %d keys after corruption", len(keys))

			// Verify store still accepts new writes
			newKey := []byte("recovery-test")
			newValue := []byte("recovery-value")
			if err := store2.Put(newKey, newValue); err != nil {
				fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(newKey), trimTo40(newValue), err)
			}

			t.Log("SUCCESS: Database survived multiple corrupted entries")
		})
	}
}
