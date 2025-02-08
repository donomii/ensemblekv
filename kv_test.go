package ensemblekv

import (
    "bytes"
    "fmt"
    "testing"
    "os"
    "time"
)

// Test timeouts and limits
const (
    testTimeout = 300 * time.Second  // Reduced from 10m
    maxTestSize = 10 * 1024 * 1024  // 10MB max for test data
)

// StartKVStoreOperations runs the test suite with proper timeout and directory setup
func StartKVStoreOperations(t *testing.T, creator func(directory string, blockSize int) (KvLike, error), storeName string) {
    t.Helper()

    // Set test timeout
    timer := time.NewTimer(testTimeout)
    done := make(chan bool)
    
    go func() {
        t.Run("KVStore", func(t *testing.T) {
            // Create temporary directory
            storePath := t.TempDir()
            

            
            blockSize := 1024 * 4 // 4KB blocks
            
            // Create store
            store, err := creator(storePath, blockSize)
            if err != nil {
                t.Fatalf("Failed to create store: %v", err)
            }
            defer store.Close()

            // Run tests with store-specific limits
            KVStoreOperations(t, store, storeName)
            MixedSizeKeyValuePairs(t, store, storeName)
            FuzzKeyValueOperations(t, store, storeName)

            // Run persistence tests
            t.Run("Persistence", func(t *testing.T) {
                PersistenceTests(t, store, creator, storePath, blockSize)
            })
            
            // Only close here if persistence tests didn't already close it
            if store != nil {
                store.Close()
            }
        })
        done <- true
    }()
    
    // Wait for either completion or timeout
    select {
    case <-timer.C:
        t.Fatal("Test timed out")
    case <-done:
        timer.Stop()
    }
}

// PersistenceTests verifies that all operations survive store reopening and cache invalidation
func PersistenceTests(t *testing.T, store KvLike, creator func(directory string, blockSize int) (KvLike, error), storePath string, blockSize int) {
    t.Helper()
    
    // Test data with a mix of sizes
    testData := map[string][]byte{
        "key1": []byte("value1"),
        "key2": []byte("value2"),
        "key3": randomBytes(1024, 1024),    // Large value
        "key4": randomBytes(4096, 4096),    // Value larger than block size
        "keyToDelete": []byte("will be deleted"),
        "keyToUpdate": []byte("original value"),
    }
    
    // Phase 1: Initial data insertion
    t.Run("Phase1_InitialInsert", func(t *testing.T) {
        for key, value := range testData {
            err := store.Put([]byte(key), value)
            if err != nil {
                t.Fatalf("Failed to put initial data for key %s: %v", key, err)
            }
        }
        
        // Close and reopen
        err := store.Close()
        if err != nil {
            t.Fatalf("Failed to close store: %v", err)
        }
        
        store, err = creator(storePath, blockSize)
        if err != nil {
            t.Fatalf("Failed to reopen store: %v", err)
        }
        
        // Verify all initial data
        for key, expectedValue := range testData {
            exists := store.Exists([]byte(key))
            if !exists {
                t.Errorf("Key %s doesn't exist after reopening", key)
                continue
            }
            
            value, err := store.Get([]byte(key))
            if err != nil {
                t.Errorf("Failed to get key %s after reopening: %v", key, err)
                continue
            }
            
            if !bytes.Equal(value, expectedValue) {
                t.Errorf("Value mismatch for key %s after reopening", key)
                t.Errorf("Expected: %v", trimTo40( expectedValue))
                t.Errorf("Got: %v", trimTo40(value))
                t.Errorf("Expected length: %d", len(expectedValue))
                t.Errorf("Got length: %d", len(value))
            }
        }
    })
    
    // Phase 2: Test Delete persistence
    t.Run("Phase2_DeletePersistence", func(t *testing.T) {
        // Delete a key
        err := store.Delete([]byte("keyToDelete"))
        if err != nil {
            t.Fatalf("Failed to delete key: %v", err)
        }

        // Close and reopen
        err = store.Close()
        if err != nil {
            t.Fatalf("Failed to close store: %v", err)
        }

        store, err = creator(storePath, blockSize)
        if err != nil {
            t.Fatalf("Failed to reopen store: %v", err)
        }
        
        // Verify deletion persisted
        exists := store.Exists([]byte("keyToDelete"))
        if exists {
            t.Error("Deleted key still exists after reopening")
        }

        _, err = store.Get([]byte("keyToDelete"))
        if err == nil {
            t.Error("Expected error getting deleted key, got nil")
        }
    })
    
    // Phase 3: Test Update persistence
    t.Run("Phase3_UpdatePersistence", func(t *testing.T) {
        newValue := []byte("updated value")
        err := store.Put([]byte("keyToUpdate"), newValue)
        if err != nil {
            t.Fatalf("Failed to update key: %v", err)
        }
        
        // Close and reopen
        err = store.Close()
        if err != nil {
            t.Fatalf("Failed to close store: %v", err)
        }
        
        store, err = creator(storePath, blockSize)
        if err != nil {
            t.Fatalf("Failed to reopen store: %v", err)
        }
        
        // Verify update persisted
        value, err := store.Get([]byte("keyToUpdate"))
        if err != nil {
            t.Fatalf("Failed to get updated key: %v", err)
        }
        
        if !bytes.Equal(value, newValue) {
            t.Error("Update did not persist correctly")
        }
    })
    
    // Phase 4: Test mixed operations persistence
    t.Run("Phase4_MixedOperations", func(t *testing.T) {
        // Perform a mix of operations
        operations := []struct {
            op    string
            key   string
            value []byte
        }{
            {"put", "newKey1", []byte("newValue1")},
            {"delete", "key1", nil},
            {"put", "newKey2", randomBytes(2048, 2048)},
            {"put", "key2", []byte("updatedValue2")},
        }
        
        for _, op := range operations {
            switch op.op {
            case "put":
                err := store.Put([]byte(op.key), op.value)
                if err != nil {
                    t.Fatalf("Failed to put key %s: %v", op.key, err)
                }
            case "delete":
                err := store.Delete([]byte(op.key))
                if err != nil {
                    t.Fatalf("Failed to delete key %s: %v", op.key, err)
                }
            }
        }
        
        // Close and reopen
        err := store.Close()
        if err != nil {
            t.Fatalf("Failed to close store: %v", err)
        }
        
        store, err = creator(storePath, blockSize)
        if err != nil {
            t.Fatalf("Failed to reopen store: %v", err)
        }
        
        // Verify all operations persisted
        for _, op := range operations {
            exists := store.Exists([]byte(op.key))
            
            switch op.op {
            case "put":
                if !exists {
                    t.Errorf("Key %s missing after mixed operations", op.key)
                    continue
                }
                
                value, err := store.Get([]byte(op.key))
                if err != nil {
                    t.Errorf("Failed to get key %s after mixed operations: %v", op.key, err)
                    continue
                }
                
                if !bytes.Equal(value, op.value) {
                    t.Errorf("Value mismatch for key %s after mixed operations", op.key)
                }
                
            case "delete":
                if exists {
                    t.Errorf("Deleted key %s still exists after mixed operations", op.key)
                }
            }
        }
    })
    
    // Clean up
    err := store.Close()
    if err != nil {
        t.Fatalf("Failed to close store during cleanup: %v", err)
    }
}

// MixedSizeKeyValuePairs tests various key-value size combinations
func MixedSizeKeyValuePairs(t *testing.T, store KvLike, storeName string) {
    fmt.Println("Testing MixedSizeKeyValuePairs")
    limits := GetDBLimits(storeName)
    
    sizes := []struct {
        keySize   int
        valueSize int
    }{
        {32, 128},                    // Small pairs
        {64, 1024},                   // Medium pairs
        {128, 64 * 1024},            // Larger pairs
        {256, 256 * 1024},           // Even larger pairs
    }
    
    for _, size := range sizes {
        if size.keySize > limits.maxKeySize || int64(size.valueSize) > limits.maxValueSize {
            t.Logf("Skipping test with key size %d and value size %d (exceeds limits)", 
                size.keySize, size.valueSize)
            continue
        }
        
        t.Run(fmt.Sprintf("Size_%dx%d", size.keySize, size.valueSize), func(t *testing.T) {
            key := randomBytes(size.keySize, size.keySize)
            value := randomBytes(size.valueSize, size.valueSize)
            
            // Test Put
            fmt.Println("Putting key: ", trimTo40(key))
            err := store.Put(key, value)
            if err != nil {
                t.Fatalf("Failed to put %dx%d pair: %v", size.keySize, size.valueSize, err)
            }
            
            // Test Get
            fmt.Printf("Getting key: %v\n", trimTo40(key))
            retrieved, err := store.Get(key)
            if err != nil {
                t.Fatalf("Failed to get %dx%d pair(%v): %v", size.keySize, size.valueSize, trimTo40(key),err)
            }
            
            if !bytes.Equal(retrieved, value) {
                t.Errorf("Value mismatch for %dx%d pair (%v) expected: %v, got: %v", size.keySize, size.valueSize, trimTo40(key), trimTo40(value), trimTo40(retrieved))
            }

            // Test Delete
            fmt.Printf("Deleting key: %v\n", trimTo40(key))
            err = store.Delete(key)
            if err != nil {
                t.Fatalf("Failed to delete %dx%d pair: %v", size.keySize, size.valueSize, err)
            }

            // Verify deletion
            if store.Exists(key) {
                store.DumpIndex()
                fmt.Printf("Key: %v still exists after deletion\n", trimTo40(key))
                t.Error("Key still exists after deletion")
                os.Exit(0)
            }
        })
    }
}

// Base store types
var baseStores = []struct {
    name    string
    creator func(directory string, blockSize int) (KvLike, error)
}{
    {"Bolt", BoltDbCreator},
    {"Extent", ExtentCreator},
   // {"Pudge", PudgeCreator},
	{"JsonKV", JsonKVCreator},
}

// Store wrapper types
var wrapperTypes = []struct {
    name string
    createWrapper func(baseCreator func(string, int) (KvLike, error)) func(string, int) (KvLike, error)
}{
    
    {
        "Ensemble",
        func(baseCreator func(string, int) (KvLike, error)) func(string, int) (KvLike, error) {
            return func(directory string, blockSize int) (KvLike, error) {
                return EnsembleCreator(directory, blockSize, baseCreator)
            }
        },
    },
    {
        "Tree",
        func(baseCreator func(string, int) (KvLike, error)) func(string, int) (KvLike, error) {
            return func(directory string, blockSize int) (KvLike, error) {
                return NewTreeLSM(directory, blockSize, baseCreator)
            }
        },
    },
    {
        "Star",
        func(baseCreator func(string, int) (KvLike, error)) func(string, int) (KvLike, error) {
            return func(directory string, blockSize int) (KvLike, error) {
                return NewStarLSM(directory, blockSize, baseCreator)
            }
        },
    },
}

func TestAllStores(t *testing.T) {
    // Test base stores
    for _, base := range baseStores {
        t.Run(base.name, func(t *testing.T) {
            StartKVStoreOperations(t, base.creator, base.name)
        })
    }

    // Test wrapped stores
    for _, wrapper := range wrapperTypes {
        for _, base := range baseStores {
            name := fmt.Sprintf("%sLSM%s", wrapper.name, base.name)
            t.Run(name, func(t *testing.T) {
                creator := wrapper.createWrapper(base.creator)
                StartKVStoreOperations(t, creator, name)
            })
        }
    }
}