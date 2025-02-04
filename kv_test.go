package ensemblekv

import (
    "bytes"
    "fmt"
    "testing"
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

// MixedSizeKeyValuePairs tests various key-value size combinations
func MixedSizeKeyValuePairs(t *testing.T, store KvLike, storeName string) {
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
            err := store.Put(key, value)
            if err != nil {
                t.Fatalf("Failed to put %dx%d pair: %v", size.keySize, size.valueSize, err)
            }
            
            // Test Get
            retrieved, err := store.Get(key)
            if err != nil {
                t.Fatalf("Failed to get %dx%d pair: %v", size.keySize, size.valueSize, err)
            }
            
            if !bytes.Equal(retrieved, value) {
                t.Errorf("Value mismatch for %dx%d pair", size.keySize, size.valueSize)
            }
            
            // Test Delete
            err = store.Delete(key)
            if err != nil {
                t.Fatalf("Failed to delete %dx%d pair: %v", size.keySize, size.valueSize, err)
            }
            
            // Verify deletion
            if store.Exists(key) {
                t.Error("Key still exists after deletion")
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
        "Line",
        func(baseCreator func(string, int) (KvLike, error)) func(string, int) (KvLike, error) {
            return func(directory string, blockSize int) (KvLike, error) {
                return LineLSMCreator(directory, blockSize, baseCreator)
            }
        },
    },
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