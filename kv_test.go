package ensemblekv

import (
    "bytes"
    "fmt"
    "path/filepath"
    "testing"
    "time"
)

// Test timeouts and limits
const (
    testTimeout = 30 * time.Second  // Reduced from 10m
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
            dir := t.TempDir()
            
            // Adjust paths based on store type
            var storePath string
            switch storeName {
            case "BoltDB", "LineBoltStore", "EnsembleBoltDbStore":
                storePath = filepath.Join(dir, "bolt.db")
            default:
                storePath = dir
            }
            
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
// GetDBLimits returns the appropriate limits for different database types
func GetDBLimits(storeName string) DBLimits {
    // Constants for commonly used sizes
    const (
        _1KB = 1024
        _1MB = 1024 * 1024
        _1GB = 1024 * 1024 * 1024
        
        // Bolt-specific limits
        boltMaxKeySize   = 32768      // 2^15 bytes
        boltMaxValueSize = 1073741822 // Bolt's max value size (~1GB)
        
        // Test-friendly sizes for larger values
        testMaxKeySize   = _1MB       // 1MB for test keys
        testMaxValueSize = 10 * _1MB  // 10MB for test values
    )
    
    switch storeName {
    case "BoltDB", "LineBoltStore", "EnsembleBoltDbStore", "LineLSMBoltStore", "TreeLSMBoltStore":
        return DBLimits{
            maxKeySize:   boltMaxKeySize,   // Bolt's hard limit
            maxValueSize: testMaxValueSize, // Limited for testing speed
            name:         storeName,
        }
        
    case "BarrelDB", "EnsembleBarrelDbStore", "LineLSMBarrelDbStore":
        return DBLimits{
            maxKeySize:   testMaxKeySize,   // 1MB reasonable limit
            maxValueSize: testMaxValueSize, // Limited for testing speed
            name:         storeName,
        }
        
    case "ExtentKeyValueStore", "EnsembleExtentStore", "LineLSMExtentStore":
        return DBLimits{
            maxKeySize:   testMaxKeySize,   // Limited for testing speed
            maxValueSize: testMaxValueSize, // Limited for testing speed
            name:         storeName,
        }
        
    default:
        // Conservative default limits matching Bolt's constraints
        return DBLimits{
            maxKeySize:   boltMaxKeySize,   // Safe default matching Bolt
            maxValueSize: testMaxValueSize, // Limited for testing speed
            name:         storeName,
        }
    }
}

// Updated test functions
func TestBoltDbStore(t *testing.T) {
    StartKVStoreOperations(t, BoltDbCreator, "BoltDB")
}

func TestBarrelDbStore(t *testing.T) {
    StartKVStoreOperations(t, BarrelDbCreator, "BarrelDB")
}

func TestExtentStore(t *testing.T) {
    StartKVStoreOperations(t, ExtentCreator, "ExtentKeyValueStore")
}

func TestLineLSMBoltStore(t *testing.T) {
    StartKVStoreOperations(t, func(directory string, blockSize int) (KvLike, error) {
        return LineLSMCreator(directory, blockSize, BoltDbCreator)
    }, "LineBoltStore")
}

func TestLineLSMBarrelStore(t *testing.T) {
	StartKVStoreOperations(t, func(directory string, blockSize int) (KvLike, error) {
		return LineLSMCreator(directory, blockSize, BarrelDbCreator)
	}, "LineLSMBarrelStore")
}


func TestLineLSMExtentStore(t *testing.T) {
    StartKVStoreOperations(t, func(directory string, blockSize int) (KvLike, error) {
        return LineLSMCreator(directory, blockSize, ExtentCreator)
    }, "ExtentKeyValueStore")
}

func TestEnsembleBoltDbStore(t *testing.T) {
	StartKVStoreOperations(t, func(directory string, blockSize int) (KvLike, error) {
        return EnsembleCreator(directory, blockSize, BoltDbCreator)
    }, "LineBoltStore")
}

func TestEnsembleBarrelDbStore(t *testing.T) {
	StartKVStoreOperations(t, func(directory string, blockSize int) (KvLike, error) {
		return EnsembleCreator(directory, blockSize, BarrelDbCreator)
	}, "LineLSMBarrelStore")
}

func TestEnsembleExtentStore(t *testing.T) {
	StartKVStoreOperations(t, func(directory string, blockSize int) (KvLike, error) {
		return EnsembleCreator(directory, blockSize, ExtentCreator)
	}, "EnsembleExtentKeyValueStore")
}

func TestTreeLSMBoltStore(t *testing.T) {
	StartKVStoreOperations(t, func(directory string, blockSize int) (KvLike, error) {
		return NewTreeLSM(directory, blockSize, BoltDbCreator)
	}, "TreeLSMBoltStore")
}

func TestTreeLSMBarrelStore(t *testing.T) {
	StartKVStoreOperations(t, func(directory string, blockSize int) (KvLike, error) {
		return NewTreeLSM(directory, blockSize, BarrelDbCreator)
	}, "TreeLSMBarrelStore")
}

func TestTreeLSMExtentStore(t *testing.T) {
	StartKVStoreOperations(t, func(directory string, blockSize int) (KvLike, error) {
		return NewTreeLSM(directory, blockSize, ExtentCreator)
	}, "TreeExtentKeyValueStore")
}


