package ensemblekv

import (
    "bytes"
    "fmt"
    "math/rand"
    "testing"
    "time"
    "encoding/hex"
)

// DBLimits defines the size constraints for different database types
type DBLimits struct {
    maxKeySize   int
    maxValueSize int64
    name         string
}


// Fuzz test parameters
const (
    minOperations = 50
    maxOperations = 100
    minKeySize    = 1
    minValueSize  = 1
)

// Helper to generate random byte slices with size constraints
func randomBytes(minSize, maxSize int) []byte {
    var size int
    if minSize == maxSize {
        size = minSize // If min and max are equal, use that size
    } else if minSize < maxSize {
        size = rand.Intn(maxSize-minSize+1) + minSize // Add 1 to make maxSize inclusive
    } else {
        // Handle invalid input by using minSize
        size = minSize
    }
    
    data := make([]byte, size)
    rand.Read(data)
    return data
}

// Helper function to trim byte slices for display
func trimTo40(data []byte) string {
    if len(data) > 40 {
        return fmt.Sprintf("%s...", hex.EncodeToString(data[:40]))
    }
    return hex.EncodeToString(data)
}

// LogEntry holds information about each operation
type LogEntry struct {
    OpNum       int
    Operation   string
    Key         string
    Value       string
    Description string
}

// RingBuffer keeps the last 50 operations
type RingBuffer struct {
    entries []LogEntry
    index   int
    size    int
}

func NewRingBuffer(size int) *RingBuffer {
    return &RingBuffer{
        entries: make([]LogEntry, size),
        size:    size,
    }
}

func (rb *RingBuffer) Add(entry LogEntry) {
    rb.entries[rb.index] = entry
    rb.index = (rb.index + 1) % rb.size
}

func (rb *RingBuffer) Dump() {
    fmt.Println("\nOperation log (last 50 operations):")
    for i := 0; i < rb.size; i++ {
        entry := rb.entries[(rb.index+i)%rb.size]
        if entry.Operation != "" {
            fmt.Printf("#%03d [%s] Key: %s, Value: %s, Description: %s\n",
                entry.OpNum, entry.Operation, entry.Key, entry.Value, entry.Description)
        }
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

    case "ExtentKeyValueStore", "EnsembleExtentStore", "LineLSMExtentStore", "TreeLSMExtentStore":
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

func FuzzKeyValueOperations(t *testing.T, store KvLike, storeName string) {
    limits := GetDBLimits(storeName)
    
    t.Run("FuzzTestRandomOperations", func(t *testing.T) {
        rand.Seed(time.Now().UnixNano())
        numOperations := rand.Intn(maxOperations-minOperations) + minOperations

        keys := make(map[string][]byte)
        ringBuffer := NewRingBuffer(50)

        for i := 0; i < numOperations; i++ {
            operation := rand.Intn(3) // 0 = Put, 1 = Get, 2 = Delete
            
            // Generate size within DB limits
            keySize := rand.Intn(limits.maxKeySize-minKeySize) + minKeySize
            valueSize := rand.Intn(int(limits.maxValueSize/2)-minValueSize) + minValueSize

            switch operation {
            case 0: // Put
                key := randomBytes(minKeySize, keySize)
                value := randomBytes(minValueSize, valueSize)
                
                err := store.Put(key, value)
                ringBuffer.Add(LogEntry{i + 1, "Put", trimTo40(key), trimTo40(value), "Put operation"})

                if err != nil {
                    ringBuffer.Dump()
                    t.Fatalf("[%s] Failed to put key/value pair: %v", limits.name, err)
                }
                keys[string(key)] = value

            case 1: // Get
                if len(keys) == 0 {
                    continue
                }
                
                randomKey := getRandomKey(keys)
                expectedValue := keys[randomKey]

                retrievedValue, err := store.Get([]byte(randomKey))
                ringBuffer.Add(LogEntry{i + 1, "Get", trimTo40([]byte(randomKey)), trimTo40(expectedValue), "Get operation"})

                if err != nil {
                    ringBuffer.Dump()
                    store.DumpIndex()
                    t.Fatalf("[%s] Failed to get key: %s, %v", limits.name, trimTo40([]byte(randomKey)), err)
                }

                if !bytes.Equal(retrievedValue, expectedValue) {
                    ringBuffer.Dump()
                    t.Errorf("[%s] Value mismatch for key %s. Expected: %s, Got: %s",
                        limits.name, trimTo40([]byte(randomKey)), trimTo40(expectedValue), trimTo40(retrievedValue))
                }

            case 2: // Delete
                if len(keys) == 0 {
                    continue
                }
                
                randomKey := getRandomKey(keys)

                err := store.Delete([]byte(randomKey))
                ringBuffer.Add(LogEntry{i + 1, "Delete", trimTo40([]byte(randomKey)), "", "Delete operation"})

                if err != nil {
                    ringBuffer.Dump()
                    t.Fatalf("[%s] Failed to delete key: %v", limits.name, err)
                }

                delete(keys, randomKey)

                if store.Exists([]byte(randomKey)) {
                    ringBuffer.Dump()
                    t.Errorf("[%s] Key %s still exists after deletion", limits.name, trimTo40([]byte(randomKey)))
                }
            }
        }
    })
}

// Helper to get a random key from the map
func getRandomKey(m map[string][]byte) string {
    for k := range m {
        return k
    }
    return ""
}

// KVStoreOperations performs basic operations test with DB-specific limits
func KVStoreOperations(t *testing.T, store KvLike, storeName string) {
    limits := GetDBLimits(storeName)

    t.Run("Basic Put and Get", func(t *testing.T) {
        key := []byte("test_key")
        value := []byte("test_value")
        
        err := store.Put(key, value)
        if err != nil {
            t.Fatalf("Failed to put value: %v", err)
        }

        retrieved, err := store.Get(key)
        if err != nil {
            t.Fatalf("Failed to get value: %v", err)
        }

        if !bytes.Equal(retrieved, value) {
            t.Errorf("Value mismatch. Expected %s, got %s", value, retrieved)
        }
    })

    t.Run("Large Key-Value Pairs", func(t *testing.T) {
        keySize := limits.maxKeySize / 2
        valueSize := int(limits.maxValueSize / 2)

        key := randomBytes(minKeySize, keySize)
        value := randomBytes(minValueSize, valueSize)

        err := store.Put(key, value)
        if err != nil {
            t.Fatalf("Failed to put large key/value pair: %v", err)
        }

        retrieved, err := store.Get(key)
        if err != nil {
            t.Fatalf("Failed to get large value: %v", err)
        }

        if !bytes.Equal(retrieved, value) {
            t.Error("Retrieved large value does not match original")
        }
    })

    t.Run("Mixed Size Operations", func(t *testing.T) {
        sizes := []struct {
            keySize   int
            valueSize int
        }{
            {32, 128},
            {64, 1024},
            {128, 1024 * 1024},
        }

        for _, size := range sizes {
            if size.keySize > limits.maxKeySize || int64(size.valueSize) > limits.maxValueSize {
                t.Logf("Skipping test with key size %d and value size %d as it exceeds DB limits",
                    size.keySize, size.valueSize)
                continue
            }

            key := randomBytes(minKeySize, size.keySize)
            value := randomBytes(minValueSize, size.valueSize)

            err := store.Put(key, value)
            if err != nil {
                t.Fatalf("Failed to put key/value pair of size %d/%d: %v",
                    size.keySize, size.valueSize, err)
            }

            retrieved, err := store.Get(key)
            if err != nil {
                t.Fatalf("Failed to get value of size %d: %v",
                    size.valueSize, err)
            }

            if !bytes.Equal(retrieved, value) {
                t.Errorf("Value mismatch for size %d/%d",
                    size.keySize, size.valueSize)
            }
        }
    })
}