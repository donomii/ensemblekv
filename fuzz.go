package ensemblekv

import (
	"bytes"
	"math/rand"
	"testing"
	"time"
	"fmt"
	"encoding/hex"
)
var runs = 50
var maxKeySize = 32000


// Helper to generate random byte slices
func randomBytes(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

// Helper function to trim byte slices for display
func trimTo40(data []byte) string {
	if len(data) > 40 {
		return fmt.Sprintf("%s...", data[:40])
	}
	return string(data)
}
// LogEntry holds information about each operation
type LogEntry struct {
	OpNum       int    // Operation number to indicate order
	Operation   string // Type of operation: Put, Get, Delete
	Key         string // Key used in the operation
	Value       string // Value used (if applicable)
	Description string // Additional info about the operation
}

// RingBuffer keeps the last 50 operations
type RingBuffer struct {
	entries []LogEntry
	index   int
	size    int
}

// NewRingBuffer initializes a ring buffer with a fixed size of 50
func NewRingBuffer(size int) *RingBuffer {
	return &RingBuffer{
		entries: make([]LogEntry, size),
		size:    size,
	}
}

// Add adds a new log entry to the ring buffer
func (rb *RingBuffer) Add(entry LogEntry) {
	rb.entries[rb.index] = entry
	rb.index = (rb.index + 1) % rb.size
}

// Dump prints the contents of the ring buffer for debugging
func (rb *RingBuffer) Dump() {
	fmt.Println("Operation log (last 50 operations):")
	for i := 0; i < rb.size; i++ {
		entry := rb.entries[(rb.index+i)%rb.size]
		if entry.Operation != "" {
			fmt.Printf("#%03d [%s] Key: %s, Value: %s, Description: %s\n",
				entry.OpNum, entry.Operation, trimTo40([]byte(entry.Key)), trimTo40([]byte(entry.Value)), entry.Description)
		}
	}
}

// Helper function to hex-encode a byte slice for clearer logging
func encodeHex(data []byte) string {
	return hex.EncodeToString(data)
}

func FuzzKeyValueOperations(t *testing.T, store KvLike, storeName string) {
	t.Run("FuzzTestRandomOperations", func(t *testing.T) {
		rand.Seed(time.Now().UnixNano())
		numOperations := rand.Intn(runs) + runs // Random number of operations

		keys := make(map[string][]byte)       // Track valid keys
		ringBuffer := NewRingBuffer(50)       // Initialize ring buffer for logging

		for i := 0; i < numOperations; i++ {
			operation := rand.Intn(3)      // 0 = Put, 1 = Get, 2 = Delete
			keySize := rand.Intn(1024) + 1 // Random key size
			valueSize := rand.Intn(4096) + 1 // Random value size

			key := randomBytes(keySize)
			value := randomBytes(valueSize)

			switch operation {
			case 0: // Put operation
				err := store.Put(key, value)
				ringBuffer.Add(LogEntry{i + 1, "Put", encodeHex(key), encodeHex(value), "Put operation"})

				if err != nil {
					ringBuffer.Dump()
					t.Fatalf("[%s] Failed to put key/value pair: %v", storeName, err)
				}
				keys[string(key)] = value

			case 1: // Get operation
				if len(keys) == 0 {
					continue // No keys to get
				}
				randomKey := getRandomKey(keys)
				expectedValue := keys[randomKey]

				retrievedValue, err := store.Get([]byte(randomKey))
				ringBuffer.Add(LogEntry{i + 1, "Get", encodeHex([]byte(randomKey)), encodeHex(expectedValue), "Get operation"})

				if err != nil {
					ringBuffer.Dump()
					store.DumpIndex()
					t.Fatalf("[%s] Failed to get key: %s, %v", storeName, encodeHex([]byte(randomKey)), err)
				}

				if !bytes.Equal(retrievedValue, expectedValue) {
					ringBuffer.Dump()
					t.Errorf("[%s] Expected value %s, got %s", storeName, encodeHex(expectedValue), encodeHex(retrievedValue))
				}

			case 2: // Delete operation
				if len(keys) == 0 {
					continue // No keys to delete
				}
				randomKey := getRandomKey(keys)

				err := store.Delete([]byte(randomKey))
				ringBuffer.Add(LogEntry{i + 1, "Delete", encodeHex([]byte(randomKey)), "", "Delete operation"})

				if err != nil {
					ringBuffer.Dump()
					t.Fatalf("[%s] Failed to delete key: %v", storeName, err)
				}

				delete(keys, randomKey)

				if store.Exists([]byte(randomKey)) {
					ringBuffer.Dump()
					t.Errorf("[%s] Expected key %s to not exist after deletion", storeName, encodeHex([]byte(randomKey)))
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
