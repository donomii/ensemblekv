package ensemblekv

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

// Test timeouts and limits
const (
	testTimeout = 300 * time.Second // Reduced from 10m
	maxTestSize = 1024 * 1024       // 10MB max for test data
)

// StartKVStoreOperations runs the test suite with proper timeout and directory setup
func StartKVStoreOperations(t *testing.T, creator CreatorFunc, storeName string) {
	t.Helper()

	// Set test timeout
	timer := time.NewTimer(testTimeout)
	done := make(chan bool)

	go func() {
		t.Run("KVStore", func(t *testing.T) {
			// Create temporary directory
			storePath := t.TempDir()

			blockSize := int64(1024 * 4) // 4KB blocks

			// Create store
			store, err := creator(storePath, blockSize, testFileCapacity)
			if err != nil {
				fatalf(t, "store=%s action=CreateError path=%s blockSize=%d fileSize=%d err=%v", storeName, storePath, blockSize, testFileCapacity, err)
			}
			defer store.Close()

			// Run tests with store-specific limits
			KVStoreOperations(t, store, storeName)
			MixedSizeKeyValuePairs(t, store, storeName)
			FuzzKeyValueOperations(t, store, storeName)

			// Run persistence tests
			runFailfast(t, "Persistence", func(t *testing.T) {
				PersistenceTests(t, store, creator, storePath, blockSize, int64(maxTestSize))
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
		fatalf(t, "action=Timeout duration=%s", testTimeout)
	case <-done:
		timer.Stop()
	}
}

// PersistenceTests verifies that all operations survive store reopening and cache invalidation
func PersistenceTests(t *testing.T, store KvLike, creator CreatorFunc, storePath string, blockSize, fileSize int64) {
	t.Helper()

	// Test data with a mix of sizes
	testData := map[string][]byte{
		"key1":        []byte("value1"),
		"key2":        []byte("value2"),
		"key3":        randomBytes(1024, 1024), // Large value
		"key4":        randomBytes(4096, 4096), // Value larger than block size
		"keyToDelete": []byte("will be deleted"),
		"keyToUpdate": []byte("original value"),
	}

	// Phase 1: Initial data insertion
	runFailfast(t, "Phase1_InitialInsert", func(t *testing.T) {
		for key, value := range testData {
			err := store.Put([]byte(key), value)
			if err != nil {
				fatalf(t, "action=PutError key=%s value=%s err=%v", key, trimTo40(value), err)
			}
		}

		// Close and reopen
		err := store.Close()
		if err != nil {
			fatalf(t, "action=CloseError path=%s err=%v", storePath, err)
		}

		store, err = creator(storePath, blockSize, fileSize)
		if err != nil {
			fatalf(t, "action=ReopenError path=%s blockSize=%d fileSize=%d err=%v", storePath, blockSize, fileSize, err)
		}

		// Verify all initial data
		for key, expectedValue := range testData {
			exists := store.Exists([]byte(key))
			if !exists {
				fatalf(t, "action=ExistsMismatch key=%s expected=true got=false", key)
				continue
			}

			value, err := store.Get([]byte(key))
			if err != nil {
				fatalf(t, "action=GetError key=%s err=%v", key, err)
				continue
			}

			if !bytes.Equal(value, expectedValue) {
				fatalf(t, "action=GetMismatch key=%s expected=%s got=%s expectedLen=%d gotLen=%d", key, trimTo40(expectedValue), trimTo40(value), len(expectedValue), len(value))
			}
		}
	})

	// Phase 2: Test Delete persistence
	runFailfast(t, "Phase2_DeletePersistence", func(t *testing.T) {
		// Delete a key
		err := store.Delete([]byte("keyToDelete"))
		if err != nil {
			fatalf(t, "action=DeleteError key=%s err=%v", "keyToDelete", err)
		}

		// Close and reopen
		err = store.Close()
		if err != nil {
			fatalf(t, "action=CloseError path=%s err=%v", storePath, err)
		}

		store, err = creator(storePath, blockSize, fileSize)
		if err != nil {
			fatalf(t, "action=ReopenError path=%s blockSize=%d fileSize=%d err=%v", storePath, blockSize, fileSize, err)
		}

		// Verify deletion persisted
		exists := store.Exists([]byte("keyToDelete"))
		if exists {
			fatalf(t, "action=ExistsMismatch key=%s expected=false got=true", "keyToDelete")
		}

		_, err = store.Get([]byte("keyToDelete"))
		if err == nil {
			fatalf(t, "action=GetUnexpectedSuccess key=%s expectedErr=true gotErr=false", "keyToDelete")
		}
	})

	// Phase 3: Test Update persistence
	runFailfast(t, "Phase3_UpdatePersistence", func(t *testing.T) {
		newValue := []byte("updated value")
		err := store.Put([]byte("keyToUpdate"), newValue)
		if err != nil {
			fatalf(t, "action=PutError key=%s value=%s err=%v", "keyToUpdate", trimTo40(newValue), err)
		}

		fmt.Println("Closing store after update")
		// Close and reopen
		err = store.Close()
		if err != nil {
			fatalf(t, "action=CloseError path=%s err=%v", storePath, err)
		}

		fmt.Println("Reopening store" + storePath + " after update")
		store, err = creator(storePath, blockSize, fileSize)
		if err != nil {
			fatalf(t, "action=ReopenError path=%s blockSize=%d fileSize=%d err=%v", storePath, blockSize, fileSize, err)
		}

		// Verify update persisted
		value, err := store.Get([]byte("keyToUpdate"))
		if err != nil {
			fatalf(t, "action=GetError key=%s err=%v", "keyToUpdate", err)
		}

		if !bytes.Equal(value, newValue) {
			fatalf(t, "action=GetMismatch key=%s expected=%s got=%s", "keyToUpdate", trimTo40(newValue), trimTo40(value))
		}
	})

	// Phase 4: Test mixed operations persistence
	runFailfast(t, "Phase4_MixedOperations", func(t *testing.T) {
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
					fatalf(t, "action=PutError key=%s value=%s err=%v", op.key, trimTo40(op.value), err)
				}
			case "delete":
				err := store.Delete([]byte(op.key))
				if err != nil {
					fatalf(t, "action=DeleteError key=%s err=%v", op.key, err)
				}
			}
		}

		// Close and reopen
		err := store.Close()
		if err != nil {
			fatalf(t, "action=CloseError path=%s err=%v", storePath, err)
		}

		store, err = creator(storePath, blockSize, fileSize)
		if err != nil {
			fatalf(t, "action=ReopenError path=%s blockSize=%d fileSize=%d err=%v", storePath, blockSize, fileSize, err)
		}

		// Verify all operations persisted
		for _, op := range operations {
			exists := store.Exists([]byte(op.key))

			switch op.op {
			case "put":
				if !exists {
					fatalf(t, "action=ExistsMismatch key=%s expected=true got=false op=%s", op.key, op.op)
					continue
				}

				value, err := store.Get([]byte(op.key))
				if err != nil {
					fatalf(t, "action=GetError key=%s err=%v", op.key, err)
					continue
				}

				if !bytes.Equal(value, op.value) {
					fatalf(t, "action=GetMismatch key=%s expected=%s got=%s", op.key, trimTo40(op.value), trimTo40(value))
				}

			case "delete":
				if exists {
					fatalf(t, "action=ExistsMismatch key=%s expected=false got=true op=%s", op.key, op.op)
				}
			}
		}
	})

	// Clean up
	err := store.Close()
	if err != nil {
		fatalf(t, "action=CloseError path=%s err=%v", storePath, err)
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
		{32, 128},         // Small pairs
		{64, 1024},        // Medium pairs
		{128, 64 * 1024},  // Larger pairs
		{256, 256 * 1024}, // Even larger pairs
	}

	for _, size := range sizes {
		if size.keySize > limits.maxKeySize || int64(size.valueSize) > limits.maxValueSize {
			t.Logf("Skipping test with key size %d and value size %d (exceeds limits)",
				size.keySize, size.valueSize)
			continue
		}

		runFailfast(t, fmt.Sprintf("Size_%dx%d", size.keySize, size.valueSize), func(t *testing.T) {
			key := randomBytes(size.keySize, size.keySize)
			value := randomBytes(size.valueSize, size.valueSize)

			// Test Put
			//fmt.Println("Putting key: ", trimTo40(key))
			err := store.Put(key, value)
			if err != nil {
				fatalf(t, "action=PutError keySize=%d valueSize=%d key=%s err=%v", size.keySize, size.valueSize, trimTo40(key), err)
			}

			// Test Get
			//fmt.Printf("Getting key: %v\n", trimTo40(key))
			retrieved, err := store.Get(key)
			if err != nil {
				fatalf(t, "action=GetError keySize=%d valueSize=%d key=%s err=%v", size.keySize, size.valueSize, trimTo40(key), err)
			}

			if !bytes.Equal(retrieved, value) {
				fatalf(t, "action=GetMismatch keySize=%d valueSize=%d key=%s expected=%s got=%s", size.keySize, size.valueSize, trimTo40(key), trimTo40(value), trimTo40(retrieved))
			}

			// Test Delete
			//fmt.Printf("Deleting key: %v\n", trimTo40(key))
			err = store.Delete(key)
			if err != nil {
				fatalf(t, "action=DeleteError keySize=%d valueSize=%d key=%s err=%v", size.keySize, size.valueSize, trimTo40(key), err)
			}

			// Verify deletion
			if store.Exists(key) {
				store.DumpIndex()
				fmt.Printf("Key: %v still exists after deletion\n", trimTo40(key))
				fatalf(t, "action=DeleteStillExists key=%s expected=false got=true", trimTo40(key))
			}
		})
	}
}

// Base store types
var baseStores = []struct {
	name    string
	creator CreatorFunc
}{
	{"Bolt", BoltDbCreator},
	{"Extent", ExtentCreator},
	{"ExtentMmap", ExtentMmapCreator},
	// {"Pudge", PudgeCreator},
	{"JsonKV", JsonKVCreator},
	{"SingleFileLSM", SingleFileKVCreator},
	{"SQLite", SQLiteCreator},
	{"MmapSingle", MmapSingleCreator},
}

// Store wrapper types
var wrapperTypes = []struct {
	name          string
	createWrapper func(baseCreator CreatorFunc) CreatorFunc
}{

	{
		"Ensemble",
		func(baseCreator CreatorFunc) CreatorFunc {
			return func(directory string, blockSize, filesize int64) (KvLike, error) {
				return EnsembleCreator(directory, blockSize, testFileCapacity, baseCreator)
			}
		},
	},
	{
		"Tree",
		func(baseCreator CreatorFunc) CreatorFunc {
			return func(directory string, blockSize, filesize int64) (KvLike, error) {
				return NewTreeLSM(directory, blockSize, testFileCapacity, 0, baseCreator)
			}
		},
	},
	{
		"Star",
		func(baseCreator CreatorFunc) CreatorFunc {
			return func(directory string, blockSize, filesize int64) (KvLike, error) {
				return NewStarLSM(directory, blockSize, testFileCapacity, baseCreator)
			}
		},
	},
}

func TestAllStores(t *testing.T) {
	// Test base stores
	for _, base := range baseStores {
		runFailfast(t, base.name, func(t *testing.T) {
			StartKVStoreOperations(t, base.creator, base.name)
		})
	}

	// Test wrapped stores
	for _, wrapper := range wrapperTypes {
		for _, base := range baseStores {
			name := fmt.Sprintf("%sLSM%s", wrapper.name, base.name)
			runFailfast(t, name, func(t *testing.T) {
				creator := wrapper.createWrapper(base.creator)
				StartKVStoreOperations(t, creator, name)
			})
		}
	}
}
