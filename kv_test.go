package ensemblekv

import (
	"bytes"
	"fmt"
	"testing"
)

func TestExtentKeyValueStore(t *testing.T) {
	StartKVStoreOperations(t, ExtentCreator, "ExtentKeyValueStore")
}

func TestBarrelDbKeyValueStore(t *testing.T) {
	StartKVStoreOperations(t, BarrelDbCreator, "BarrelDbKeyValueStore")
}

func TestEnsembleExtentStore(t *testing.T) {
	StartKVStoreOperations(t, func(directory string, blockSize int) (KvLike, error) {
		return EnsembleCreator(directory, blockSize, ExtentCreator)
	}, "EnsembleExtentStore")
}

func TestLineLSMExtentStore(t *testing.T) {
	StartKVStoreOperations(t, func(directory string, blockSize int) (KvLike, error) {
		return EnsembleCreator(directory, blockSize, ExtentCreator)
	}, "EnsembleExtentStore")
}

func TestEnsembleBarrelDbStore(t *testing.T) {
	StartKVStoreOperations(t, func(directory string, blockSize int) (KvLike, error) {
		return EnsembleCreator(directory, blockSize, BarrelDbCreator)
	}, "EnsembleBarrelDbStore")
}

func TestEnsembleBoltDbStore(t *testing.T) {
	StartKVStoreOperations(t, func(directory string, blockSize int) (KvLike, error) {
		return EnsembleCreator(directory, blockSize, BoltDbCreator)
	}, "EnsembleBoltDbStore")
}

/*
func TestEnsembleLotusStore(t *testing.T) {
	StartKVStoreOperations(t, func(directory string, blockSize int) (KvLike, error) {
		return EnsembleCreator(directory, blockSize, LotusCreator)
	})
}
*/

/* Completely broken
func TestNuDbKeyValueStore(t *testing.T) {
	StartKVStoreOperations(t, NuDbCreator)
}
*/

/*
func TestLotusKeyValueStore(t *testing.T) {
	StartKVStoreOperations(t, LotusCreator)
}
*/

// Update StartKVStoreOperations to include fuzz testing
func StartKVStoreOperations(t *testing.T, creator func(directory string, blockSize int) (KvLike, error), storeName string) {
	t.Run("KVStore", func(t *testing.T) {
		// Temp dir for testing
		dir := t.TempDir()
		blockSize := 1024

		// Create a new store
		store, err := creator(dir, blockSize)
		if err != nil {
			t.Fatalf("Failed to create store: %v", err)
		}

		// Run basic operations
		KVStoreOperations(t, store)

		// Run large key/value tests
		LargeKeyValuePairs(t, store)

		// Run mixed size key/value tests
		MixedSizeKeyValuePairs(t, store)

		// Run fuzz tests
		FuzzKeyValueOperations(t, store, storeName)
	})
}



func KVStoreOperations(t *testing.T, store KvLike) {
	t.Run("Put and Get", func(t *testing.T) {
		key := []byte("key1")
		expectedValue := []byte("value1")
		err := store.Put(key, expectedValue)
		if err != nil {
			t.Fatalf("Failed to put value: %v", err)
		}

		actualValue, err := store.Get(key)
		if err != nil {
			t.Fatalf("Failed to get value: %v", err)
		}

		if !bytes.Equal(actualValue, expectedValue) {
			t.Errorf("Expected value %s, got %s", trimTo40(expectedValue), trimTo40(actualValue))
		}
	})

	t.Run("Exists", func(t *testing.T) {
		key := []byte("key2")
		value := []byte("value2")
		err := store.Put(key, value)
		if err != nil {
			t.Fatalf("Failed to put value: %v", err)
		}

		exists := store.Exists(key)
		if !exists {
			t.Errorf("Expected key %s to exist", trimTo40(key))
		}

		notExists := store.Exists([]byte("nonexistent"))
		if notExists {
			t.Errorf("Expected key 'nonexistent' to not exist")
		}
	})

	t.Run("Delete", func(t *testing.T) {
		key := []byte("key3")
		value := []byte("value3")
		err := store.Put(key, value)
		if err != nil {
			t.Fatalf("Failed to put value: %v", err)
		}

		err = store.Delete(key)
		if err != nil {
			t.Fatalf("Failed to delete key: %v", err)
		}

		exists := store.Exists(key)
		if exists {
			store.MapFunc(func(k, v []byte) error {
				//fmt.Printf("Key: %s\n", trimTo40(k))
				return nil
			})
			t.Errorf("Expected key %s to not exist after delete", trimTo40(key))
		}
	})
}

func LargeKeyValuePairs(t *testing.T, store KvLike) {
	t.Run("LargeKeyValue", func(t *testing.T) {
		largeKey := bytes.Repeat([]byte("k"), 1024*1)        // 1MB key
		largeValue := bytes.Repeat([]byte("v"), 1024*1024*5) // 50MB value
		err := store.Put(largeKey, largeValue)
		if err != nil {
			t.Fatalf("Failed to put large key/value pair: %v", err)
		}

		retrievedValue, err := store.Get(largeKey)
		if err != nil {
			t.Fatalf("Failed to get large value: %v", err)
		}

		if !bytes.Equal(retrievedValue, largeValue) {
			t.Error("Retrieved value does not match the original large value")
		}

		if !store.Exists(largeKey) {
			t.Error("Large key does not exist after being put")
		}

		err = store.Delete(largeKey)
		if err != nil {
			t.Fatalf("Failed to delete large key: %v", err)
		}

		if store.Exists(largeKey) {
			t.Error("Large key exists after being deleted")
		}

	})
}

func MixedSizeKeyValuePairs(t *testing.T, store KvLike) {
	keySizes := []int{32, 1024, 32 * 1024}                 // , 64 * 1024 . Various key sizes.  Bbolt can't handle more than 32k
	valueSizes := []int{128, 1024 * 1024, 4 * 1024 * 1024} // Various value sizes

	for _, ks := range keySizes {
		for _, vs := range valueSizes {
			t.Run(fmt.Sprintf("TestMixedSizes_%d_%d", ks, vs), func(t *testing.T) {
				key := bytes.Repeat([]byte("k"), ks)
				value := bytes.Repeat([]byte("v"), vs)
				err := store.Put(key, value)
				if err != nil {
					t.Fatalf("Failed to put key/value pair of size %d/%d: %v", ks, vs, err)
				}

				retrievedValue, err := store.Get(key)
				if err != nil {
					t.Fatalf("Failed to get value of size %d: %v", vs, err)
				}

				if !bytes.Equal(retrievedValue, value) {
					t.Errorf("Retrieved value does not match the original value for size %d/%d", ks, vs)
				}

				err = store.Delete(key)
				if err != nil {
					t.Fatalf("Failed to delete key of size %d: %v", ks, err)
				}

			})
		}
	}
}
