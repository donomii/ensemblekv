package ensemblekv

import (
	"bytes"
	"fmt"
	"testing"
)

// ConcurrentMapFuncTests verifies that keys can be modified inside MapFunc
func ConcurrentMapFuncTests(t *testing.T, store KvLike, storeName string) {
	fmt.Println("Testing ConcurrentMapFuncTests")

	runFailfast(t, "ConcurrentMapFunc", func(t *testing.T) {
		// Setup initial data
		initialData := map[string][]byte{
			"cmap1": []byte("val1"),
			"cmap2": []byte("val2"),
			"cmap3": []byte("val3"),
		}

		for k, v := range initialData {
			if err := store.Put([]byte(k), v); err != nil {
				t.Fatalf("Setup Put failed: %v", err)
			}
		}

		// MapFunc with modification
		// For each key, we will add a suffix "_new" and delete the original
		processedCount := 0
		_, err := store.MapFunc(func(k, v []byte) error {
			processedCount++
			newKey := append(k, []byte("_new")...)
			newValue := append(v, []byte("_updated")...)

			// Put new
			if err := store.Put(newKey, newValue); err != nil {
				return fmt.Errorf("MapFunc Put failed: %v", err)
			}

			// Delete old
			if err := store.Delete(k); err != nil {
				return fmt.Errorf("MapFunc Delete failed: %v", err)
			}
			return nil
		})

		if err != nil {
			t.Fatalf("MapFunc failed: %v", err)
		}

		// Verify
		for k := range initialData {
			if store.Exists([]byte(k)) {
				t.Errorf("Key %s should have been deleted", k)
			}
			newKey := k + "_new"
			if !store.Exists([]byte(newKey)) {
				t.Errorf("New key %s should exist", newKey)
			}
		}
	})

	runFailfast(t, "ConcurrentMapPrefixFunc", func(t *testing.T) {
		// Setup initial data
		prefix := "cpref_"
		initialData := map[string][]byte{
			prefix + "1": []byte("val1"),
			prefix + "2": []byte("val2"),
			"other_1":    []byte("val3"),
		}

		for k, v := range initialData {
			if err := store.Put([]byte(k), v); err != nil {
				t.Fatalf("Setup Put failed: %v", err)
			}
		}

		// MapPrefixFunc with modification
		_, err := store.MapPrefixFunc([]byte(prefix), func(k, v []byte) error {
			// Update the value
			newValue := append(v, []byte("_updated")...)
			if err := store.Put(k, newValue); err != nil {
				return err
			}
			return nil
		})

		if err != nil {
			t.Fatalf("MapPrefixFunc failed: %v", err)
		}

		// Verify
		val1, _ := store.Get([]byte(prefix + "1"))
		if !bytes.Equal(val1, []byte("val1_updated")) {
			t.Errorf("Value for %s1 not updated, got %s", prefix, val1)
		}

		// Verify non-prefix item untouched
		val3, _ := store.Get([]byte("other_1"))
		if !bytes.Equal(val3, []byte("val3")) {
			t.Errorf("Value for other_1 changed, got %s", val3)
		}
	})
}
