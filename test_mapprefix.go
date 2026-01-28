package ensemblekv

import (
	"fmt"
	"os"
	"path/filepath"
)

func testMapPrefixFunc(storeName string, store KvLike) {
	fmt.Printf("\n=== Testing %s ===\n", storeName)

	// Add some test data
	testData := map[string]string{
		"user:1:name":     "Alice",
		"user:1:email":    "alice@example.com",
		"user:2:name":     "Bob",
		"user:2:email":    "bob@example.com",
		"product:1:name":  "Widget",
		"product:1:price": "10.99",
		"config:setting":  "value",
		"other:key":       "other",
	}

	// Put test data
	for key, value := range testData {
		err := store.Put([]byte(key), []byte(value))
		if err != nil {
			fmt.Printf("Error putting %s: %v\n", key, err)
			return
		}
	}

	// Test prefix searches
	testPrefixes := []string{"user:", "user:1:", "product:", "config:", "nonexistent:"}

	for _, prefix := range testPrefixes {
		fmt.Printf("\nSearching for prefix '%s':\n", prefix)

		keys, err := store.MapPrefixFunc([]byte(prefix), func(k, v []byte) error {
			fmt.Printf("  Found: %s = %s\n", string(k), string(v))
			return nil
		})

		if err != nil {
			fmt.Printf("  Error: %v\n", err)
		} else {
			fmt.Printf("  Total keys found: %d\n", len(keys))
		}
	}

	// Clean up
	store.Close()
}

func main() {
	tempDir := "/tmp/ensemblekv_test"
	os.RemoveAll(tempDir)

	// Test different stores
	testStores := []struct {
		name    string
		creator func() KvLike
	}{
		{
			"SingleFileLSM",
			func() KvLike {
				dir := filepath.Join(tempDir, "singlefile")
				os.MkdirAll(dir, 0755)
				store, _ := NewSingleFileLSM(filepath.Join(dir, "test.db"), 1024, 10*1024*1024)
				return store
			},
		},
		{
			"JsonKV",
			func() KvLike {
				dir := filepath.Join(tempDir, "json")
				store, _ := JsonKVCreator(dir, 1024, 10*1024*1024)
				return store
			},
		},
		{
			"SQLite",
			func() KvLike {
				dir := filepath.Join(tempDir, "sqlite")
				store, _ := NewSQLiteKVStore(dir, 1024, 10*1024*1024)
				return store
			},
		},
	}

	for _, test := range testStores {
		store := test.creator()
		if store != nil {
			testMapPrefixFunc(test.name, store)
		}
	}

	fmt.Println("\n=== All tests completed ===")
}
