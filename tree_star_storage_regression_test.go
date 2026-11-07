package ensemblekv

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestTreeLSMSplitReopenAndDeletion(t *testing.T) {
	directory := t.TempDir()
	store, err := NewTreeLSM(directory, 4096, 100, 0, JsonKVCreator)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	expected := make(map[string]string)
	var matchingKeys []string
	for candidate := 0; len(matchingKeys) < 90 && candidate < 100000; candidate++ {
		key := fmt.Sprintf("key-%d", candidate)
		if strings.HasPrefix(store.hashKey([]byte(key)), "3b") {
			matchingKeys = append(matchingKeys, key)
			if err := store.Put([]byte(key), []byte("x")); err != nil {
				t.Fatal(err)
			}
			expected[key] = "x"
		}
	}
	if len(matchingKeys) != 90 {
		t.Fatalf("found %d keys with hash prefix 3b; expected 90", len(matchingKeys))
	}
	requireManagerContents(t, store, expected)
	updatedKey, deletedKey := matchingKeys[0], matchingKeys[1]
	if err := store.Put([]byte(updatedKey), []byte("new")); err != nil {
		t.Fatal(err)
	}
	expected[updatedKey] = "new"
	if !store.isReadOnly || !store.subStores["3"].isReadOnly || !store.subStores["3"].subStores["3b"].isReadOnly {
		t.Fatal("expected three split levels for keys sharing two hash characters")
	}
	if err := store.Delete([]byte(deletedKey)); err != nil {
		t.Fatal(err)
	}
	delete(expected, deletedKey)
	for pass := 0; pass < 2; pass++ {
		requireManagerContents(t, store, expected)
		if _, err := store.Get([]byte(deletedKey)); err == nil || store.Exists([]byte(deletedKey)) {
			t.Fatalf("pass %d: deleted key %q reappeared from a parent", pass, deletedKey)
		}
		if _, err := store.Get([]byte("missing")); err == nil || store.Exists([]byte("missing")) {
			t.Fatalf("pass %d: missing key exists", pass)
		}
		if pass == 0 {
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = NewTreeLSM(directory, 4096, 100, 0, JsonKVCreator)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := store.Put([]byte(deletedKey), []byte("restored")); err != nil {
		t.Fatal(err)
	}
	expected[deletedKey] = "restored"
	requireManagerContents(t, store, expected)
}

func TestStarLSMSplitReopenAndDeletion(t *testing.T) {
	directory := t.TempDir()
	store, err := NewStarLSM(directory, 4096, 1<<20, JsonKVCreator)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	expected := map[string]string{"key-live": "old", "key-deleted": "old", "other": "value"}
	for key, value := range expected {
		if err := store.Put([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	requireManagerContents(t, store, expected)
	hash := store.hashKey([]byte("key-live"))
	if err := store.split(hash); err != nil {
		t.Fatal(err)
	}
	if err := store.subStores[hash[:1]].split(hash); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("key-live"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	expected["key-live"] = "new"
	if err := store.Delete([]byte("key-deleted")); err != nil {
		t.Fatal(err)
	}
	delete(expected, "key-deleted")
	for pass := 0; pass < 2; pass++ {
		requireManagerContents(t, store, expected)
		for _, key := range []string{"key-deleted", "missing"} {
			if _, err := store.Get([]byte(key)); err == nil || store.Exists([]byte(key)) {
				t.Fatalf("pass %d: absent key %q exists", pass, key)
			}
		}
		if pass == 0 {
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = NewStarLSM(directory, 4096, 1<<20, JsonKVCreator)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if !store.isReadOnly || !store.subStores[hash[:1]].isReadOnly {
		t.Fatal("reopened parent stores must route writes into their children")
	}
	if err := store.Put([]byte("key-deleted"), []byte("restored")); err != nil {
		t.Fatal(err)
	}
	expected["key-deleted"] = "restored"
	requireManagerContents(t, store, expected)
}

func TestStarLSMPutAfterSizeSplit(t *testing.T) {
	store, err := NewStarLSM(t.TempDir(), 4096, 128<<20, SingleFileKVCreator)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	largeKey := []byte("large")
	largeValue := bytes.Repeat([]byte("x"), starMaxStoreSize)
	if err := store.Put(largeKey, largeValue); err != nil {
		t.Fatal(err)
	}
	if store.currentStore.Size() <= starMaxStoreSize {
		t.Fatalf("backing store size %d did not exceed split threshold %d", store.currentStore.Size(), starMaxStoreSize)
	}
	var smallKey []byte
	for candidate := 0; ; candidate++ {
		smallKey = []byte(fmt.Sprintf("key-%d", candidate))
		if store.hashKey(smallKey)[:1] != store.hashKey(largeKey)[:1] {
			break
		}
	}
	if err := store.Put(smallKey, []byte("small")); err != nil {
		t.Fatal(err)
	}
	if !store.isReadOnly {
		t.Fatal("Put did not split the full store")
	}
	if value, err := store.Get(smallKey); err != nil || string(value) != "small" {
		t.Fatalf("new key after split=%q, error=%v; expected small", value, err)
	}
	if value, err := store.Get(largeKey); err != nil || !bytes.Equal(value, largeValue) {
		t.Fatalf("large value after split has %d bytes, error=%v; expected unchanged %d bytes", len(value), err, len(largeValue))
	}
}

func TestTreeAndStarIterationAllowsWrites(t *testing.T) {
	creators := map[string]CreatorFunc{
		"tree": func(directory string, blockSize, fileSize int64) (KvLike, error) {
			return NewTreeLSM(directory, blockSize, fileSize, 0, JsonKVCreator)
		},
		"star": func(directory string, blockSize, fileSize int64) (KvLike, error) {
			return NewStarLSM(directory, blockSize, fileSize, JsonKVCreator)
		},
	}
	for name, creator := range creators {
		for _, prefix := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/prefix=%v", name, prefix), func(t *testing.T) {
				store, err := creator(t.TempDir(), 4096, 1<<20)
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				if err := store.Put([]byte("key-original"), []byte("old")); err != nil {
					t.Fatal(err)
				}
				calls := 0
				callback := func(key, value []byte) error {
					calls++
					if err := store.Delete(key); err != nil {
						return err
					}
					return store.Put([]byte("key-new"), []byte("new"))
				}
				if prefix {
					_, err = store.MapPrefixFunc([]byte("key-"), callback)
				} else {
					_, err = store.MapFunc(callback)
				}
				if err != nil || calls != 1 {
					t.Fatalf("iteration error=%v, calls=%d; expected one successful callback", err, calls)
				}
				requireManagerContents(t, store, map[string]string{"key-new": "new"})
			})
		}
	}
}
