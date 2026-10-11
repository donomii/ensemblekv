package ensemblekv

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func requireManagerContents(t *testing.T, store KvLike, expected map[string]string) {
	t.Helper()
	keys := make(map[string]bool)
	for _, key := range store.Keys() {
		if keys[string(key)] {
			t.Fatalf("Keys returned duplicate key %q", key)
		}
		keys[string(key)] = true
	}
	if len(keys) != len(expected) {
		t.Fatalf("Keys returned %d keys, expected %d: %v", len(keys), len(expected), keys)
	}
	for key, expectedValue := range expected {
		value, err := store.Get([]byte(key))
		if err != nil || string(value) != expectedValue || !store.Exists([]byte(key)) || !keys[key] {
			t.Fatalf("key %q: Get=%q, error=%v, Exists=%v, Keys contains key=%v; expected %q", key, value, err, store.Exists([]byte(key)), keys[key], expectedValue)
		}
	}
	values := make(map[string]string)
	_, err := store.MapFunc(func(key, value []byte) error {
		if _, exists := values[string(key)]; exists {
			t.Fatalf("MapFunc returned duplicate key %q", key)
		}
		values[string(key)] = string(value)
		return nil
	})
	if err != nil || !reflect.DeepEqual(values, expected) {
		t.Fatalf("MapFunc=%v, error=%v; expected %v", values, err, expected)
	}
	values = make(map[string]string)
	_, err = store.MapPrefixFunc([]byte("key-"), func(key, value []byte) error {
		if _, exists := values[string(key)]; exists {
			t.Fatalf("MapPrefixFunc returned duplicate key %q", key)
		}
		values[string(key)] = string(value)
		return nil
	})
	prefixExpected := make(map[string]string)
	for key, value := range expected {
		if strings.HasPrefix(key, "key-") {
			prefixExpected[key] = value
		}
	}
	if err != nil || !reflect.DeepEqual(values, prefixExpected) {
		t.Fatalf("MapPrefixFunc=%v, error=%v; expected %v", values, err, prefixExpected)
	}
}

func TestLinelsmPersistenceAndTombstones(t *testing.T) {
	for name, creator := range map[string]CreatorFunc{"json": JsonKVCreator, "bolt": BoltDbCreator, "singlefile": SingleFileKVCreator} {
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			store, err := NewLinelsm(directory, 4096, 1000, 1<<20, creator)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { store.Close() })
			for key, value := range map[string]string{"key-live": "old", "key-deleted": "old", "d:literal": "literal", "t:literal": "literal"} {
				if err := store.Put([]byte(key), []byte(value)); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Flush(); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"key-live", "key-deleted"} {
				if err := store.Delete([]byte(key)); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := store.Put([]byte("key-live"), []byte("new")); err != nil {
				t.Fatal(err)
			}
			expected := map[string]string{"key-live": "new", "d:literal": "literal", "t:literal": "literal"}
			requireManagerContents(t, store, expected)
			if err := store.Flush(); err != nil {
				t.Fatal(err)
			}
			requireManagerContents(t, store, expected)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = NewLinelsm(directory, 4096, 1000, 1<<20, creator)
			if err != nil {
				t.Fatal(err)
			}
			requireManagerContents(t, store, expected)
			if _, err := store.Get([]byte("key-deleted")); err == nil || store.Exists([]byte("key-deleted")) {
				t.Fatal("deleted key reappeared after reopen")
			}
			if err := store.Put([]byte("key-after-reopen"), []byte("saved")); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = NewLinelsm(directory, 4096, 1000, 1<<20, creator)
			if err != nil {
				t.Fatal(err)
			}
			expected["key-after-reopen"] = "saved"
			requireManagerContents(t, store, expected)
		})
	}
}

func TestLinelsmEmptyValuesAndCallbackErrors(t *testing.T) {
	store, err := NewLinelsm(t.TempDir(), 4096, 1000, 1<<20, JsonKVCreator)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Put([]byte("key-empty"), nil); err != nil {
		t.Fatal(err)
	}
	requireManagerContents(t, store, map[string]string{"key-empty": ""})
	expectedError := errors.New("callback failure")
	for _, prefix := range []bool{false, true} {
		callback := func(key, value []byte) error { return expectedError }
		var callbackError error
		if prefix {
			_, callbackError = store.MapPrefixFunc([]byte("key-"), callback)
		} else {
			_, callbackError = store.MapFunc(callback)
		}
		if !errors.Is(callbackError, expectedError) {
			t.Fatalf("prefix=%v: callback error=%v; expected %v", prefix, callbackError, expectedError)
		}
	}
}

func TestLinelsmRejectsMissingTierStore(t *testing.T) {
	directory := t.TempDir()
	metadata := StoreMetadata{Tiers: []TierMetadata{{Level: 0, StoreIds: []string{"tier-0-store-missing"}}}}
	data, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, metadataFile), data, 0644); err != nil {
		t.Fatal(err)
	}
	store, err := NewLinelsm(directory, 4096, 1000, 1<<20, JsonKVCreator)
	if err == nil {
		store.Close()
		t.Fatal("opening a missing tier silently created an empty store")
	}
}

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

func TestManagerIterationAllowsWrites(t *testing.T) {
	creators := map[string]CreatorFunc{
		"line": func(directory string, blockSize, fileSize int64) (KvLike, error) {
			return NewLinelsm(directory, blockSize, 1000, fileSize, JsonKVCreator)
		},
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

func TestLinelsmRejectsUnreferencedStores(t *testing.T) {
	for _, metadataPresent := range []bool{false, true} {
		t.Run(fmt.Sprintf("metadata=%v", metadataPresent), func(t *testing.T) {
			directory := t.TempDir()
			if err := os.Mkdir(filepath.Join(directory, "tier-3-store-old"), 0755); err != nil {
				t.Fatal(err)
			}
			if metadataPresent {
				if err := os.WriteFile(filepath.Join(directory, metadataFile), []byte(`{"tiers":null}`), 0644); err != nil {
					t.Fatal(err)
				}
			}
			store, err := NewLinelsm(directory, 4096, 1000, 1<<20, JsonKVCreator)
			if err == nil {
				store.Close()
				t.Fatal("opening tier directories without references silently ignored existing data")
			}
		})
	}
}

func TestLinelsmMetadataValidationClosesOpenedStores(t *testing.T) {
	for _, duplicateStore := range []bool{false, true} {
		t.Run(fmt.Sprintf("duplicateStore=%v", duplicateStore), func(t *testing.T) {
			directory := t.TempDir()
			storeID := "tier-0-store-valid"
			backingStore, err := BoltDbCreator(filepath.Join(directory, storeID), 4096, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			if err := backingStore.Close(); err != nil {
				t.Fatal(err)
			}
			metadata := StoreMetadata{Tiers: []TierMetadata{{Level: 0, StoreIds: []string{storeID}}}}
			if duplicateStore {
				metadata.Tiers[0].StoreIds = append(metadata.Tiers[0].StoreIds, storeID)
			} else {
				metadata.Tiers = append(metadata.Tiers, TierMetadata{Level: 0})
			}
			data, err := json.Marshal(metadata)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(directory, metadataFile), data, 0644); err != nil {
				t.Fatal(err)
			}
			store, err := NewLinelsm(directory, 4096, 1000, 1<<20, BoltDbCreator)
			if err == nil {
				store.Close()
				t.Fatal("accepted duplicate tier metadata")
			}
			backingStore, err = BoltDbCreator(filepath.Join(directory, storeID), 4096, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			if err := backingStore.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
