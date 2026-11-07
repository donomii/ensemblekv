package ensemblekv

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

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

func TestManagerIterationAllowsWrites(t *testing.T) {
	creators := map[string]CreatorFunc{
		"line": func(directory string, blockSize, fileSize int64) (KvLike, error) {
			return NewLinelsm(directory, blockSize, 1000, fileSize, JsonKVCreator)
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
