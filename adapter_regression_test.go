package ensemblekv

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	nudb "github.com/iand/gonudb"
	bolt "go.etcd.io/bbolt"
)

func TestBoltReturnsTransactionErrors(t *testing.T) {
	store, err := NewBoltDbShim(t.TempDir(), 4096, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(nil, []byte("value")); !errors.Is(err, bolt.ErrKeyRequired) {
		t.Fatalf("empty key: got %v, want %v", err, bolt.ErrKeyRequired)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("key"), []byte("value")); !errors.Is(err, bolt.ErrDatabaseNotOpen) {
		t.Fatalf("closed write: got %v, want %v", err, bolt.ErrDatabaseNotOpen)
	}
	if err := store.Delete([]byte("key")); !errors.Is(err, bolt.ErrDatabaseNotOpen) {
		t.Fatalf("closed delete: got %v, want %v", err, bolt.ErrDatabaseNotOpen)
	}
	if _, err := store.Get([]byte("key")); !errors.Is(err, bolt.ErrDatabaseNotOpen) {
		t.Fatalf("closed read: got %v, want %v", err, bolt.ErrDatabaseNotOpen)
	}
	if _, err := store.MapFunc(func(key, value []byte) error { return nil }); !errors.Is(err, bolt.ErrDatabaseNotOpen) {
		t.Fatalf("closed iteration: got %v, want %v", err, bolt.ErrDatabaseNotOpen)
	}
}

func TestBoltIterationValuesDoNotAliasStorage(t *testing.T) {
	store, err := NewBoltDbShim(t.TempDir(), 4096, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, prefix := range []bool{false, true} {
		if err := store.Put([]byte("key"), []byte("value")); err != nil {
			t.Fatal(err)
		}
		mutate := func(key, value []byte) error {
			value[0] = 'X'
			return nil
		}
		if prefix {
			_, err = store.MapPrefixFunc([]byte("k"), mutate)
		} else {
			_, err = store.MapFunc(mutate)
		}
		if err != nil {
			t.Fatal(err)
		}
		value, err := store.Get([]byte("key"))
		if err != nil || string(value) != "value" {
			t.Fatalf("prefix=%v callback changed database value: value=%q err=%v", prefix, value, err)
		}
	}
}

func TestSQLiteLiteralBinaryPrefixes(t *testing.T) {
	store, err := NewSQLiteKVStore(t.TempDir(), 4096, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	allKeys := []string{"Abc", "abc", "a_c", "axc", "a%c", "a\x00x", "a\xffx", "other"}
	for _, key := range allKeys {
		if err := store.Put([]byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	for _, prefix := range []string{"A", "a_", "a%", "a\x00", "a\xff", ""} {
		var got, want []string
		for _, key := range allKeys {
			if bytes.HasPrefix([]byte(key), []byte(prefix)) {
				want = append(want, key)
			}
		}
		visited, err := store.MapPrefixFunc([]byte(prefix), func(key, value []byte) error {
			got = append(got, string(key))
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(got)
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) || len(visited) != len(want) {
			t.Fatalf("prefix=%q got=%q visited=%v want=%q", prefix, got, visited, want)
		}
	}
}

func TestJsonBinaryKeysSurviveReopen(t *testing.T) {
	directory := t.TempDir()
	store, err := JsonKVCreator(directory, 4096, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	keys := [][]byte{{0xff}, {0xfe}, []byte("\ufffd"), []byte("normal"), {}, {0}}
	for _, key := range keys {
		if err := store.Put(key, append([]byte("value:"), key...)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = JsonKVCreator(directory, 4096, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, key := range keys {
		value, err := store.Get(key)
		want := append([]byte("value:"), key...)
		if err != nil || !bytes.Equal(value, want) {
			t.Fatalf("key=%x reopened value=%x err=%v want=%x", key, value, err, want)
		}
	}
}

func TestJsonLegacyStoreRemainsReadable(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "store.json"), []byte(`{"key":"dmFsdWU="}`), 0600); err != nil {
		t.Fatal(err)
	}
	store, err := JsonKVCreator(directory, 4096, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	value, err := store.Get([]byte("key"))
	if err != nil || string(value) != "value" {
		t.Fatalf("legacy value=%q err=%v", value, err)
	}
}

func TestNuDBRejectsUnsupportedOverwrite(t *testing.T) {
	store, err := NewNuDbShim(t.TempDir(), 4096, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Put([]byte("key"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("key"), []byte("new")); !errors.Is(err, nudb.ErrKeyExists) {
		t.Fatalf("overwrite returned %v, want %v", err, nudb.ErrKeyExists)
	}
}

func TestJsonSavePreservesPrivatePermissions(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "store.json")
	if err := os.WriteFile(path, []byte(`{"key":"dmFsdWU="}`), 0600); err != nil {
		t.Fatal(err)
	}
	store, err := JsonKVCreator(directory, 4096, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("key"), []byte("updated")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("saved store permissions=%o, want 600", info.Mode().Perm())
	}
}

func TestAdapterIterationAllowsDeletingUpcomingKey(t *testing.T) {
	for name, creator := range map[string]CreatorFunc{"bolt": BoltDbCreator, "sqlite": SQLiteCreator} {
		for _, prefix := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/prefix=%v", name, prefix), func(t *testing.T) {
				store, err := creator(t.TempDir(), 4096, 1024*1024)
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				for _, key := range []string{"a", "b", "c"} {
					if err := store.Put([]byte(key), []byte("value")); err != nil {
						t.Fatal(err)
					}
				}
				var seen []string
				callback := func(key, value []byte) error {
					seen = append(seen, string(key))
					if string(key) == "a" {
						return store.Delete([]byte("b"))
					}
					return nil
				}
				if prefix {
					_, err = store.MapPrefixFunc(nil, callback)
				} else {
					_, err = store.MapFunc(callback)
				}
				if err != nil || !reflect.DeepEqual(seen, []string{"a", "c"}) {
					t.Fatalf("seen=%q error=%v, want [a c] and no error", seen, err)
				}
			})
		}
	}
}
