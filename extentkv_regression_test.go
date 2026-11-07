package ensemblekv

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
)

func openExtentRegressionStore(t *testing.T, directory string) *ExtentKeyValStore {
	t.Helper()
	store, err := NewExtentKeyValueStore(directory, 4096, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func putExtentRegressionValue(t *testing.T, store *ExtentKeyValStore, key, value string) {
	t.Helper()
	if err := store.Put([]byte(key), []byte(value)); err != nil {
		t.Fatalf("Put(%q, %q): %v", key, value, err)
	}
}

func requireExtentRegressionValue(t *testing.T, store *ExtentKeyValStore, key, expected string) {
	t.Helper()
	value, err := store.Get([]byte(key))
	if err != nil || string(value) != expected {
		t.Fatalf("Get(%q) = %q, %v; want %q, nil", key, value, err, expected)
	}
}

func TestExtentLatestValueBeforeTombstone(t *testing.T) {
	directory := t.TempDir()
	store := openExtentRegressionStore(t, directory)
	putExtentRegressionValue(t, store, "seed", "s")
	putExtentRegressionValue(t, store, "other", "other-value")
	putExtentRegressionValue(t, store, "key", "old")
	putExtentRegressionValue(t, store, "key", "new")
	if err := store.Delete([]byte("other")); err != nil {
		t.Fatal(err)
	}
	requireExtentRegressionValue(t, store, "key", "new")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openExtentRegressionStore(t, directory)
	defer store.Close()
	requireExtentRegressionValue(t, store, "key", "new")
}

func TestExtentEmptyValuesSurviveReopen(t *testing.T) {
	directory := t.TempDir()
	store := openExtentRegressionStore(t, directory)
	putExtentRegressionValue(t, store, "first", "")
	putExtentRegressionValue(t, store, "second", "")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openExtentRegressionStore(t, directory)
	defer store.Close()
	requireExtentRegressionValue(t, store, "first", "")
	requireExtentRegressionValue(t, store, "second", "")
	putExtentRegressionValue(t, store, "third", "value")
	store.ClearCache()
	requireExtentRegressionValue(t, store, "third", "value")
}

func TestExtentRejectsEmptyKeys(t *testing.T) {
	directory := t.TempDir()
	store := openExtentRegressionStore(t, directory)
	if err := store.Put(nil, nil); err == nil {
		t.Fatal("Put accepted an empty key that cannot be deleted")
	}
	if err := store.Delete(nil); err == nil {
		t.Fatal("Delete accepted an empty key")
	}
	if err := store.Delete([]byte("missing")); err != nil {
		t.Fatal(err)
	}
	if store.Exists([]byte("missing")) {
		t.Fatal("deleting a missing key inserted it")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openExtentRegressionStore(t, directory)
	defer store.Close()
	if keys := store.Keys(); len(keys) != 0 {
		t.Fatalf("empty store has keys after reopen: %q", keys)
	}
}

func TestExtentDisabledCaching(t *testing.T) {
	previous := EnableIndexCaching
	EnableIndexCaching = false
	defer func() { EnableIndexCaching = previous }()
	directory := t.TempDir()
	store := openExtentRegressionStore(t, directory)
	putExtentRegressionValue(t, store, "key", "old")
	putExtentRegressionValue(t, store, "key", "new")
	requireExtentRegressionValue(t, store, "key", "new")
	if !store.Exists([]byte("key")) {
		t.Fatal("inserted key does not exist")
	}
	if err := store.Delete([]byte("key")); err != nil {
		t.Fatal(err)
	}
	if store.Exists([]byte("key")) {
		t.Fatal("deleted key exists")
	}
	putExtentRegressionValue(t, store, "remaining", "value")
	if keys := store.Keys(); len(keys) != 1 || string(keys[0]) != "remaining" {
		t.Fatalf("Keys() = %q; want [remaining]", keys)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openExtentRegressionStore(t, directory)
	defer store.Close()
	requireExtentRegressionValue(t, store, "remaining", "value")
	keys, err := store.List()
	if err != nil || !reflect.DeepEqual(keys, []string{"remaining"}) {
		t.Fatalf("List() = %q, %v; want [remaining], nil", keys, err)
	}
}

func TestExtentListsExcludeDeletedAndMissingKeys(t *testing.T) {
	store := openExtentRegressionStore(t, t.TempDir())
	defer store.Close()
	putExtentRegressionValue(t, store, "keep", "value")
	putExtentRegressionValue(t, store, "deleted", "value")
	if err := store.Delete([]byte("deleted")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get([]byte("missing")); err == nil {
		t.Fatal("missing key unexpectedly returned a value")
	}
	keys, err := store.List()
	sort.Strings(keys)
	if err != nil || !reflect.DeepEqual(keys, []string{"keep"}) {
		t.Fatalf("List() = %q, %v; want [keep], nil", keys, err)
	}
	if keys := store.Keys(); len(keys) != 1 || string(keys[0]) != "keep" {
		t.Fatalf("Keys() = %q; want [keep]", keys)
	}
}

func appendExtentRegressionFile(t *testing.T, directory, name string, data []byte) {
	t.Helper()
	file, err := os.OpenFile(filepath.Join(directory, name), os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.Write(data); err != nil {
		t.Fatal(err)
	}
}

func TestExtentInterruptedPutRecovery(t *testing.T) {
	for _, phase := range []string{"value-data", "partial-value-index", "value-index", "key-data", "partial-key-index"} {
		t.Run(phase, func(t *testing.T) {
			directory := t.TempDir()
			store := openExtentRegressionStore(t, directory)
			putExtentRegressionValue(t, store, "a", "A")
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			appendExtentRegressionFile(t, directory, "values.dat", []byte("B"))
			boundary := binary.BigEndian.AppendUint64(nil, 2)
			switch phase {
			case "partial-value-index":
				appendExtentRegressionFile(t, directory, "values.index", boundary[:4])
			case "value-index", "key-data", "partial-key-index":
				appendExtentRegressionFile(t, directory, "values.index", boundary)
			}
			if phase == "key-data" || phase == "partial-key-index" {
				appendExtentRegressionFile(t, directory, "keys.dat", []byte("b"))
			}
			if phase == "partial-key-index" {
				appendExtentRegressionFile(t, directory, "keys.index", boundary[:4])
			}
			store = openExtentRegressionStore(t, directory)
			defer store.Close()
			requireExtentRegressionValue(t, store, "a", "A")
			if store.Exists([]byte("b")) {
				t.Fatal("incomplete key b became visible")
			}
			putExtentRegressionValue(t, store, "c", "C")
			store.ClearCache()
			requireExtentRegressionValue(t, store, "c", "C")
		})
	}
}

func TestExtentRecoveryDropsIncompleteRecordsWithoutGrowingData(t *testing.T) {
	for _, name := range []string{"keys.dat", "values.dat"} {
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			store := openExtentRegressionStore(t, directory)
			putExtentRegressionValue(t, store, "a", "A")
			putExtentRegressionValue(t, store, "broken", "BROKEN")
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.Truncate(filepath.Join(directory, name), 6); err != nil {
				t.Fatal(err)
			}
			store = openExtentRegressionStore(t, directory)
			defer store.Close()
			requireExtentRegressionValue(t, store, "a", "A")
			if store.Exists([]byte("broken")) {
				t.Fatal("incomplete key broken became visible")
			}
			for _, dataName := range []string{"keys.dat", "values.dat"} {
				stat, err := os.Stat(filepath.Join(directory, dataName))
				if err != nil || stat.Size() != 1 {
					t.Fatalf("%s after recovery: stat=%v error=%v; want one complete byte", dataName, stat, err)
				}
			}
			putExtentRegressionValue(t, store, "c", "C")
			store.ClearCache()
			requireExtentRegressionValue(t, store, "c", "C")
		})
	}
}

func TestExtentRejectsCorruptInitialIndexesWithoutTruncatingData(t *testing.T) {
	for _, name := range []string{"keys.index", "values.index"} {
		for _, size := range []int64{0, 4} {
			directory := t.TempDir()
			store := openExtentRegressionStore(t, directory)
			putExtentRegressionValue(t, store, "key", "value")
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.Truncate(filepath.Join(directory, name), size); err != nil {
				t.Fatal(err)
			}
			if store, err := NewExtentKeyValueStore(directory, 4096, 1024*1024); err == nil {
				store.Close()
				t.Fatalf("accepted %s truncated to %d bytes", name, size)
			}
			for dataName, expected := range map[string]string{"keys.dat": "key", "values.dat": "value"} {
				data, err := os.ReadFile(filepath.Join(directory, dataName))
				if err != nil || string(data) != expected {
					t.Fatalf("%s = %q, %v; want %q after failed recovery", dataName, data, err, expected)
				}
			}
		}
	}
}

func TestExtentInterruptedDeletionRestoresEOF(t *testing.T) {
	for _, partialKeyIndex := range []bool{false, true} {
		directory := t.TempDir()
		store := openExtentRegressionStore(t, directory)
		putExtentRegressionValue(t, store, "a", "A")
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		appendExtentRegressionFile(t, directory, "values.dat", []byte("DELETED"))
		file, err := os.OpenFile(filepath.Join(directory, "values.index"), os.O_WRONLY, 0644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.WriteAt(binary.BigEndian.AppendUint64(nil, ^uint64(0)), 8); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		appendExtentRegressionFile(t, directory, "values.index", binary.BigEndian.AppendUint64(nil, 8))
		if partialKeyIndex {
			appendExtentRegressionFile(t, directory, "keys.dat", []byte("a"))
			file, err := os.OpenFile(filepath.Join(directory, "keys.index"), os.O_WRONLY, 0644)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.WriteAt(binary.BigEndian.AppendUint64(nil, ^uint64(0)), 8); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
		}
		store = openExtentRegressionStore(t, directory)
		requireExtentRegressionValue(t, store, "a", "A")
		putExtentRegressionValue(t, store, "c", "C")
		store.ClearCache()
		requireExtentRegressionValue(t, store, "c", "C")
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestExtentMapPropagatesCallbackError(t *testing.T) {
	store := openExtentRegressionStore(t, t.TempDir())
	defer store.Close()
	putExtentRegressionValue(t, store, "prefix:a", "A")
	putExtentRegressionValue(t, store, "prefix:b", "B")
	callbackError := errors.New("callback failed")
	for _, prefix := range []bool{false, true} {
		calls := 0
		callback := func(key, value []byte) error {
			calls++
			return callbackError
		}
		var err error
		if prefix {
			_, err = store.MapPrefixFunc([]byte("prefix:"), callback)
		} else {
			_, err = store.MapFunc(callback)
		}
		if !errors.Is(err, callbackError) || calls != 1 {
			t.Fatalf("prefix=%v: error=%v calls=%d; want callback error after one call", prefix, err, calls)
		}
	}
}

func TestExtentFlushReportsValueIndexFailure(t *testing.T) {
	store := openExtentRegressionStore(t, t.TempDir())
	defer store.Close()
	putExtentRegressionValue(t, store, "key", "value")
	if err := store.valuesIndex.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(); err == nil {
		t.Fatal("Flush succeeded after the values index was closed")
	}
}

func TestExtentConcurrentCacheAccess(t *testing.T) {
	store := openExtentRegressionStore(t, t.TempDir())
	defer store.Close()
	putExtentRegressionValue(t, store, "stable", "value")
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for iteration := 0; iteration < 50; iteration++ {
				switch worker {
				case 0:
					store.ClearCache()
				case 1:
					if err := store.Put([]byte("changing"), []byte("value")); err != nil {
						t.Error(err)
						return
					}
					if err := store.Delete([]byte("changing")); err != nil {
						t.Error(err)
						return
					}
				case 2:
					value, err := store.Get([]byte("stable"))
					if err != nil || string(value) != "value" {
						t.Errorf("Get(stable) = %q, %v; want value, nil", value, err)
						return
					}
				case 3:
					if !store.Exists([]byte("stable")) {
						t.Error("stable key disappeared")
						return
					}
					store.Keys()
				}
			}
		}(worker)
	}
	workers.Wait()
}
