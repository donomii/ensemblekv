package ensemblekv

import (
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func TestSingleFileKeysUseNewestSSTable(t *testing.T) {
	store := openSingleFileRegressionStore(t, filepath.Join(t.TempDir(), "store.lsm"))
	assertSFPut(t, store, []byte("deleted"), []byte("old"), "first table")
	assertSFDelete(t, store, []byte("reinserted"), "first table")
	assertSFFlush(t, store, "first table")
	assertSFDelete(t, store, []byte("deleted"), "second table")
	assertSFPut(t, store, []byte("reinserted"), []byte("new"), "second table")
	assertSFFlush(t, store, "second table")
	keys := make([]string, 0)
	for _, key := range store.Keys() {
		keys = append(keys, string(key))
	}
	sort.Strings(keys)
	if !reflect.DeepEqual(keys, []string{"reinserted"}) {
		t.Fatalf("expected only reinserted key, received %q", keys)
	}
	visited, err := store.MapFunc(func(key, value []byte) error { return nil })
	if err != nil || !reflect.DeepEqual(visited, map[string]bool{"reinserted": true}) {
		t.Fatalf("expected iteration over reinserted key, received %v, error %v", visited, err)
	}
}
