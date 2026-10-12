package ensemblekv

import (
	"bytes"
	"errors"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func TestSingleFileStreamsHistoryInPublicationOrder(t *testing.T) {
	store := openSingleFileRegressionStore(t, filepath.Join(t.TempDir(), "store"))
	assertSFPut(t, store, []byte("a"), []byte("old"), "first table")
	assertSFPut(t, store, []byte("b"), []byte{}, "first table")
	assertSFFlush(t, store, "first table")
	assertSFDelete(t, store, []byte("a"), "second table")
	assertSFPut(t, store, []byte("b"), []byte("new"), "second table")
	assertSFFlush(t, store, "second table")
	assertSFPut(t, store, []byte("a"), []byte("newest"), "memtable")
	var records []string
	err := store.streamKeyRecords(func(key []byte, live bool) error {
		state := "deleted"
		if live {
			state = "live"
		}
		records = append(records, string(key)+":"+state)
		return nil
	})
	expected := []string{"a:live", "b:live", "a:deleted", "b:live", "a:live"}
	if err != nil || !reflect.DeepEqual(records, expected) {
		t.Fatalf("stream records=%q, expected=%q, error=%v", records, expected, err)
	}
}

func TestSingleFileStreamChecksumsUseBoundedMemory(t *testing.T) {
	store := openSingleFileRegressionStore(t, filepath.Join(t.TempDir(), "store"))
	value := bytes.Repeat([]byte("large-value"), 2<<20)
	assertSFPut(t, store, []byte("large"), value, "large table")
	assertSFFlush(t, store, "large table")
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	count := 0
	err := store.streamKeyRecords(func(key []byte, live bool) error {
		count++
		if string(key) != "large" || !live {
			t.Fatalf("unexpected key %q live=%v", key, live)
		}
		return nil
	})
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	if err != nil || count != 1 {
		t.Fatalf("large table enumeration: count=%d error=%v", count, err)
	}
	if allocated > 512<<10 {
		t.Fatalf("enumerating one %d-byte value allocated %d bytes; expected at most 512KiB", len(value), allocated)
	}
	meta := &store.sstables[0]
	writeSingleFileBytes(t, store, []byte("!"), meta.offset+meta.size-1)
	if err := store.streamKeyRecords(func(key []byte, live bool) error { return nil }); err == nil {
		t.Fatal("stream accepted corruption at the end of a large value")
	}
}

func TestSingleFileStreamPropagatesCallbackFailure(t *testing.T) {
	store := openSingleFileRegressionStore(t, filepath.Join(t.TempDir(), "store"))
	for _, key := range []string{"a", "b", "c"} {
		assertSFPut(t, store, []byte(key), []byte("value"), "table")
	}
	assertSFFlush(t, store, "table")
	failure := errors.New("stop iteration")
	calls := 0
	err := store.streamKeyRecords(func(key []byte, live bool) error {
		calls++
		return failure
	})
	if !errors.Is(err, failure) || calls != 1 {
		t.Fatalf("callback failure: calls=%d error=%v", calls, err)
	}
}
