package ensemblekv

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	nudb "github.com/iand/gonudb"
)

func TestStoreMetricsLiveKeysAndBytes(t *testing.T) {
	creators := map[string]CreatorFunc{
		"Bolt": BoltDbCreator, "SQLite": SQLiteCreator, "Json": JsonKVCreator,
		"Mmap": MmapSingleCreator, "MegaPool": MegapoolCreator, "Extent": ExtentCreator,
		"SingleFile": SingleFileKVCreator, "Pudge": PudgeCreator,
		"Line": func(directory string, blockSize, fileSize int64) (KvLike, error) {
			return NewLinelsm(directory, blockSize, 1000, fileSize, BoltDbCreator)
		},
	}
	for name, creator := range creators {
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			store, err := creator(directory, 4096, 4*1024*1024)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if store != nil {
					if err := store.Close(); err != nil {
						t.Error(err)
					}
				}
			}()
			expected := map[string][]byte{}
			assertStoreMetrics(t, store, expected)
			keys := []string{"plain", "é\x00界", "binary\xff*", "empty-value"}
			for index, key := range keys {
				value := bytes.Repeat([]byte{byte(index + 1)}, index*7)
				if err := store.Put([]byte(key), value); err != nil {
					t.Fatal(err)
				}
				expected[key] = value
				assertStoreMetrics(t, store, expected)
			}
			for _, length := range []int{128, 3, 51} {
				value := bytes.Repeat([]byte{'v'}, length)
				if err := store.Put([]byte(keys[0]), value); err != nil {
					t.Fatal(err)
				}
				expected[keys[0]] = value
				assertStoreMetrics(t, store, expected)
			}
			if err := store.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := store.Delete([]byte(keys[1])); err != nil {
				t.Fatal(err)
			}
			delete(expected, keys[1])
			assertStoreMetrics(t, store, expected)
			if err := store.Put([]byte(keys[2]), []byte{}); err != nil {
				t.Fatal(err)
			}
			expected[keys[2]] = []byte{}
			assertStoreMetrics(t, store, expected)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store = nil
			store, err = creator(directory, 4096, 4*1024*1024)
			if err != nil {
				t.Fatal(err)
			}
			assertStoreMetrics(t, store, expected)
		})
	}
}

func assertStoreMetrics(t *testing.T, store KvLike, expected map[string][]byte) {
	t.Helper()
	metrics, ok := store.(interface {
		Count() (int64, error)
		SizeBytes() (int64, error)
	})
	if !ok {
		t.Fatalf("store %T has no explicit key-count and byte-size operations", store)
	}
	count, err := metrics.Count()
	if err != nil || count != int64(len(expected)) {
		t.Fatalf("Count() = %d, %v; expected %d live keys", count, err, len(expected))
	}
	var expectedBytes int64
	for key, value := range expected {
		expectedBytes += int64(len(key) + len(value))
	}
	size, err := metrics.SizeBytes()
	if err != nil || size != expectedBytes {
		t.Fatalf("SizeBytes() = %d, %v; expected %d live key and value bytes", size, err, expectedBytes)
	}
}

func TestNuDBMetricsIncludeBufferedInserts(t *testing.T) {
	store, err := NewNuDbShim(filepath.Join(t.TempDir(), "data"), 4096, 4*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	expected := map[string][]byte{}
	assertStoreMetrics(t, store, expected)
	for _, key := range []string{"plain", "é\x00界", "binary\xff*"} {
		value := []byte("value:" + key)
		if err := store.Put([]byte(key), value); err != nil {
			t.Fatal(err)
		}
		expected[key] = value
		assertStoreMetrics(t, store, expected)
	}
}

func TestLineThresholdUsesLogicalBytes(t *testing.T) {
	for name, creator := range map[string]CreatorFunc{"Bolt": BoltDbCreator, "Mmap": MmapSingleCreator, "SingleFile": SingleFileKVCreator} {
		t.Run(name, func(t *testing.T) {
			store, err := NewLinelsm(t.TempDir(), 4096, 1000, 4*1024*1024, creator)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			store.maxTierSizes[0] = 64
			if err := store.Put([]byte("a"), []byte("small")); err != nil {
				t.Fatal(err)
			}
			if len(store.tiers[0]) != 1 || len(store.tiers[1]) != 0 {
				t.Fatal("small logical record flushed because backing storage reserves space")
			}
			if err := store.Put([]byte("a"), bytes.Repeat([]byte{'x'}, 80)); err != nil {
				t.Fatal(err)
			}
			if len(store.tiers[0]) != 0 || len(store.tiers[1]) != 1 {
				t.Fatal("large value did not flush the byte-limited tier")
			}
			assertStoreMetrics(t, store, map[string][]byte{"a": bytes.Repeat([]byte{'x'}, 80)})
		})
	}
}

func TestStoreMetricsPropagateClosedStorageErrors(t *testing.T) {
	for name, creator := range map[string]CreatorFunc{
		"Bolt": BoltDbCreator, "SQLite": SQLiteCreator, "Pudge": PudgeCreator, "Mmap": MmapSingleCreator,
		"MegaPool": MegapoolCreator, "SingleFile": SingleFileKVCreator, "Extent": ExtentCreator,
	} {
		t.Run(name, func(t *testing.T) {
			store, err := creator(t.TempDir(), 4096, 4*1024*1024)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			metrics := store.(interface {
				Count() (int64, error)
				SizeBytes() (int64, error)
			})
			if _, err := metrics.Count(); err == nil {
				t.Error("Count accepted closed storage")
			}
			if _, err := metrics.SizeBytes(); err == nil {
				t.Error("SizeBytes accepted closed storage")
			}
		})
	}
}

func TestMissingKeyErrorsRemainDistinctFromStorageErrors(t *testing.T) {
	creators := map[string]CreatorFunc{
		"Bolt": BoltDbCreator, "SQLite": SQLiteCreator, "Pudge": PudgeCreator, "Mmap": MmapSingleCreator,
		"MegaPool": MegapoolCreator, "SingleFile": SingleFileKVCreator, "Extent": ExtentCreator,
		"Json": JsonKVCreator,
		"Line": func(directory string, blockSize, fileSize int64) (KvLike, error) {
			return NewLinelsm(directory, blockSize, 1000, fileSize, BoltDbCreator)
		},
	}
	for name, creator := range creators {
		t.Run(name, func(t *testing.T) {
			store, err := creator(t.TempDir(), 4096, 4*1024*1024)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			key := []byte("missing")
			if _, err := store.Get(key); !errors.Is(err, errKeyNotFound) {
				t.Fatalf("missing-key Get returned %v; expected errKeyNotFound", err)
			}
			if err := store.Put(key, []byte("present")); err != nil {
				t.Fatal(err)
			}
			if err := store.Delete(key); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Get(key); !errors.Is(err, errKeyNotFound) {
				t.Fatalf("deleted-key Get returned %v; expected errKeyNotFound", err)
			}
		})
	}
}

func TestLineGetPropagatesBackingReadFailure(t *testing.T) {
	store, err := NewLinelsm(t.TempDir(), 4096, 1000, 4*1024*1024, BoltDbCreator)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("present"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := store.tiers[0][0].Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get([]byte("present")); err == nil || errors.Is(err, errKeyNotFound) {
		t.Fatalf("closed backing store Get returned %v; expected a storage error", err)
	}
}

func TestMmapGetAfterCloseReportsStorageError(t *testing.T) {
	store, err := NewMmapSingleKV(t.TempDir(), 4096, 4*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("present"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"present", "missing"} {
		if _, err := store.Get([]byte(key)); err == nil || errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("Get(%q) after Close returned %v; expected a storage lifecycle error", key, err)
		}
	}
	if err := store.Put([]byte("after-close"), []byte("value")); err == nil {
		t.Fatal("Put accepted a closed mapping")
	}
	if err := store.Flush(); err == nil {
		t.Fatal("Flush accepted a closed mapping")
	}
}

func TestMissingKeyErrorsPreserveNativeSentinels(t *testing.T) {
	for _, backend := range []struct {
		name        string
		creator     CreatorFunc
		nativeError error
	}{
		{"MegaPool", MegapoolCreator, os.ErrNotExist},
		{"NuDB", NuDbCreator, nudb.ErrKeyNotFound},
	} {
		t.Run(backend.name, func(t *testing.T) {
			store, err := backend.creator(t.TempDir(), 4096, 4*1024*1024)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			_, err = store.Get([]byte("missing"))
			if !errors.Is(err, ErrKeyNotFound) || !errors.Is(err, backend.nativeError) {
				t.Fatalf("Get returned %v; expected both ErrKeyNotFound and native cause %v", err, backend.nativeError)
			}
		})
	}
}
