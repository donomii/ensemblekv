package ensemblekv

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func TestEnsembleMetricsTrackActualMutations(t *testing.T) {
	for _, backend := range concurrentCompactionBackends {
		t.Run(backend.name, func(t *testing.T) {
			directory := t.TempDir()
			store := openConcurrentCompactionStore(t, directory, 3, backend.creator)
			expected := map[string][]byte{}
			assertEnsembleMetrics(t, store, expected)
			for index := 0; index < 8; index++ {
				key := fmt.Sprintf("key-%d", index)
				value := bytes.Repeat([]byte{byte(index)}, index*3)
				if err := store.Put([]byte(key), value); err != nil {
					t.Fatal(err)
				}
				expected[key] = value
			}
			assertEnsembleMetrics(t, store, expected)
			for repeat := 0; repeat < 3; repeat++ {
				for key := range expected {
					value := []byte(fmt.Sprintf("replacement-%d", repeat))
					if err := store.Put([]byte(key), value); err != nil {
						t.Fatal(err)
					}
					expected[key] = value
				}
				assertEnsembleMetrics(t, store, expected)
			}
			for _, key := range []string{"key-0", "key-2", "key-4"} {
				if err := store.Delete([]byte(key)); err != nil {
					t.Fatal(err)
				}
				delete(expected, key)
				assertEnsembleMetrics(t, store, expected)
			}
			for repeat := 0; repeat < 3; repeat++ {
				for _, key := range []string{"missing", "key-0"} {
					deleteErr := store.Delete([]byte(key))
					if _, err := store.Get([]byte(key)); !errors.Is(err, ErrKeyNotFound) {
						t.Fatalf("Delete(%q) = %v; subsequent Get returned %v, expected absent key", key, deleteErr, err)
					}
				}
				assertEnsembleMetrics(t, store, expected)
			}
			if err := store.Put([]byte("key-0"), []byte{}); err != nil {
				t.Fatal(err)
			}
			expected["key-0"] = []byte{}
			assertEnsembleMetrics(t, store, expected)
			if err := store.ConcurrentCompact(); err != nil {
				t.Fatal(err)
			}
			assertEnsembleMetrics(t, store, expected)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store = openConcurrentCompactionStore(t, directory, 3, backend.creator)
			assertEnsembleMetrics(t, store, expected)
		})
	}
}

func assertEnsembleMetrics(t *testing.T, store *EnsembleKv, expected map[string][]byte) {
	t.Helper()
	assertStoreMetrics(t, store, expected)
	if count := store.totalKeys.Load(); count != int64(len(expected)) {
		t.Fatalf("totalKeys = %d; expected %d distinct live keys", count, len(expected))
	}
	for key, expectedValue := range expected {
		value, err := store.Get([]byte(key))
		if err != nil || !bytes.Equal(value, expectedValue) {
			t.Fatalf("Get(%q) = %x, %v; expected %x", key, value, err, expectedValue)
		}
	}
}

func TestEnsembleMetricsFollowNilValueSemantics(t *testing.T) {
	for _, backend := range concurrentCompactionBackends {
		t.Run(backend.name, func(t *testing.T) {
			store := openConcurrentCompactionStore(t, t.TempDir(), 1, backend.creator)
			key := []byte("nil-value")
			if err := store.Put(key, []byte("present")); err != nil {
				t.Fatal(err)
			}
			expected := map[string][]byte{"nil-value": []byte("present")}
			err := store.Put(key, nil)
			switch backend.name {
			case "SingleFile":
				if err != nil {
					t.Fatal(err)
				}
				delete(expected, string(key))
			case "SQLite":
				if err == nil {
					t.Fatal("SQLite accepted a NULL value despite its NOT NULL constraint")
				}
			default:
				if err != nil {
					t.Fatal(err)
				}
				expected[string(key)] = nil
			}
			assertEnsembleMetrics(t, store, expected)
		})
	}
}

func TestEnsembleMetricsRecoverFromStaleMetadata(t *testing.T) {
	for _, staleCount := range []int64{-8, 0, 999} {
		t.Run(fmt.Sprint(staleCount), func(t *testing.T) {
			directory := t.TempDir()
			store := openConcurrentCompactionStore(t, directory, 2, BoltDbCreator)
			expected := map[string][]byte{"first": []byte("value"), "second": []byte{}}
			for key, value := range expected {
				if err := store.Put([]byte(key), value); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store.totalKeys.Store(staleCount)
			if err := store.saveMetadata(); err != nil {
				t.Fatal(err)
			}
			store = openConcurrentCompactionStore(t, directory, 2, BoltDbCreator)
			assertEnsembleMetrics(t, store, expected)
		})
	}
}

func TestEnsembleMetricsTrackPartiallyFailedMutations(t *testing.T) {
	directory := t.TempDir()
	store := openConcurrentCompactionStore(t, directory, 1, JsonKVCreator)
	backing, _ := store.substores.Load(0)
	jsonStore := backing.(*JsonKV)
	originalFilename := jsonStore.filename
	setFilename := func(filename string) {
		t.Helper()
		if err := store.runInSubstoreWithErr(0, func(backing KvLike) error {
			jsonStore.mutex.Lock()
			defer jsonStore.mutex.Unlock()
			jsonStore.filename = filename
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		jsonStore.mutex.Lock()
		defer jsonStore.mutex.Unlock()
		jsonStore.filename = originalFilename
	})
	key := []byte("partial")
	setFilename(filepath.Join(directory, "absent-parent", "store.json"))
	if err := store.Put(key, []byte("value")); err == nil {
		t.Fatal("write to a missing parent directory succeeded")
	}
	assertEnsembleMetrics(t, store, map[string][]byte{"partial": []byte("value")})
	if dirty, _ := store.flushStore.Load(0); !dirty {
		t.Fatal("partially successful insertion was not marked for persistence")
	}
	if err := store.Delete(key); err == nil {
		t.Fatal("delete persistence to a missing parent directory succeeded")
	}
	assertEnsembleMetrics(t, store, map[string][]byte{})
	setFilename(originalFilename)
	if err := store.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openConcurrentCompactionStore(t, directory, 1, JsonKVCreator)
	assertEnsembleMetrics(t, store, map[string][]byte{})
}

func TestEnsembleMetricsConcurrentWriters(t *testing.T) {
	store := openConcurrentCompactionStore(t, t.TempDir(), 4, BoltDbCreator)
	const writers = 8
	const records = 20
	errors := make(chan error, writers)
	var workers sync.WaitGroup
	for writer := 0; writer < writers; writer++ {
		workers.Add(1)
		go func(writer int) {
			defer workers.Done()
			for index := 0; index < records; index++ {
				key := []byte(fmt.Sprintf("writer-%d/key-%d", writer, index))
				for repeat := 0; repeat < 3; repeat++ {
					if err := store.Put(key, []byte("final")); err != nil {
						errors <- err
						return
					}
				}
				if index%2 == 0 {
					if err := store.Delete(key); err != nil {
						errors <- err
						return
					}
				}
				if err := store.Put([]byte("shared"), []byte("value")); err != nil {
					errors <- err
					return
				}
			}
		}(writer)
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	expected := map[string][]byte{"shared": []byte("value")}
	for writer := 0; writer < writers; writer++ {
		for index := 1; index < records; index += 2 {
			expected[fmt.Sprintf("writer-%d/key-%d", writer, index)] = []byte("final")
		}
	}
	assertEnsembleMetrics(t, store, expected)
}

func TestEnsembleMetricsRejectUnknownCountUntilReopen(t *testing.T) {
	directory := t.TempDir()
	store := openConcurrentCompactionStore(t, directory, 1, BoltDbCreator)
	key := []byte("persisted-before-close")
	err := store.runInSubstoreWithErr(0, func(backing KvLike) error {
		return store.countMutation(0, backing, key, func() error {
			if err := backing.Put(key, []byte("value")); err != nil {
				return err
			}
			return backing.Close()
		})
	})
	if err == nil {
		t.Fatal("mutation accepted a failed post-write read from closed storage")
	}
	if _, err := store.Count(); err == nil {
		t.Fatal("Count returned a result after the post-write state became unreadable")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openConcurrentCompactionStore(t, directory, 1, BoltDbCreator)
	assertEnsembleMetrics(t, store, map[string][]byte{string(key): []byte("value")})
}

func TestEnsembleMetricsKeepSourceCountAfterDualWriteFailure(t *testing.T) {
	store := openConcurrentCompactionStore(t, t.TempDir(), 1, BoltDbCreator)
	state, _, err := store.prepareCompaction(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.abortCompaction(0, state); err != nil {
			t.Error(err)
		}
	})
	if err := state.target.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("source-only"), []byte("value")); err == nil {
		t.Fatal("dual write accepted a closed replacement store")
	}
	assertEnsembleMetrics(t, store, map[string][]byte{"source-only": []byte("value")})
	if err := store.finishCompaction(0, state); err == nil {
		t.Fatal("compaction accepted its failed replacement write")
	}
}
