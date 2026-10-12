package ensemblekv

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

var concurrentCompactionBackends = []struct {
	name    string
	creator CreatorFunc
}{
	{"Bolt", BoltDbCreator},
	{"Extent", ExtentCreator},
	{"SQLite", SQLiteCreator},
	{"SingleFile", SingleFileKVCreator},
	{"Json", JsonKVCreator},
	{"Mmap", MmapSingleCreator},
	{"MegaPool", MegapoolCreator},
}

func openConcurrentCompactionStore(t *testing.T, directory string, shards int64, creator CreatorFunc) *EnsembleKv {
	t.Helper()
	store, err := NewEnsembleKv(directory, shards, 4096, 1000, 4*1024*1024, creator)
	if err != nil {
		t.Fatalf("open ensemble at %s: %v", directory, err)
	}
	t.Cleanup(func() {
		if store.running.Load() {
			if err := store.Close(); err != nil {
				t.Errorf("close ensemble at %s: %v", directory, err)
			}
		}
	})
	return store
}

func requireConcurrentCompactionContents(t *testing.T, store *EnsembleKv, expected map[string][]byte, deleted []string) {
	t.Helper()
	keys := store.Keys()
	if len(keys) != len(expected) {
		t.Errorf("expected %d keys, received %d: %q", len(expected), len(keys), keys)
	}
	seen := make(map[string]bool)
	for _, key := range keys {
		if _, exists := expected[string(key)]; !exists || seen[string(key)] {
			t.Errorf("unexpected or duplicate key %q in %q", key, keys)
		}
		seen[string(key)] = true
	}
	for key, value := range expected {
		actual, err := store.Get([]byte(key))
		if err != nil || !bytes.Equal(actual, value) {
			t.Errorf("key %q: expected %q, received %q, error %v", key, value, actual, err)
		}
		if !store.Exists([]byte(key)) {
			t.Errorf("expected key %q to exist", key)
		}
	}
	for _, key := range deleted {
		value, err := store.Get([]byte(key))
		if err == nil || store.Exists([]byte(key)) {
			t.Errorf("deleted key %q was retained with value %q and error %v", key, value, err)
		}
	}
	visited, err := store.MapFunc(func(key, value []byte) error {
		expectedValue, exists := expected[string(key)]
		if !exists || !bytes.Equal(value, expectedValue) {
			return fmt.Errorf("iteration key %q: expected %q with presence %t, received %q", key, expectedValue, exists, value)
		}
		return nil
	})
	if err != nil || len(visited) != len(expected) {
		t.Errorf("iteration expected %d keys, visited %v, error %v", len(expected), visited, err)
	}
}

func concurrentCompactionValue(writer, round, key int) []byte {
	switch key % 4 {
	case 1:
		return []byte{}
	case 2:
		return []byte{0xff, byte(writer), byte(round), 0}
	default:
		return []byte(fmt.Sprintf("writer-%d/round-%d/key-%d", writer, round, key))
	}
}

func writeDuringCompaction(store *EnsembleKv, writer int, firstWrite chan<- int, continueWrites <-chan struct{}) error {
	for round := 0; round < 3; round++ {
		for index := 0; index < 12; index++ {
			key := []byte(fmt.Sprintf("writer-%d/key-%d", writer, index))
			value := concurrentCompactionValue(writer, round, index)
			if err := store.Put(key, value); err != nil {
				return fmt.Errorf("writer %d round %d put %q: %w", writer, round, key, err)
			}
			if round == 0 && index == 0 {
				firstWrite <- writer
				<-continueWrites
			}
			if index%3 == 0 {
				if err := store.Delete(key); err != nil {
					return fmt.Errorf("writer %d round %d delete %q: %w", writer, round, key, err)
				}
			}
		}
	}
	return nil
}

func TestEnsembleCompactionConcurrentMutationsAcrossBackends(t *testing.T) {
	for _, backend := range concurrentCompactionBackends {
		t.Run(backend.name, func(t *testing.T) {
			compactionStarted := make(chan struct{})
			continueCompaction := make(chan struct{})
			var release sync.Once
			defer release.Do(func() { close(continueCompaction) })
			var gateUsed atomic.Bool
			creator := func(directory string, blockSize, fileSize int64) (KvLike, error) {
				store, err := backend.creator(directory, blockSize, fileSize)
				if err != nil {
					return nil, err
				}
				if strings.HasSuffix(directory, ".compacting") && gateUsed.CompareAndSwap(false, true) {
					close(compactionStarted)
					select {
					case <-continueCompaction:
					case <-time.After(30 * time.Second):
						store.Close()
						return nil, fmt.Errorf("compaction creator for %s timed out waiting for writers", directory)
					}
				}
				return store, nil
			}
			directory := t.TempDir()
			store := openConcurrentCompactionStore(t, directory, 3, creator)
			expected := map[string][]byte{"stable": {0, 1, 255}}
			deleted := make([]string, 0)
			if err := store.Put([]byte("stable"), expected["stable"]); err != nil {
				t.Fatal(err)
			}
			for writer := 0; writer < 3; writer++ {
				for index := 0; index < 12; index++ {
					key := fmt.Sprintf("writer-%d/key-%d", writer, index)
					if index < 6 {
						if err := store.Put([]byte(key), []byte("initial")); err != nil {
							t.Fatalf("initial put %q: %v", key, err)
						}
					}
					if index%3 == 0 {
						deleted = append(deleted, key)
					} else {
						expected[key] = concurrentCompactionValue(writer, 2, index)
					}
				}
			}
			completed := make(chan error, 5)
			for compactor := 0; compactor < 2; compactor++ {
				go func() {
					for round := 0; round < 2; round++ {
						if err := store.ConcurrentCompact(); err != nil {
							completed <- fmt.Errorf("compactor %d round %d: %w", compactor, round, err)
							return
						}
					}
					completed <- nil
				}()
			}
			select {
			case <-compactionStarted:
			case err := <-completed:
				t.Fatalf("compaction ended before creating replacement: %v", err)
			case <-time.After(30 * time.Second):
				t.Fatal("compaction did not reach replacement creation within 30 seconds")
			}
			firstWrite := make(chan int, 3)
			for writer := 0; writer < 3; writer++ {
				go func() { completed <- writeDuringCompaction(store, writer, firstWrite, continueCompaction) }()
			}
			for range 3 {
				select {
				case <-firstWrite:
				case err := <-completed:
					t.Fatalf("writer ended before all writers started: %v", err)
				case <-time.After(30 * time.Second):
					t.Fatal("writers did not start within 30 seconds")
				}
			}
			release.Do(func() { close(continueCompaction) })
			for range 5 {
				select {
				case err := <-completed:
					if err != nil {
						t.Errorf("concurrent mutation or compaction failed: %v", err)
					}
				case <-time.After(30 * time.Second):
					t.Fatal("concurrent mutation or compaction did not complete within 30 seconds")
				}
			}
			requireConcurrentCompactionContents(t, store, expected, deleted)
			if err := store.ConcurrentCompact(); err != nil {
				t.Fatalf("repeat compaction after writers finished: %v", err)
			}
			requireConcurrentCompactionContents(t, store, expected, deleted)
			if err := store.Close(); err != nil {
				t.Fatalf("close compacted ensemble: %v", err)
			}
			reopened := openConcurrentCompactionStore(t, directory, 3, backend.creator)
			requireConcurrentCompactionContents(t, reopened, expected, deleted)
		})
	}
}

func TestEnsembleCompactionMutationOrderingAcrossBackends(t *testing.T) {
	for _, backend := range concurrentCompactionBackends {
		t.Run(backend.name, func(t *testing.T) {
			directory := t.TempDir()
			store := openConcurrentCompactionStore(t, directory, 1, backend.creator)
			put := func(key string, value []byte) {
				t.Helper()
				if err := store.Put([]byte(key), value); err != nil {
					t.Fatalf("put %q = %q: %v", key, value, err)
				}
			}
			remove := func(key string) {
				t.Helper()
				if err := store.Delete([]byte(key)); err != nil {
					t.Fatalf("delete %q: %v", key, err)
				}
			}
			initial := map[string][]byte{
				"delete-before":          []byte("original"),
				"update-before":          []byte("original"),
				"reinsert-before":        []byte("original"),
				"put-then-delete-before": []byte("original"),
				"delete-after":           []byte("original"),
				"update-after":           []byte("original"),
				"stable":                 []byte("unchanged"),
				"empty":                  {},
				"binary":                 {0, 255, 0},
			}
			for key, value := range initial {
				put(key, value)
			}
			state, snapshot, err := store.prepareCompaction(0)
			if err != nil {
				t.Fatalf("prepare compaction: %v", err)
			}
			keys := compactionTestKeys(t, snapshot)
			t.Cleanup(func() {
				if err := store.abortCompaction(0, state); err != nil {
					t.Errorf("release compaction state: %v", err)
				}
			})
			if len(keys) != len(initial) {
				t.Fatalf("expected snapshot of %d keys, received %q", len(initial), keys)
			}
			remove("delete-before")
			put("update-before", []byte("updated before copy"))
			remove("reinsert-before")
			put("reinsert-before", []byte("reinserted before copy"))
			put("put-then-delete-before", []byte("updated before deletion"))
			remove("put-then-delete-before")
			put("new-before", []byte("absent from snapshot"))
			put("new-deleted", []byte("absent from snapshot"))
			remove("new-deleted")
			put("new-reinserted", []byte("first insertion"))
			remove("new-reinserted")
			put("new-reinserted", []byte("second insertion"))
			for _, key := range keys {
				if err := store.copyCompactionKey(0, state, key); err != nil {
					t.Fatalf("copy snapshot key %q: %v", key, err)
				}
			}
			remove("delete-after")
			put("update-after", []byte("updated after copy"))
			put("new-after", []byte("created after copy"))
			if err := store.finishCompaction(0, state); err != nil {
				t.Fatalf("publish compacted store: %v", err)
			}
			expected := map[string][]byte{
				"update-before":   []byte("updated before copy"),
				"reinsert-before": []byte("reinserted before copy"),
				"update-after":    []byte("updated after copy"),
				"stable":          []byte("unchanged"),
				"empty":           {},
				"binary":          {0, 255, 0},
				"new-before":      []byte("absent from snapshot"),
				"new-reinserted":  []byte("second insertion"),
				"new-after":       []byte("created after copy"),
			}
			deleted := []string{"delete-before", "put-then-delete-before", "delete-after", "new-deleted"}
			requireConcurrentCompactionContents(t, store, expected, deleted)
			if err := store.ConcurrentCompact(); err != nil {
				t.Fatalf("repeat compaction after ordered mutations: %v", err)
			}
			requireConcurrentCompactionContents(t, store, expected, deleted)
			if err := store.Close(); err != nil {
				t.Fatalf("close compacted ensemble: %v", err)
			}
			reopened := openConcurrentCompactionStore(t, directory, 1, backend.creator)
			requireConcurrentCompactionContents(t, reopened, expected, deleted)
		})
	}
}

func TestEnsembleCompactionFailedMutationsPreventPublication(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		closeSource bool
		deleteKey   bool
	}{
		{name: "target-write-failure"},
		{name: "source-write-failure", closeSource: true},
		{name: "target-delete-failure", deleteKey: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			directory := t.TempDir()
			store := openConcurrentCompactionStore(t, directory, 1, BoltDbCreator)
			expected := map[string][]byte{"victim": []byte("original"), "keep": []byte("stable")}
			for key, value := range expected {
				if err := store.Put([]byte(key), value); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Flush(); err != nil {
				t.Fatal(err)
			}
			state, snapshot, err := store.prepareCompaction(0)
			if err != nil {
				t.Fatal(err)
			}
			keys := compactionTestKeys(t, snapshot)
			t.Cleanup(func() {
				if err := store.abortCompaction(0, state); err != nil {
					t.Errorf("abort failed compaction: %v", err)
				}
			})
			for _, key := range keys {
				if err := store.copyCompactionKey(0, state, key); err != nil {
					t.Fatal(err)
				}
			}
			failedStore := state.target.(*BoltDbShim)
			if testCase.closeSource {
				source, _ := store.substores.Load(0)
				failedStore = source.(*BoltDbShim)
			}
			if err := failedStore.boltHandle.Close(); err != nil {
				t.Fatal(err)
			}
			deleted := make([]string, 0)
			if testCase.deleteKey {
				err = store.Delete([]byte("victim"))
				delete(expected, "victim")
				deleted = append(deleted, "victim")
			} else {
				err = store.Put([]byte("victim"), []byte("updated"))
				if !testCase.closeSource {
					expected["victim"] = []byte("updated")
				}
			}
			if !errors.Is(err, bolt.ErrDatabaseNotOpen) || !errors.Is(state.failure, bolt.ErrDatabaseNotOpen) {
				t.Fatalf("expected closed Bolt failure retained by compaction: mutation error %v, state failure %v", err, state.failure)
			}
			if !testCase.closeSource {
				if dirty, _ := store.flushStore.Load(0); !dirty {
					t.Error("source mutation succeeded but its dirty flag was not retained after target failure")
				}
				if err := store.Put([]byte("after-failure"), []byte("source remains writable")); err != nil {
					t.Fatalf("write to source after compaction was invalidated: %v", err)
				}
				expected["after-failure"] = []byte("source remains writable")
				requireConcurrentCompactionContents(t, store, expected, deleted)
			}
			if err := store.finishCompaction(0, state); !errors.Is(err, bolt.ErrDatabaseNotOpen) {
				t.Fatalf("expected publication rejected with the retained Bolt error, received %v", err)
			}
			if err := store.abortCompaction(0, state); err != nil {
				t.Fatalf("abort invalidated compaction: %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened := openConcurrentCompactionStore(t, directory, 1, BoltDbCreator)
			requireConcurrentCompactionContents(t, reopened, expected, deleted)
		})
	}
}

func compactionTestKeys(t *testing.T, index *diskKeyIndex) [][]byte {
	t.Helper()
	var keys [][]byte
	if err := index.each(func(key []byte, state compactionKeyState) error {
		if state == compactionPending {
			keys = append(keys, key)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return keys
}

func TestEnsembleCompactionClosedTargetUnregisteredBeforeAbort(t *testing.T) {
	directory := t.TempDir()
	store := openConcurrentCompactionStore(t, directory, 1, MmapSingleCreator)
	expected := map[string][]byte{"original": []byte("preserved")}
	if err := store.Put([]byte("original"), expected["original"]); err != nil {
		t.Fatal(err)
	}
	state, snapshot, err := store.prepareCompaction(0)
	if err != nil {
		t.Fatal(err)
	}
	keys := compactionTestKeys(t, snapshot)
	t.Cleanup(func() {
		if err := store.abortCompaction(0, state); err != nil {
			t.Errorf("abort unpublished compaction: %v", err)
		}
	})
	for _, key := range keys {
		if err := store.copyCompactionKey(0, state, key); err != nil {
			t.Fatal(err)
		}
	}
	invalidEntry := filepath.Join(state.subPath+".compacting", "invalid-entry")
	if err := os.Symlink(".", invalidEntry); err != nil {
		t.Fatal(err)
	}
	if err := store.finishCompaction(0, state); err == nil {
		t.Fatal("expected publication to reject the symbolic link during target directory sync")
	}
	if !state.targetClosed || state.sourceClosed {
		t.Fatalf("expected closed target and open source, received targetClosed=%t sourceClosed=%t", state.targetClosed, state.sourceClosed)
	}
	if _, active := store.compactingStores.Load(0); active {
		t.Fatal("closed mapped target remains registered for writes after publication failure")
	}
	if err := store.Put([]byte("after-failure"), []byte("source remains writable")); err != nil {
		t.Fatalf("write before deferred compaction cleanup: %v", err)
	}
	expected["after-failure"] = []byte("source remains writable")
	requireConcurrentCompactionContents(t, store, expected, nil)
	if err := store.abortCompaction(0, state); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openConcurrentCompactionStore(t, directory, 1, MmapSingleCreator)
	requireConcurrentCompactionContents(t, reopened, expected, nil)
}
