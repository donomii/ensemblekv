package ensemblekv

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestCompactionDiskIndexPreservesLargeKeys(t *testing.T) {
	directory := t.TempDir()
	store := openConcurrentCompactionStore(t, directory, 1, ExtentCreator)
	key := bytes.Repeat([]byte("long-key-"), 5000)
	if len(key) <= bolt.MaxKeySize {
		t.Fatal("fixture does not exceed the scratch engine's key limit")
	}
	if err := store.Put(key, []byte("retained")); err != nil {
		t.Fatal(err)
	}
	if err := store.ConcurrentCompact(); err != nil {
		t.Fatal(err)
	}
	if value, err := store.Get(key); err != nil || string(value) != "retained" {
		t.Fatalf("large key after compaction: value=%q error=%v", value, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openConcurrentCompactionStore(t, directory, 1, ExtentCreator)
	if value, err := store.Get(key); err != nil || string(value) != "retained" {
		t.Fatalf("large key after reopening: value=%q error=%v", value, err)
	}
}

func TestCompactionDiskIndexResolvesDuplicateAndDeletedRecords(t *testing.T) {
	store, err := NewExtentKeyValueStore(t.TempDir(), 4096, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, key := range []string{"updated", "deleted", "reinserted", "empty"} {
		if err := store.Put([]byte(key), []byte("old")); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{"deleted", "reinserted"} {
		if err := store.Delete([]byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	for key, value := range map[string][]byte{"updated": []byte("latest"), "reinserted": []byte("new"), "empty": {}} {
		if err := store.Put([]byte(key), value); err != nil {
			t.Fatal(err)
		}
	}
	index, err := newDiskKeyIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer index.close()
	if err := index.collect(store); err != nil {
		t.Fatal(err)
	}
	states := make(map[string]compactionKeyState)
	if err := index.each(func(key []byte, state compactionKeyState) error {
		states[string(key)] = state
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	expected := map[string]compactionKeyState{
		"updated": compactionPending, "deleted": compactionDeleted,
		"reinserted": compactionPending, "empty": compactionPending,
	}
	if !reflect.DeepEqual(states, expected) {
		t.Fatalf("resolved states=%v, expected=%v", states, expected)
	}
}

func TestCompactionDiskIndexFailureCleanup(t *testing.T) {
	store, err := NewBoltDbShim(t.TempDir(), 4096, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, key := range []string{"first", "second"} {
		if err := store.Put([]byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	scratch := t.TempDir()
	for _, variable := range []string{"TMPDIR", "TMP", "TEMP"} {
		t.Setenv(variable, scratch)
	}
	failure := errors.New("stop key visitation")
	for _, raw := range []bool{true, false} {
		calls := 0
		visit := func(key []byte) error {
			calls++
			return failure
		}
		var err error
		if raw {
			err = streamKeyRecords(store, func(key []byte, live bool) error { return visit(key) })
		} else {
			err = forEachLiveKey(store, visit)
		}
		if !errors.Is(err, failure) || calls != 1 {
			t.Fatalf("callback failure raw=%v: calls=%d error=%v", raw, calls, err)
		}
		entries, err := os.ReadDir(scratch)
		if err != nil || len(entries) != 0 {
			t.Fatalf("callback failure left scratch files: entries=%v error=%v", entries, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := forEachLiveKey(store, func(key []byte) error { return nil }); !errors.Is(err, bolt.ErrDatabaseNotOpen) {
		t.Fatalf("closed source enumeration: %v", err)
	}
	entries, err := os.ReadDir(scratch)
	if err != nil || len(entries) != 0 {
		t.Fatalf("source failure left scratch files: entries=%v error=%v", entries, err)
	}
}

func TestCompactionNestedScratchStaysUnderOwnedParent(t *testing.T) {
	for _, manager := range []bool{false, true} {
		name := "MegaPool"
		if manager {
			name = "LineLSM/MegaPool"
		}
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			var source KvLike
			var pool *MegaPool
			if manager {
				line, err := NewLinelsm(directory, 4096, 1000, 1<<20, MegapoolCreator)
				if err != nil {
					t.Fatal(err)
				}
				source = line
			} else {
				var err error
				pool, err = OpenMegaPool(filepath.Join(directory, "pool.db"), 1<<20)
				if err != nil {
					t.Fatal(err)
				}
				source = pool
			}
			defer func() {
				if err := source.Close(); err != nil {
					t.Error(err)
				}
			}()
			for _, key := range []string{"alpha", "beta", "gamma"} {
				if err := source.Put([]byte(key), []byte("value")); err != nil {
					t.Fatal(err)
				}
			}
			if manager {
				pool = source.(*Linelsm).tiers[0][0].(*MegaPool)
			}
			owned, global := t.TempDir(), t.TempDir()
			for _, variable := range []string{"TMPDIR", "TMP", "TEMP"} {
				t.Setenv(variable, global)
			}
			assertEmpty := func(path string) {
				t.Helper()
				entries, err := os.ReadDir(path)
				if err != nil || len(entries) != 0 {
					t.Fatalf("scratch path %s has entries=%v error=%v", path, entries, err)
				}
			}
			assertOwned := func() error {
				assertEmpty(global)
				databases, err := filepath.Glob(filepath.Join(owned, ".ensemble-keys-*", "keys.db"))
				if err != nil || len(databases) == 0 {
					return fmt.Errorf("raw callback found no active scratch database under %s: %v", owned, err)
				}
				return nil
			}
			visitorFailure := errors.New("stop nested visitation")
			for _, failVisitor := range []bool{false, true} {
				calls := 0
				err := streamKeyRecordsAt(source, owned, func(key []byte, live bool) error {
					calls++
					if err := assertOwned(); err != nil {
						return err
					}
					if failVisitor {
						return visitorFailure
					}
					return nil
				})
				if failVisitor {
					if !errors.Is(err, visitorFailure) || calls != 1 {
						t.Fatalf("nested visitor failure: calls=%d error=%v", calls, err)
					}
				} else if err != nil || calls != 3 {
					t.Fatalf("nested enumeration: calls=%d error=%v", calls, err)
				}
				assertEmpty(owned)
				assertEmpty(global)
			}
			root := pool.nodeAt(pool.header.BtreeRoot)
			originalLeft := root.Left
			root.Left = pool.header.BtreeRoot
			err := streamKeyRecordsAt(source, owned, func(key []byte, live bool) error { return nil })
			root.Left = originalLeft
			if err == nil {
				t.Fatal("nested enumeration accepted a repeated source tree node")
			}
			assertEmpty(owned)
			assertEmpty(global)
		})
	}
}

func TestCompactionStreamsMultiplePudgePages(t *testing.T) {
	store, err := NewPudgeShim(t.TempDir(), 4096, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	const records = 777
	for index := 0; index < records; index++ {
		if err := store.Put([]byte(fmt.Sprintf("key-%04d", index)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	for _, deleted := range []int{0, 255, 256, 512, 776} {
		if err := store.Delete([]byte(fmt.Sprintf("key-%04d", deleted))); err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[string]bool)
	if err := forEachLiveKey(store, func(key []byte) error {
		if seen[string(key)] {
			return fmt.Errorf("duplicate paged key %q", key)
		}
		seen[string(key)] = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != records-5 {
		t.Fatalf("paged enumeration returned %d keys, expected %d", len(seen), records-5)
	}
	for index := 0; index < records; index++ {
		key := fmt.Sprintf("key-%04d", index)
		if seen[key] != store.Exists([]byte(key)) {
			t.Fatalf("paged enumeration disagrees with stored key %q", key)
		}
	}
}

func TestCompactionDiskCursorHandlesMutationsOnBothSides(t *testing.T) {
	directory := t.TempDir()
	store := openConcurrentCompactionStore(t, directory, 1, BoltDbCreator)
	var original []string
	expected := make(map[string][]byte)
	for index := 0; index < 8; index++ {
		key := fmt.Sprintf("original-%d", index)
		original = append(original, key)
		expected[key] = []byte("initial")
		if err := store.Put([]byte(key), expected[key]); err != nil {
			t.Fatal(err)
		}
	}
	sort.Slice(original, func(left, right int) bool {
		leftID, rightID := sha256.Sum256([]byte(original[left])), sha256.Sum256([]byte(original[right]))
		return bytes.Compare(leftID[:], rightID[:]) < 0
	})
	pivot := sha256.Sum256([]byte(original[3]))
	var before, after string
	for candidate := 0; before == "" || after == ""; candidate++ {
		key := fmt.Sprintf("inserted-%d", candidate)
		id := sha256.Sum256([]byte(key))
		if bytes.Compare(id[:], pivot[:]) < 0 {
			before = key
		} else {
			after = key
		}
	}
	state, keys, err := store.prepareCompaction(0)
	if err != nil {
		t.Fatal(err)
	}
	defer func(compacting *EnsembleKv) {
		if err := compacting.abortCompaction(0, state); err != nil {
			t.Error(err)
		}
	}(store)
	mutated := false
	err = keys.each(func(key []byte, status compactionKeyState) error {
		if string(key) == original[3] {
			mutated = true
			for _, changed := range []string{before, after, original[0]} {
				if err := store.Put([]byte(changed), []byte("updated during copy")); err != nil {
					return err
				}
				expected[changed] = []byte("updated during copy")
			}
			for _, deleted := range []string{original[1], original[7]} {
				if err := store.Delete([]byte(deleted)); err != nil {
					return err
				}
				delete(expected, deleted)
			}
		}
		return store.copyCompactionKey(0, state, key)
	})
	if err != nil || !mutated {
		t.Fatalf("copy traversal: mutated=%v error=%v", mutated, err)
	}
	if err := store.finishCompaction(0, state); err != nil {
		t.Fatal(err)
	}
	requireConcurrentCompactionContents(t, store, expected, nil)
	if err := store.abortCompaction(0, state); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openConcurrentCompactionStore(t, directory, 1, BoltDbCreator)
	requireConcurrentCompactionContents(t, store, expected, nil)
}

func measureCompactionIndexHeap(t *testing.T, records int) uint64 {
	t.Helper()
	store, err := NewBoltDbShim(filepath.Join(t.TempDir(), "source"), 4096, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.boltHandle.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("Blocks"))
		for index := 0; index < records; index++ {
			key := bytes.Repeat([]byte("k"), 8192)
			binary.BigEndian.PutUint64(key[:8], uint64(index))
			if err := bucket.Put(key, []byte("value")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	index, err := newDiskKeyIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer index.close()
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	stop, sampled := make(chan struct{}), make(chan uint64)
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		peak := baseline.HeapAlloc
		for {
			var current runtime.MemStats
			runtime.ReadMemStats(&current)
			peak = max(peak, current.HeapAlloc)
			select {
			case <-stop:
				sampled <- peak - baseline.HeapAlloc
				return
			case <-ticker.C:
			}
		}
	}()
	err = index.collect(store)
	close(stop)
	peak := <-sampled
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := index.each(func(key []byte, state compactionKeyState) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	if count != records {
		t.Fatalf("collected %d keys, expected %d", count, records)
	}
	return peak
}

func TestCompactionDiskIndexHeapDoesNotScaleWithAllKeys(t *testing.T) {
	small := measureCompactionIndexHeap(t, 128)
	large := measureCompactionIndexHeap(t, 4096)
	t.Logf("key payloads: small=%d large=%d; peak additional heap: small=%d large=%d", 128*8192, 4096*8192, small, large)
	if large > 24<<20 || large > small+16<<20 {
		t.Fatalf("scratch index retained unbounded key data: small peak=%d, large peak=%d", small, large)
	}
}
