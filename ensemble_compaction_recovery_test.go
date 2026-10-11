package ensemblekv

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func requireCompactionValue(t *testing.T, store KvLike, key, expected string) {
	t.Helper()
	value, err := store.Get([]byte(key))
	if err != nil || string(value) != expected {
		t.Fatalf("Get(%q)=%q, error=%v; expected %q", key, value, err, expected)
	}
}

func TestEnsembleCompactionAbortsSourceReadFailure(t *testing.T) {
	directory := t.TempDir()
	store, err := NewEnsembleKv(directory, 1, 4096, 1000, 1<<20, ExtentCreator)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Put([]byte("preserved"), []byte("original value")); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(); err != nil {
		t.Fatal(err)
	}
	requireCompactionValue(t, store, "preserved", "original value")
	backingStore, _ := store.substores.Load(0)
	source := backingStore.(*ExtentKeyValStore)
	dataPath := source.valuesFile.Name()
	before, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	state, keys, err := store.prepareCompaction(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatalf("prepared %d source keys; expected 1", len(keys))
	}
	source.globalLock.Lock()
	err = source.valuesFile.Close()
	source.globalLock.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	compactErr := store.copyCompactionKey(0, state, keys[0])
	compactErr = errors.Join(compactErr, store.abortCompaction(0, state))
	if !errors.Is(compactErr, os.ErrClosed) {
		t.Errorf("compaction error=%v; expected source read failure wrapping %v", compactErr, os.ErrClosed)
	}
	after, err := os.ReadFile(dataPath)
	if err != nil || !bytes.Equal(after, before) {
		t.Fatalf("source data after failed compaction=%q, error=%v; expected preserved bytes %q", after, err, before)
	}
	active, _ := store.substores.Load(0)
	if active != source {
		t.Fatal("read failure replaced the original source store")
	}
	source.globalLock.Lock()
	source.valuesFile, err = os.OpenFile(dataPath, os.O_RDWR, 0644)
	source.globalLock.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	requireCompactionValue(t, store, "preserved", "original value")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewEnsembleKv(directory, 1, 4096, 1000, 1<<20, ExtentCreator)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	requireCompactionValue(t, reopened, "preserved", "original value")
}

func TestEnsembleCompactionAbortsTargetSyncFailure(t *testing.T) {
	directory := t.TempDir()
	creator := func(path string, blockSize, fileSize int64) (KvLike, error) {
		backing, err := NewMmapSingleKV(path, blockSize, fileSize)
		if err != nil {
			return nil, err
		}
		if strings.HasSuffix(path, ".compacting") {
			if err := backing.file.Close(); err != nil {
				return nil, err
			}
		}
		return backing, nil
	}
	store, err := NewEnsembleKv(directory, 1, 4096, 1000, 1<<20, creator)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("preserved"), []byte("original value")); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(); err != nil {
		t.Fatal(err)
	}
	original, _ := store.substores.Load(0)
	if err := store.ConcurrentCompact(); !errors.Is(err, os.ErrClosed) {
		t.Errorf("compaction error=%v; expected target sync failure wrapping %v", err, os.ErrClosed)
	}
	active, _ := store.substores.Load(0)
	if active != original {
		t.Error("target sync failure promoted the replacement store")
	}
	requireCompactionValue(t, store, "preserved", "original value")
	if err := store.Put([]byte("after-failure"), []byte("still writable")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewEnsembleKv(directory, 1, 4096, 1000, 1<<20, MmapSingleCreator)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	requireCompactionValue(t, reopened, "preserved", "original value")
	requireCompactionValue(t, reopened, "after-failure", "still writable")
}

func TestEnsembleCompactionRejectsClosedSource(t *testing.T) {
	directory := t.TempDir()
	store, err := NewEnsembleKv(directory, 1, 4096, 1000, 1<<20, BoltDbCreator)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("preserved"), []byte("original value")); err != nil {
		t.Fatal(err)
	}
	backing, _ := store.substores.Load(0)
	source := backing.(*BoltDbShim)
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(directory, "0", "0", "bolt.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ConcurrentCompact(); !errors.Is(err, bolt.ErrDatabaseNotOpen) {
		t.Errorf("compaction error=%v; expected closed source error %v", err, bolt.ErrDatabaseNotOpen)
	}
	after, err := os.ReadFile(filepath.Join(directory, "0", "0", "bolt.db"))
	if err != nil || !bytes.Equal(after, before) {
		t.Errorf("failed compaction changed the closed source file: error=%v, before=%d bytes, after=%d bytes", err, len(before), len(after))
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewEnsembleKv(directory, 1, 4096, 1000, 1<<20, BoltDbCreator)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	requireCompactionValue(t, reopened, "preserved", "original value")
}

func TestEnsembleCompactionAbortsTargetCloseFailure(t *testing.T) {
	var target *MmapSingleKV
	var targetMapping []byte
	creator := func(path string, blockSize, fileSize int64) (KvLike, error) {
		backing, err := NewMmapSingleKV(path, blockSize, fileSize)
		if err != nil {
			return nil, err
		}
		if strings.HasSuffix(path, ".compacting") {
			target = backing
			targetMapping = backing.data
			backing.data = backing.data[:len(backing.data)-1]
		}
		return backing, nil
	}
	defer func() {
		if target != nil {
			target.mutex.Lock()
			target.data = targetMapping
			target.mutex.Unlock()
			if err := target.Close(); err != nil {
				t.Errorf("restore and close target mapping after fault injection: %v", err)
			}
		}
	}()
	store, err := NewEnsembleKv(t.TempDir(), 1, 4096, 1000, 1<<20, creator)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Put([]byte("preserved"), []byte("original value")); err != nil {
		t.Fatal(err)
	}
	original, _ := store.substores.Load(0)
	if err := store.ConcurrentCompact(); !errors.Is(err, syscall.EINVAL) {
		t.Errorf("compaction error=%v; expected target unmap failure wrapping %v", err, syscall.EINVAL)
	}
	active, _ := store.substores.Load(0)
	if active != original {
		t.Error("target close failure promoted the replacement store")
	}
	requireCompactionValue(t, store, "preserved", "original value")
	if target == nil {
		t.Fatal("compaction never created the target")
	}
	if err := target.Flush(); err != nil {
		t.Fatalf("target Flush must remain valid for a Close-only failure: %v", err)
	}
}

func writeCompactionFixture(t *testing.T, directory, key, value string) {
	t.Helper()
	store, err := JsonKVCreator(directory, 4096, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte(key), []byte(value)); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEnsembleCompactionStartupRecovery(t *testing.T) {
	testCases := []struct {
		name           string
		primary        bool
		backup         bool
		retired        bool
		incomplete     bool
		corruptPrimary bool
		corruptBackup  bool
		emptyPrimary   bool
		emptyBackup    bool
		legacy         bool
		invalidMarker  bool
		wantError      bool
	}{
		{name: "primary-with-incomplete-copy", primary: true, incomplete: true},
		{name: "backup-without-primary", backup: true},
		{name: "backup-with-incomplete-copy", backup: true, incomplete: true},
		{name: "corrupt-primary-with-backup", backup: true, corruptPrimary: true},
		{name: "valid-primary-with-backup", primary: true, backup: true},
		{name: "empty-primary-with-backup", emptyPrimary: true, backup: true},
		{name: "incomplete-copy-without-source", incomplete: true, wantError: true},
		{name: "committed-primary-with-retired-backup", primary: true, retired: true},
		{name: "retired-backup-without-primary", retired: true, wantError: true},
		{name: "retired-backup-with-corrupt-primary", retired: true, corruptPrimary: true, wantError: true},
		{name: "conflicting-backup-states", primary: true, backup: true, retired: true, wantError: true},
		{name: "corrupt-backup-preserves-valid-primary", primary: true, backup: true, corruptBackup: true, wantError: true},
		{name: "empty-backup-preserves-valid-primary", primary: true, backup: true, emptyBackup: true, wantError: true},
		{name: "legacy-ambiguous-backup", primary: true, backup: true, legacy: true, wantError: true},
		{name: "invalid-protocol-marker", primary: true, invalidMarker: true, wantError: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			directory := t.TempDir()
			primary := filepath.Join(directory, "0", "0")
			if testCase.primary {
				writeCompactionFixture(t, primary, "preserved", "primary value")
			}
			if testCase.emptyPrimary {
				if err := os.MkdirAll(primary, 0755); err != nil {
					t.Fatal(err)
				}
			}
			if testCase.backup {
				if testCase.emptyBackup {
					if err := os.MkdirAll(primary+".old", 0755); err != nil {
						t.Fatal(err)
					}
				} else {
					writeCompactionFixture(t, primary+".old", "preserved", "original value")
				}
				if testCase.corruptBackup {
					if err := os.WriteFile(filepath.Join(primary+".old", "store.json"), []byte("invalid backup JSON"), 0644); err != nil {
						t.Fatal(err)
					}
				}
			}
			if testCase.retired {
				writeCompactionFixture(t, primary+".retired", "preserved", "retired value")
			}
			if testCase.incomplete {
				writeCompactionFixture(t, primary+".compacting", "partial-only", "incomplete copy")
			}
			if testCase.corruptPrimary {
				if err := os.MkdirAll(primary, 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(primary, "store.json"), []byte("not valid JSON"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			if (testCase.backup && !testCase.legacy) || testCase.invalidMarker {
				marker := "ensemble-compaction-v1\n"
				if testCase.invalidMarker {
					marker = "unknown protocol\n"
				}
				if err := os.WriteFile(primary+".compaction", []byte(marker), 0644); err != nil {
					t.Fatal(err)
				}
			}
			store, err := NewEnsembleKv(directory, 1, 4096, 1000, 1<<20, JsonKVCreator)
			if testCase.wantError {
				if err == nil {
					store.Close()
					t.Error("opening an incomplete or conflicting compaction state succeeded")
				}
				for suffix, expected := range map[string]bool{".compacting": testCase.incomplete, ".retired": testCase.retired, ".old": testCase.backup} {
					if expected {
						if _, err := os.Stat(primary + suffix); err != nil {
							t.Errorf("failed recovery removed preserved directory %s: %v", primary+suffix, err)
						}
					}
				}
				if testCase.primary {
					preserved, err := JsonKVCreator(primary, 4096, 1<<20)
					if err != nil {
						t.Fatalf("failed recovery damaged the existing primary: %v", err)
					}
					defer preserved.Close()
					requireCompactionValue(t, preserved, "preserved", "primary value")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			expected := "original value"
			if testCase.primary && !testCase.backup {
				expected = "primary value"
			}
			requireCompactionValue(t, store, "preserved", expected)
			if store.Exists([]byte("partial-only")) {
				t.Error("incomplete replacement contents became authoritative")
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := NewEnsembleKv(directory, 1, 4096, 1000, 1<<20, JsonKVCreator)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			requireCompactionValue(t, reopened, "preserved", expected)
		})
	}
}

func TestEnsembleCompactionDiscardsStaleTargetOnRetry(t *testing.T) {
	directory := t.TempDir()
	store, err := NewEnsembleKv(directory, 1, 4096, 1000, 1<<20, JsonKVCreator)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Put([]byte("preserved"), []byte("original value")); err != nil {
		t.Fatal(err)
	}
	writeCompactionFixture(t, filepath.Join(directory, "0", "0.compacting"), "stale-only", "stale value")
	if err := store.ConcurrentCompact(); err != nil {
		t.Fatal(err)
	}
	requireManagerContents(t, store, map[string]string{"preserved": "original value"})
}

func TestEnsembleCompactionRestoresSourceAfterReplacementOpenFailure(t *testing.T) {
	for name, contents := range map[string]string{"invalid-json": "invalid replacement JSON", "empty-store": "[]"} {
		t.Run(name, func(t *testing.T) { testCompactionReplacementFailure(t, contents) })
	}
}

func testCompactionReplacementFailure(t *testing.T, replacementContents string) {
	directory := t.TempDir()
	injected := false
	creator := func(path string, blockSize, fileSize int64) (KvLike, error) {
		if !injected && filepath.Base(path) == "0" {
			if _, err := os.Stat(path + ".old"); err == nil {
				injected = true
				if err := os.WriteFile(filepath.Join(path, "store.json"), []byte(replacementContents), 0644); err != nil {
					return nil, err
				}
			}
		}
		return JsonKVCreator(path, blockSize, fileSize)
	}
	store, err := NewEnsembleKv(directory, 1, 4096, 1000, 1<<20, creator)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("preserved"), []byte("original value")); err != nil {
		t.Fatal(err)
	}
	if err := store.ConcurrentCompact(); err == nil {
		t.Error("opening the corrupted replacement did not fail compaction")
	}
	if !injected {
		t.Fatal("compaction never reached replacement reopen")
	}
	requireCompactionValue(t, store, "preserved", "original value")
	if err := store.Put([]byte("after-failure"), []byte("still writable")); err != nil {
		t.Fatal(err)
	}
	if err := store.ConcurrentCompact(); err != nil {
		t.Fatalf("retry after failed replacement open: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewEnsembleKv(directory, 1, 4096, 1000, 1<<20, JsonKVCreator)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	requireCompactionValue(t, reopened, "preserved", "original value")
	requireCompactionValue(t, reopened, "after-failure", "still writable")
}

func TestEnsembleCompactionCloseOverlap(t *testing.T) {
	directory := t.TempDir()
	targetEntered := make(chan struct{})
	releaseTarget := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseTarget) }) }
	defer release()
	var createdMu sync.Mutex
	var created []*BoltDbShim
	creator := func(path string, blockSize, fileSize int64) (KvLike, error) {
		if strings.HasSuffix(path, ".compacting") {
			close(targetEntered)
			<-releaseTarget
		}
		backing, err := NewBoltDbShim(path, blockSize, fileSize)
		if err == nil {
			createdMu.Lock()
			created = append(created, backing)
			createdMu.Unlock()
		}
		return backing, err
	}
	store, err := NewEnsembleKv(directory, 1, 4096, 1000, 1<<20, creator)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("preserved"), []byte("original value")); err != nil {
		t.Fatal(err)
	}
	compactDone := make(chan error, 1)
	go func() { compactDone <- store.ConcurrentCompact() }()
	select {
	case <-targetEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("compaction did not reach replacement creation")
	}
	closeStarted := make(chan struct{})
	closeDone := make(chan error, 1)
	go func() {
		close(closeStarted)
		closeDone <- store.Close()
	}()
	<-closeStarted
	release()
	select {
	case err := <-compactDone:
		if err != nil && !strings.Contains(err.Error(), "closed") {
			t.Errorf("compaction overlapping Close returned unrelated error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("compaction did not finish after releasing replacement creation")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Errorf("Close overlapping compaction: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish after compaction")
	}
	createdMu.Lock()
	openedStores := append([]*BoltDbShim(nil), created...)
	createdMu.Unlock()
	for index, backing := range openedStores {
		err := backing.boltHandle.View(func(transaction *bolt.Tx) error { return nil })
		if !errors.Is(err, bolt.ErrDatabaseNotOpen) {
			t.Errorf("created backing store %d remained usable after Close: %v", index, err)
		}
		backing.Close()
	}
	if _, err := store.Get([]byte("preserved")); err == nil {
		t.Error("closed ensemble accepted Get")
	}
	reopened, err := NewEnsembleKv(directory, 1, 4096, 1000, 1<<20, BoltDbCreator)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	requireCompactionValue(t, reopened, "preserved", "original value")
}

func TestEnsembleCompactionRejectsClosedStore(t *testing.T) {
	store, err := NewEnsembleKv(t.TempDir(), 1, 4096, 1000, 1<<20, JsonKVCreator)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("preserved"), []byte("original value")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.ConcurrentCompact(); err == nil {
		t.Error("closed ensemble accepted compaction")
	}
	if err := store.Put([]byte("new"), []byte("value")); err == nil {
		t.Error("closed ensemble accepted Put")
	}
	if err := store.Delete([]byte("preserved")); err == nil {
		t.Error("closed ensemble accepted Delete")
	}
	if err := store.Flush(); err == nil {
		t.Error("closed ensemble accepted Flush")
	}
}

func TestEnsembleCompactionCopiesSplitManagers(t *testing.T) {
	creators := map[string]CreatorFunc{
		"tree": func(directory string, blockSize, fileSize int64) (KvLike, error) {
			return NewTreeLSM(directory, blockSize, fileSize, 0, JsonKVCreator)
		},
		"star": func(directory string, blockSize, fileSize int64) (KvLike, error) {
			return NewStarLSM(directory, blockSize, fileSize, JsonKVCreator)
		},
	}
	for name, creator := range creators {
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			store, err := NewEnsembleKv(directory, 1, 4096, 1000, 100, creator)
			if err != nil {
				t.Fatal(err)
			}
			expected := make(map[string]string)
			for index := 0; index < 90; index++ {
				key := fmt.Sprintf("key-%d", index)
				if err := store.Put([]byte(key), []byte("x")); err != nil {
					t.Fatal(err)
				}
				expected[key] = "x"
			}
			if name == "star" {
				backing, _ := store.substores.Load(0)
				star := backing.(*StarLSM)
				star.mutex.Lock()
				err := star.split(star.hashKey([]byte("key-0")))
				star.mutex.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Put([]byte("key-0"), []byte("updated")); err != nil {
				t.Fatal(err)
			}
			expected["key-0"] = "updated"
			if err := store.Delete([]byte("key-1")); err != nil {
				t.Fatal(err)
			}
			delete(expected, "key-1")
			if err := store.ConcurrentCompact(); err != nil {
				t.Fatal(err)
			}
			requireManagerContents(t, store, expected)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := NewEnsembleKv(directory, 1, 4096, 1000, 100, creator)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			requireManagerContents(t, reopened, expected)
		})
	}
}

func TestEnsembleCompactionPreservesConcurrentNilDeletion(t *testing.T) {
	store, err := NewEnsembleKv(t.TempDir(), 1, 4096, 1000, 1<<20, SingleFileKVCreator)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Put([]byte("deleted"), []byte("original")); err != nil {
		t.Fatal(err)
	}
	state, keys, err := store.prepareCompaction(0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.abortCompaction(0, state)
	if err := store.Put([]byte("deleted"), nil); err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if err := store.copyCompactionKey(0, state, key); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.finishCompaction(0, state); err != nil {
		t.Fatalf("compaction after successful nil deletion: %v", err)
	}
	if store.Exists([]byte("deleted")) {
		t.Fatal("nil deletion was lost during compaction")
	}
}

func TestCompactionRecoveryInventoryNativeLayouts(t *testing.T) {
	creators := map[string]CreatorFunc{
		"json": JsonKVCreator, "bolt": BoltDbCreator, "sqlite": SQLiteCreator, "singlefile": SingleFileKVCreator,
		"mmap": MmapSingleCreator, "mega": MegapoolCreator, "extent": ExtentCreator,
		"pudge": func(path string, blockSize, fileSize int64) (KvLike, error) {
			return NewPudgeShim(path, blockSize, fileSize)
		},
		"line": func(path string, blockSize, fileSize int64) (KvLike, error) {
			return NewLinelsm(path, blockSize, 1000, fileSize, JsonKVCreator)
		},
		"tree": func(path string, blockSize, fileSize int64) (KvLike, error) {
			return NewTreeLSM(path, blockSize, fileSize, 0, JsonKVCreator)
		},
		"star": func(path string, blockSize, fileSize int64) (KvLike, error) {
			return NewStarLSM(path, blockSize, fileSize, JsonKVCreator)
		},
		"ensemble": func(path string, blockSize, fileSize int64) (KvLike, error) {
			return NewEnsembleKv(path, 2, blockSize, 1000, fileSize, JsonKVCreator)
		},
	}
	for name, creator := range creators {
		t.Run(name, func(t *testing.T) {
			path := t.TempDir()
			store, err := creator(path, 4096, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := inspectCompactionRecovery(path); err != nil {
				t.Fatalf("inspect valid empty %s store: %v", name, err)
			}
			store, err = creator(path, 4096, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Put([]byte("preserved"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			inventory, err := inspectCompactionRecovery(path)
			if err != nil {
				entries, _ := os.ReadDir(path)
				var names []string
				for _, entry := range entries {
					names = append(names, entry.Name())
				}
				t.Fatalf("inspect %s storage %v: %v", name, names, err)
			}
			store, err = creator(path, 4096, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if err := validateCompactionRecoveryStore(path, store, inventory); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompactionRecoveryInventoryRejectsMissingFiles(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "backup")
	store, err := NewExtentKeyValueStore(path, 4096, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("preserved"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(path, "values.index")
	if err := os.Rename(missing, filepath.Join(directory, "saved-values.index")); err != nil {
		t.Fatal(err)
	}
	if _, err := inspectCompactionRecovery(path); err == nil {
		t.Fatal("incomplete Extent backup passed pre-open validation")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("pre-open validation recreated the missing index: %v", err)
	}
	empty := t.TempDir()
	if err := os.WriteFile(filepath.Join(empty, "unrelated.txt"), []byte("not a database"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := inspectCompactionRecovery(empty); err == nil {
		t.Fatal("unrelated files passed pre-open validation")
	}
}
