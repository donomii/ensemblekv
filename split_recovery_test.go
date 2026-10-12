package ensemblekv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func openSplitTestStore(t *testing.T, kind, directory string, creator CreatorFunc) KvLike {
	t.Helper()
	store, err := openSplitTestManager(kind, directory, creator)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func openSplitTestManager(kind, directory string, creator CreatorFunc) (KvLike, error) {
	if kind == "tree" {
		return NewTreeLSM(directory, 4096, 1<<20, 0, creator)
	}
	return NewStarLSM(directory, 4096, 1<<20, creator)
}

func splitTestSource(store KvLike) KvLike {
	if tree, ok := store.(*TreeLSM); ok {
		return tree.currentStore
	}
	return store.(*StarLSM).currentStore
}

func splitTestNow(store KvLike) error {
	if tree, ok := store.(*TreeLSM); ok {
		tree.mutex.Lock()
		defer tree.mutex.Unlock()
		return tree.split()
	}
	star := store.(*StarLSM)
	star.mutex.Lock()
	defer star.mutex.Unlock()
	return star.split("")
}

func splitTestIsPublished(store KvLike) bool {
	if tree, ok := store.(*TreeLSM); ok {
		return tree.isReadOnly
	}
	return store.(*StarLSM).isReadOnly
}

func splitTestData() map[string]string {
	values := map[string]string{"empty-value": ""}
	for index := 0; index < 64; index++ {
		key := fmt.Sprintf("split-key-%d", index)
		values[key] = "value-" + key
	}
	return values
}

func persistSplitInterruption(t *testing.T, kind, directory, phase string, creator CreatorFunc, store KvLike) {
	t.Helper()
	layout := splitLayout{Version: 1, Kind: kind}
	path, err := prepareSplit(directory, layout, splitTestSource(store))
	if err != nil {
		t.Fatal(err)
	}
	if phase == "prepared" {
		return
	}
	children := make(map[string]KvLike)
	for partition := 0; partition < 16; partition++ {
		prefix := fmt.Sprintf("%x", partition)
		childPath := filepath.Join(path, splitChildName(kind, "", partition))
		var child KvLike
		if kind == "tree" {
			child, err = newTreeLSM(childPath, 4096, 1<<20, 1, creator, prefix)
		} else {
			child, err = newStarLSM(childPath, 4096, 1<<20, creator, nil, prefix, 1)
		}
		if err != nil {
			t.Fatal(err)
		}
		children[prefix] = child
		if phase == "one-child" {
			break
		}
	}
	defer func() {
		for _, child := range children {
			if err := child.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	if phase == "one-child" || phase == "all-children" {
		return
	}
	copied := 0
	err = forEachLiveKey(splitTestSource(store), func(key []byte) error {
		if phase == "partial-copy" && copied >= 3 {
			return nil
		}
		prefix := fmt.Sprintf("%016x", defaultHashFunc(key))[:1]
		value, err := splitTestSource(store).Get(key)
		if err != nil {
			return err
		}
		if value == nil {
			value = []byte{}
		}
		copied++
		return children[prefix].Put(key, value)
	})
	if err != nil {
		t.Fatal(err)
	}
	if phase == "partial-copy" || phase == "copied" {
		return
	}
	for _, child := range children {
		if err := child.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if phase == "flushed" {
		return
	}
	for prefix, child := range children {
		if err := child.Close(); err != nil {
			t.Fatal(err)
		}
		delete(children, prefix)
	}
	if phase == "closed" {
		return
	}
	if err := syncCompactionTree(path); err != nil {
		t.Fatal(err)
	}
	if phase == "synced" {
		return
	}
	if err := os.Rename(path, filepath.Join(directory, splitCommitted)); err != nil {
		t.Fatal(err)
	}
	if phase == "renamed" {
		return
	}
	if err := syncCompactionPath(directory); err != nil {
		t.Fatal(err)
	}
}

func TestSplitRecoveryAtEveryPublicationPhase(t *testing.T) {
	backends := []struct {
		name    string
		creator CreatorFunc
	}{{"Bolt", BoltDbCreator}, {"Json", JsonKVCreator}, {"Extent", ExtentCreator}, {"SingleFile", SingleFileKVCreator}}
	phases := []string{"prepared", "one-child", "all-children", "partial-copy", "copied", "flushed", "closed", "synced", "renamed", "committed"}
	for _, kind := range []string{"tree", "star"} {
		for _, backend := range backends {
			for _, phase := range phases {
				t.Run(kind+"/"+backend.name+"/"+phase, func(t *testing.T) {
					directory := t.TempDir()
					store := openSplitTestStore(t, kind, directory, backend.creator)
					expected := splitTestData()
					for key, value := range expected {
						if err := store.Put([]byte(key), []byte(value)); err != nil {
							t.Fatal(err)
						}
					}
					persistSplitInterruption(t, kind, directory, phase, backend.creator, store)
					if err := store.Close(); err != nil {
						t.Fatal(err)
					}
					store = openSplitTestStore(t, kind, directory, backend.creator)
					t.Cleanup(func() { store.Close() })
					requireManagerContents(t, store, expected)
					published := phase == "renamed" || phase == "committed"
					if splitTestIsPublished(store) != published {
						t.Fatalf("phase %s: published=%t; expected %t", phase, splitTestIsPublished(store), published)
					}
					for key := range expected {
						if key == "empty-value" {
							continue
						}
						if err := store.Delete([]byte(key)); err != nil {
							t.Fatal(err)
						}
						delete(expected, key)
						break
					}
					if err := store.Put([]byte("after-recovery"), []byte("new")); err != nil {
						t.Fatal(err)
					}
					expected["after-recovery"] = "new"
					if !published {
						if err := splitTestNow(store); err != nil {
							t.Fatal(err)
						}
					}
					if err := store.Close(); err != nil {
						t.Fatal(err)
					}
					store = openSplitTestStore(t, kind, directory, backend.creator)
					requireManagerContents(t, store, expected)
				})
			}
		}
	}
}

func TestSplitRejectsAmbiguousLegacyChildren(t *testing.T) {
	for _, kind := range []string{"tree", "star"} {
		for _, childCount := range []int{1, 16} {
			t.Run(fmt.Sprintf("%s/%d-children", kind, childCount), func(t *testing.T) {
				directory := t.TempDir()
				store := openSplitTestStore(t, kind, directory, BoltDbCreator)
				if err := store.Put([]byte("parent-only"), []byte("preserved")); err != nil {
					t.Fatal(err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				for partition := 0; partition < childCount; partition++ {
					child := openSplitTestStore(t, kind, filepath.Join(directory, splitChildName(kind, "", partition)), BoltDbCreator)
					if err := child.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if reopened, err := openSplitTestManager(kind, directory, BoltDbCreator); err == nil {
					reopened.Close()
					t.Fatal("legacy split without completion evidence was accepted")
				}
				original, err := BoltDbCreator(filepath.Join(directory, "current"), 4096, 1<<20)
				if err != nil {
					t.Fatal(err)
				}
				defer original.Close()
				value, err := original.Get([]byte("parent-only"))
				if err != nil || string(value) != "preserved" {
					t.Fatalf("parent value=%q error=%v; expected preserved", value, err)
				}
			})
		}
	}
}

func TestSplitConcurrentWritesAndRepeatedReopen(t *testing.T) {
	for _, kind := range []string{"tree", "star"} {
		t.Run(kind, func(t *testing.T) {
			directory := t.TempDir()
			store := openSplitTestStore(t, kind, directory, BoltDbCreator)
			defer func() { store.Close() }()
			for index := 0; index < 64; index++ {
				if err := store.Put([]byte(fmt.Sprintf("seed-%d", index)), []byte("before")); err != nil {
					t.Fatal(err)
				}
			}
			var writes sync.WaitGroup
			writes.Add(1)
			go func() {
				defer writes.Done()
				for index := 0; index < 64; index++ {
					key := []byte(fmt.Sprintf("seed-%d", index))
					if index%2 == 0 {
						if err := store.Put(key, []byte("after")); err != nil {
							t.Error(err)
						}
					} else if err := store.Delete(key); err != nil {
						t.Error(err)
					}
				}
			}()
			if err := splitTestNow(store); err != nil {
				t.Fatal(err)
			}
			writes.Wait()
			for pass := 0; pass < 3; pass++ {
				for index := 0; index < 64; index++ {
					value, err := store.Get([]byte(fmt.Sprintf("seed-%d", index)))
					if index%2 == 0 {
						if err != nil || !bytes.Equal(value, []byte("after")) {
							t.Fatalf("pass %d key %d: value=%q error=%v; expected after", pass, index, value, err)
						}
					} else if err == nil {
						t.Fatalf("pass %d key %d: deleted key reappeared as %q", pass, index, value)
					}
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				store = openSplitTestStore(t, kind, directory, BoltDbCreator)
			}
		})
	}
}

func TestSplitCreationFailureKeepsParentAuthoritative(t *testing.T) {
	for _, kind := range []string{"tree", "star"} {
		t.Run(kind, func(t *testing.T) {
			directory := t.TempDir()
			blocked := false
			creator := func(path string, blockSize, fileSize int64) (KvLike, error) {
				store, err := BoltDbCreator(path, blockSize, fileSize)
				if err != nil || blocked || !strings.Contains(path, splitPending) {
					return store, err
				}
				blocked = true
				blocker := filepath.Join(directory, splitPending, splitChildName(kind, "", 1))
				file, createErr := os.OpenFile(blocker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
				if createErr != nil {
					store.Close()
					return nil, createErr
				}
				if err := file.Close(); err != nil {
					store.Close()
					return nil, err
				}
				return store, nil
			}
			store := openSplitTestStore(t, kind, directory, creator)
			defer store.Close()
			expected := splitTestData()
			for key, value := range expected {
				if err := store.Put([]byte(key), []byte(value)); err != nil {
					t.Fatal(err)
				}
			}
			if err := splitTestNow(store); err == nil {
				t.Fatal("split succeeded despite a regular file blocking a child directory")
			}
			if splitTestIsPublished(store) {
				t.Fatal("failed split published incomplete children")
			}
			requireManagerContents(t, store, expected)
			if err := store.Put([]byte("after-failure"), []byte("retained")); err != nil {
				t.Fatal(err)
			}
			expected["after-failure"] = "retained"
			if err := splitTestNow(store); err != nil {
				t.Fatalf("retry after failed child creation: %v", err)
			}
			requireManagerContents(t, store, expected)
		})
	}
}

func TestSplitRecoveryRejectsMissingSourceOrCommittedChild(t *testing.T) {
	for _, kind := range []string{"tree", "star"} {
		for _, target := range []string{"source", "committed-child"} {
			t.Run(kind+"/"+target, func(t *testing.T) {
				directory := t.TempDir()
				store := openSplitTestStore(t, kind, directory, BoltDbCreator)
				if err := store.Put([]byte("retained"), []byte("value")); err != nil {
					t.Fatal(err)
				}
				var missing string
				if target == "source" {
					if _, err := prepareSplit(directory, splitLayout{Version: 1, Kind: kind}, splitTestSource(store)); err != nil {
						t.Fatal(err)
					}
					missing = filepath.Join(directory, "current")
				} else {
					if err := splitTestNow(store); err != nil {
						t.Fatal(err)
					}
					missing = filepath.Join(directory, splitCommitted, splitChildName(kind, "", 0), "current")
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(missing, filepath.Join(directory, "preserved-original")); err != nil {
					t.Fatal(err)
				}
				if opened, err := openSplitTestManager(kind, directory, BoltDbCreator); err == nil {
					opened.Close()
					t.Fatalf("split with missing %s was accepted", target)
				}
				if _, err := os.Stat(missing); !os.IsNotExist(err) {
					t.Fatalf("missing %s was recreated; stat error=%v", target, err)
				}
			})
		}
	}
}

func TestSplitThresholdCountsLogicalBytesAndMakesProgress(t *testing.T) {
	store, err := NewTreeLSM(t.TempDir(), 4096, 100, 0, JsonKVCreator)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	key := bytes.Repeat([]byte("k"), 60)
	for _, size := range []int{60, 80, 10} {
		value := bytes.Repeat([]byte("v"), size)
		if err := store.Put(key, value); err != nil {
			t.Fatal(err)
		}
		if store.isReadOnly {
			t.Fatalf("one key was split for a %d-byte replacement", size)
		}
		if actual, err := store.Get(key); err != nil || !bytes.Equal(actual, value) {
			t.Fatalf("overwrite returned %q, error=%v; expected %q", actual, err, value)
		}
	}
	if needed, err := splitNeeded(store.currentStore, key, []byte("shorter"), 90); err != nil || needed {
		t.Fatalf("shrinking replacement needs split=%t, error=%v", needed, err)
	}
	if needed, err := splitNeeded(store.currentStore, []byte("second-key"), bytes.Repeat([]byte("x"), 40), 90); err != nil || !needed {
		t.Fatalf("second distinct key needs split=%t, error=%v", needed, err)
	}
}

func TestSplitValidationPreservesUnpublishedStateOnSourceError(t *testing.T) {
	for _, kind := range []string{"tree", "star"} {
		t.Run(kind, func(t *testing.T) {
			directory := t.TempDir()
			store := openSplitTestStore(t, kind, directory, ExtentCreator)
			defer store.Close()
			if err := store.Put([]byte("source-key"), []byte("source-value")); err != nil {
				t.Fatal(err)
			}
			path, err := prepareSplit(directory, splitLayout{Version: 1, Kind: kind}, splitTestSource(store))
			if err != nil {
				t.Fatal(err)
			}
			sentinel := filepath.Join(path, "unpublished-data")
			if err := os.WriteFile(sentinel, []byte("retained"), 0600); err != nil {
				t.Fatal(err)
			}
			source := splitTestSource(store).(*ExtentKeyValStore)
			stat, err := source.valuesIndex.Stat()
			if err != nil {
				t.Fatal(err)
			}
			var invalid [8]byte
			binary.BigEndian.PutUint64(invalid[:], 1<<30)
			if _, err := source.valuesIndex.WriteAt(invalid[:], stat.Size()-8); err != nil {
				t.Fatal(err)
			}
			if err := splitTestNow(store); err == nil {
				t.Fatal("split accepted an index pointing beyond the source values file")
			}
			if data, err := os.ReadFile(sentinel); err != nil || string(data) != "retained" {
				t.Fatalf("source validation removed pending state: data=%q error=%v", data, err)
			}
			if splitTestIsPublished(store) {
				t.Fatal("invalid source was published")
			}
		})
	}
}

func TestSplitMissingCommittedChildDoesNotReadStaleParent(t *testing.T) {
	for _, kind := range []string{"tree", "star"} {
		t.Run(kind, func(t *testing.T) {
			store := openSplitTestStore(t, kind, t.TempDir(), BoltDbCreator)
			defer store.Close()
			key := []byte("historical-parent-value")
			if err := store.Put(key, []byte("old")); err != nil {
				t.Fatal(err)
			}
			if err := splitTestNow(store); err != nil {
				t.Fatal(err)
			}
			prefix := fmt.Sprintf("%016x", defaultHashFunc(key))[:1]
			if tree, ok := store.(*TreeLSM); ok {
				if err := tree.subStores[prefix].Close(); err != nil {
					t.Fatal(err)
				}
				delete(tree.subStores, prefix)
			} else {
				star := store.(*StarLSM)
				if err := star.subStores[prefix].Close(); err != nil {
					t.Fatal(err)
				}
				delete(star.subStores, prefix)
			}
			if value, err := store.Get(key); err == nil || store.Exists(key) {
				t.Fatalf("missing child exposed historical value=%q error=%v", value, err)
			}
		})
	}
}
