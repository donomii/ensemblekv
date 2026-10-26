package ensemblekv

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func TestCompactionKeysLocalBackends(t *testing.T) {
	creators := map[string]CreatorFunc{
		"Extent": ExtentCreator, "SingleFile": SingleFileKVCreator, "Bolt": BoltDbCreator,
		"SQLite": SQLiteCreator, "JSON": JsonKVCreator, "MmapSingle": MmapSingleCreator,
		"MegaPool": MegapoolCreator,
		"Pudge": func(directory string, blockSize, fileSize int64) (KvLike, error) {
			return NewPudgeShim(directory, blockSize, fileSize)
		},
		"Line": func(directory string, blockSize, fileSize int64) (KvLike, error) {
			return LineLSMCreator(directory, blockSize, fileSize, BoltDbCreator)
		},
		"Tree": func(directory string, blockSize, fileSize int64) (KvLike, error) {
			return NewTreeLSM(directory, blockSize, fileSize, 0, BoltDbCreator)
		},
		"Star": func(directory string, blockSize, fileSize int64) (KvLike, error) {
			return NewStarLSM(directory, blockSize, fileSize, BoltDbCreator)
		},
		"Ensemble": func(directory string, blockSize, fileSize int64) (KvLike, error) {
			return NewEnsembleKv(directory, 2, blockSize, 1000, fileSize, BoltDbCreator)
		},
	}
	for name, creator := range creators {
		t.Run(name, func(t *testing.T) {
			store, err := creator(t.TempDir(), 4096, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			for _, key := range []string{"a", "deleted", "updated"} {
				if err := store.Put([]byte(key), []byte("old")); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := store.Delete([]byte("deleted")); err != nil {
				t.Fatal(err)
			}
			if err := store.Put([]byte("updated"), []byte("new")); err != nil {
				t.Fatal(err)
			}
			if err := store.Put([]byte("c"), []byte{}); err != nil {
				t.Fatal(err)
			}
			keys, err := compactionKeys(store)
			if err != nil {
				t.Fatal(err)
			}
			names := make([]string, len(keys))
			for index, key := range keys {
				names[index] = string(key)
			}
			sort.Strings(names)
			if !reflect.DeepEqual(names, []string{"a", "c", "updated"}) {
				t.Fatalf("compaction keys=%q; expected [a c updated]", names)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if name != "JSON" {
				if keys, err := compactionKeys(store); err == nil {
					t.Fatalf("closed store returned compaction keys %q without an error", keys)
				}
			}
		})
	}
}

func TestCompactionKeysRejectsUnsupportedNuDB(t *testing.T) {
	store, err := NewNuDbShim(t.TempDir(), 4096, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := compactionKeys(store); err == nil {
		t.Fatal("NuDB compaction accepted an unavailable enumeration path")
	}
}

func TestCompactionKeysRejectsCorruptMegaPoolNode(t *testing.T) {
	store, err := OpenMegaPool(filepath.Join(t.TempDir(), "pool"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	node := store.nodeAt(store.header.BtreeRoot)
	node.Left = int64(len(store.data))
	if _, err := compactionKeys(store); err == nil {
		t.Fatal("compaction silently pruned an invalid MegaPool node")
	}
}

func TestCompactionKeysRejectsIncompleteExtentIndexes(t *testing.T) {
	store := openExtentRegressionStore(t, t.TempDir())
	defer store.Close()
	putExtentRegressionValue(t, store, "key", "value")
	before, err := os.ReadFile(store.valuesFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.valuesIndex.WriteAt([]byte{0}, 16); err != nil {
		t.Fatal(err)
	}
	if _, err := compactionKeys(store); err == nil {
		t.Fatal("compaction accepted unpaired index bytes")
	}
	after, err := os.ReadFile(store.valuesFile.Name())
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("failed enumeration changed source values: %q, error=%v", after, err)
	}
}

func TestCompactionKeysRejectsPartialManagerSplits(t *testing.T) {
	for _, name := range []string{"Tree", "Star"} {
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			open := func(path string) KvLike {
				t.Helper()
				var store KvLike
				var err error
				if name == "Tree" {
					store, err = NewTreeLSM(path, 4096, 1<<20, 0, BoltDbCreator)
				} else {
					store, err = NewStarLSM(path, 4096, 1<<20, BoltDbCreator)
				}
				if err != nil {
					t.Fatal(err)
				}
				return store
			}
			parent := open(directory)
			if err := parent.Put([]byte("preserved"), []byte("original")); err != nil {
				t.Fatal(err)
			}
			if err := parent.Close(); err != nil {
				t.Fatal(err)
			}
			childName := "0"
			if name == "Tree" {
				childName = "substore_30"
			}
			child := open(filepath.Join(directory, childName))
			if err := child.Close(); err != nil {
				t.Fatal(err)
			}
			parent = open(directory)
			defer parent.Close()
			if keys, err := compactionKeys(parent); err == nil {
				t.Fatalf("partial %s split returned keys %q without an error", name, keys)
			}
			var original KvLike
			if tree, ok := parent.(*TreeLSM); ok {
				original = tree.currentStore
			} else {
				original = parent.(*StarLSM).currentStore
			}
			value, err := original.Get([]byte("preserved"))
			if err != nil || string(value) != "original" {
				t.Fatalf("original parent data after rejected compaction=%q error=%v", value, err)
			}
		})
	}
}
