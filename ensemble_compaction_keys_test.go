package ensemblekv

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
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

func TestCompactionKeysRejectsSkippedSingleFileTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store")
	store, err := NewSingleFileLSM(path, 4096, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("preserved"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(); err != nil {
		t.Fatal(err)
	}
	tableOffset := store.sstables[0].offset
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, tableOffset+24); err != nil {
		t.Fatal(err)
	}
	file.Close()
	store, err = NewSingleFileLSM(path, 4096, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if len(store.sstables) != 0 {
		t.Fatal("fixture did not reproduce a skipped corrupt table")
	}
	var before, after [40]byte
	if _, err := store.file.ReadAt(before[:], 0); err != nil {
		t.Fatal(err)
	}
	if _, err := compactionKeys(store); err == nil {
		t.Fatal("compaction accepted fewer loaded tables than the persisted header")
	}
	if _, err := store.file.ReadAt(after[:], 0); err != nil || before != after {
		t.Fatalf("failed enumeration changed source header: error=%v", err)
	}
}

func TestCompactionKeysRejectsInvalidSingleFileEntries(t *testing.T) {
	for _, corruption := range []string{"key-length", "value-length", "entry-checksum", "entry-count", "key-range"} {
		t.Run(corruption, func(t *testing.T) {
			store, err := NewSingleFileLSM(filepath.Join(t.TempDir(), "store"), 4096, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if err := store.Put([]byte("key"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if err := store.Flush(); err != nil {
				t.Fatal(err)
			}
			meta := &store.sstables[0]
			data := make([]byte, meta.size)
			if _, err := store.file.ReadAt(data, meta.offset); err != nil {
				t.Fatal(err)
			}
			switch corruption {
			case "key-length":
				binary.LittleEndian.PutUint64(data[8:16], 0)
			case "value-length":
				binary.LittleEndian.PutUint64(data[16:24], ^uint64(1))
			case "entry-checksum":
				data[0] ^= 0xff
			case "entry-count":
				meta.numEntries++
			case "key-range":
				meta.maxKey = []byte("wrong")
			}
			meta.checksum = crc32.ChecksumIEEE(data)
			if _, err := store.file.WriteAt(data, meta.offset); err != nil {
				t.Fatal(err)
			}
			if _, err := compactionKeys(store); err == nil {
				t.Fatalf("compaction accepted %s corruption", corruption)
			}
		})
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

func TestEnsembleCompactionReclaimsExtentAndSingleFileData(t *testing.T) {
	for name, creator := range map[string]CreatorFunc{"Extent": ExtentCreator, "SingleFile": SingleFileKVCreator} {
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			store, err := NewEnsembleKv(directory, 1, 4096, 100000, 1<<20, creator)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { store.Close() })
			var expected []byte
			for version := 0; version < 12; version++ {
				expected = bytes.Repeat([]byte{byte(version + 1)}, 4096)
				if err := store.Put([]byte("live"), expected); err != nil {
					t.Fatal(err)
				}
				if err := store.Put([]byte("deleted"), expected); err != nil {
					t.Fatal(err)
				}
				if err := store.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Delete([]byte("deleted")); err != nil {
				t.Fatal(err)
			}
			if err := store.Flush(); err != nil {
				t.Fatal(err)
			}
			source, _ := store.substores.Load(0)
			before := compactionStoredDataSize(t, source)
			if err := store.ConcurrentCompact(); err != nil {
				t.Fatal(err)
			}
			replacement, _ := store.substores.Load(0)
			after := compactionStoredDataSize(t, replacement)
			if after >= before {
				t.Fatalf("compaction did not reclaim %s data: before=%d after=%d", name, before, after)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = NewEnsembleKv(directory, 1, 4096, 100000, 1<<20, creator)
			if err != nil {
				t.Fatal(err)
			}
			value, err := store.Get([]byte("live"))
			if err != nil || !bytes.Equal(value, expected) {
				t.Fatalf("reopened live value length=%d error=%v; expected latest %d bytes", len(value), err, len(expected))
			}
			if store.Exists([]byte("deleted")) {
				t.Fatal("deleted key returned after compaction and reopen")
			}
			t.Logf("%s stored data bytes: before=%d after=%d", name, before, after)
		})
	}
}

func compactionStoredDataSize(t *testing.T, store KvLike) int64 {
	t.Helper()
	var file *os.File
	switch source := store.(type) {
	case *ExtentKeyValStore:
		file = source.valuesFile
	case *SingleFileLSM:
		file = source.file
	default:
		t.Fatalf("unexpected reclamation backend %T", store)
	}
	stat, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	return stat.Size()
}

func TestEnsembleCompactionReclaimsMmapAppendSpace(t *testing.T) {
	directory := t.TempDir()
	store, err := NewEnsembleKv(directory, 1, 4096, 100000, 1<<20, MmapSingleCreator)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	value := bytes.Repeat([]byte("v"), 32*1024)
	for version := 0; version < 31; version++ {
		if err := store.Put([]byte("live"), value); err != nil {
			t.Fatal(err)
		}
	}
	sourceStore, _ := store.substores.Load(0)
	source := sourceStore.(*MmapSingleKV)
	beforeOffset := source.offset
	capacity := source.fileSize
	if capacity-beforeOffset >= int64(headerSize+len("next")+len(value)) {
		t.Fatalf("fixture left enough room for another append: used=%d capacity=%d", beforeOffset, capacity)
	}
	if err := store.ConcurrentCompact(); err != nil {
		t.Fatal(err)
	}
	replacementStore, _ := store.substores.Load(0)
	replacement := replacementStore.(*MmapSingleKV)
	afterOffset := replacement.offset
	if afterOffset >= beforeOffset {
		t.Fatalf("compaction did not reduce used append bytes: before=%d after=%d", beforeOffset, afterOffset)
	}
	if err := store.Put([]byte("next"), value); err != nil {
		t.Fatal(err)
	}
	stat, err := replacement.file.Stat()
	if err != nil || stat.Size() != capacity {
		t.Fatalf("append after compaction changed capacity: stat=%v error=%v expected=%d", stat, err, capacity)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = NewEnsembleKv(directory, 1, 4096, 100000, 1<<20, MmapSingleCreator)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"live", "next"} {
		actual, err := store.Get([]byte(key))
		if err != nil || !bytes.Equal(actual, value) {
			t.Fatalf("reopened key %q length=%d error=%v; expected %d bytes", key, len(actual), err, len(value))
		}
	}
	t.Logf("MmapSingle used append bytes: before=%d after=%d; file capacity=%d", beforeOffset, afterOffset, capacity)
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
