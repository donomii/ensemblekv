package ensemblekv

import (
	"bytes"
	"os"
	"testing"
)

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
