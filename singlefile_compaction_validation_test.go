package ensemblekv

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

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
	if _, err := store.keysForCompaction(); err == nil {
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
			if _, err := store.keysForCompaction(); err == nil {
				t.Fatalf("compaction accepted %s corruption", corruption)
			}
		})
	}
}
