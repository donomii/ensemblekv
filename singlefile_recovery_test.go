package ensemblekv

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"path/filepath"
	"testing"
)

func closeSingleFileWithoutFlush(t *testing.T, store *SingleFileLSM) {
	t.Helper()
	if err := store.file.Close(); err != nil {
		t.Fatal(err)
	}
	store.closed = true
}

func requireSingleFileValue(t *testing.T, store *SingleFileLSM, key string, expected []byte) {
	t.Helper()
	value, err := store.Get([]byte(key))
	if err != nil || !bytes.Equal(value, expected) {
		t.Fatalf("key %q: expected %d bytes with checksum %08x, received %d bytes with checksum %08x, error %v", key, len(expected), crc32.ChecksumIEEE(expected), len(value), crc32.ChecksumIEEE(value), err)
	}
}

func TestSingleFileWALReuseDoesNotReplayPreviousBatch(t *testing.T) {
	for _, deletion := range []bool{false, true} {
		name := "update"
		if deletion {
			name = "delete"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store.lsm")
			store := openSingleFileRegressionStore(t, path)
			assertSFPut(t, store, []byte("a"), []byte("1"), "first batch")
			assertSFPut(t, store, []byte("b"), []byte("1"), "first batch")
			assertSFFlush(t, store, "first batch")
			if deletion {
				assertSFDelete(t, store, []byte("b"), "second batch")
			} else {
				assertSFPut(t, store, []byte("b"), []byte("2"), "second batch")
			}
			assertSFFlush(t, store, "second batch")
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store = openSingleFileRegressionStore(t, path)
			requireSingleFileValue(t, store, "a", []byte("1"))
			if deletion {
				assertSFGetFails(t, store, []byte("b"), "reopened deletion")
			} else {
				requireSingleFileValue(t, store, "b", []byte("2"))
			}
		})
	}
}

func TestSingleFileRecoveryAppendsAfterRecoveredWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.lsm")
	store := openSingleFileRegressionStore(t, path)
	assertSFPut(t, store, []byte("first"), []byte("durable"), "before recovery")
	closeSingleFileWithoutFlush(t, store)
	store = openSingleFileRegressionStore(t, path)
	assertSFPut(t, store, []byte("second"), []byte("later"), "after recovery")
	closeSingleFileWithoutFlush(t, store)
	store = openSingleFileRegressionStore(t, path)
	requireSingleFileValue(t, store, "first", []byte("durable"))
	requireSingleFileValue(t, store, "second", []byte("later"))
}

func TestSingleFileRecoveryDoesNotExposeStaleWALTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.lsm")
	store := openSingleFileRegressionStore(t, path)
	assertSFPut(t, store, []byte("a"), []byte("1"), "old WAL")
	assertSFPut(t, store, []byte("b"), []byte("1"), "old WAL")
	oldWAL := make([]byte, store.walSize)
	if _, err := store.file.ReadAt(oldWAL, store.walOffset); err != nil {
		t.Fatal(err)
	}
	assertSFFlush(t, store, "old WAL")
	assertSFPut(t, store, []byte("b"), []byte("2"), "latest table")
	assertSFFlush(t, store, "latest table")
	// Retain an old tail behind the invalid first header, as after interrupted WAL clearing.
	if _, err := store.file.WriteAt(oldWAL[walEntryHeaderSize:], store.walOffset+walEntryHeaderSize); err != nil {
		t.Fatal(err)
	}
	if err := store.file.Sync(); err != nil {
		t.Fatal(err)
	}
	closeSingleFileWithoutFlush(t, store)
	store = openSingleFileRegressionStore(t, path)
	assertSFPut(t, store, []byte("c"), []byte("3"), "reused WAL")
	closeSingleFileWithoutFlush(t, store)
	store = openSingleFileRegressionStore(t, path)
	requireSingleFileValue(t, store, "b", []byte("2"))
	requireSingleFileValue(t, store, "c", []byte("3"))
}

func TestSingleFileFlushPreservesPublishedIndex(t *testing.T) {
	store := openSingleFileRegressionStore(t, filepath.Join(t.TempDir(), "store.lsm"))
	assertSFPut(t, store, []byte("first"), []byte("value"), "first table")
	assertSFFlush(t, store, "first table")
	info, err := store.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	indexOffset := store.indexOffset
	index := make([]byte, info.Size()-indexOffset)
	if _, err := store.file.ReadAt(index, indexOffset); err != nil {
		t.Fatal(err)
	}
	assertSFPut(t, store, []byte("second"), bytes.Repeat([]byte("v"), len(index)), "second table")
	assertSFFlush(t, store, "second table")
	retainedIndex := make([]byte, len(index))
	if _, err := store.file.ReadAt(retainedIndex, indexOffset); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(retainedIndex, index) {
		t.Fatal("new SSTable overwrote the previously published index")
	}
}

func TestSingleFileWALCapacityFlushesTombstones(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.lsm")
	store := openSingleFileRegressionStore(t, path)
	store.sstableOffset = store.walOffset + 128
	if err := store.syncHeader(); err != nil {
		t.Fatal(err)
	}
	assertSFPut(t, store, []byte("retained"), []byte("value"), "first table")
	assertSFFlush(t, store, "first table")
	for range 10 {
		assertSFDelete(t, store, []byte("missing"), "bounded WAL")
	}
	if store.walSize > store.sstableOffset-store.walOffset {
		t.Fatalf("WAL size %d exceeds reserved capacity %d", store.walSize, store.sstableOffset-store.walOffset)
	}
	closeSingleFileWithoutFlush(t, store)
	store = openSingleFileRegressionStore(t, path)
	requireSingleFileValue(t, store, "retained", []byte("value"))
}

func TestSingleFileLargeValuePreservesExistingTables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.lsm")
	store := openSingleFileRegressionStore(t, path)
	assertSFPut(t, store, []byte("retained"), []byte("value"), "first table")
	assertSFFlush(t, store, "first table")
	largeValue := bytes.Repeat([]byte("L"), walMaxSize+1)
	assertSFPut(t, store, []byte("large"), largeValue, "large value")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openSingleFileRegressionStore(t, path)
	requireSingleFileValue(t, store, "retained", []byte("value"))
	requireSingleFileValue(t, store, "large", largeValue)
}

func TestSingleFileWALRejectsEntryOutsideReservedRegion(t *testing.T) {
	store := openSingleFileRegressionStore(t, filepath.Join(t.TempDir(), "store.lsm"))
	offset := store.sstableOffset - walEntryHeaderSize
	header := make([]byte, walEntryHeaderSize)
	binary.LittleEndian.PutUint64(header[0:8], uint64(crc32.ChecksumIEEE([]byte("k"))))
	binary.LittleEndian.PutUint64(header[8:16], 1)
	binary.LittleEndian.PutUint64(header[16:24], 0)
	if _, err := store.file.WriteAt(header, offset); err != nil {
		t.Fatal(err)
	}
	if _, err := store.file.WriteAt([]byte("k"), store.sstableOffset); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.readWALEntry(offset); err == nil {
		t.Fatal("expected WAL entry extending beyond its reserved region to fail")
	}
}

func TestSingleFileWALRejectsEmptyKey(t *testing.T) {
	store := openSingleFileRegressionStore(t, filepath.Join(t.TempDir(), "store.lsm"))
	if err := store.Put(nil, []byte("value")); err == nil {
		t.Fatal("expected empty key rejected because recovery requires a nonempty key")
	}
	if err := store.Delete(nil); err == nil {
		t.Fatal("expected empty deletion key rejected")
	}
}

func TestSingleFileWALDoesNotModifyCallerKeyStorage(t *testing.T) {
	store := openSingleFileRegressionStore(t, filepath.Join(t.TempDir(), "store.lsm"))
	storage := []byte("key-preserved")
	expected := bytes.Clone(storage)
	assertSFPut(t, store, storage[:3], []byte("value"), "borrowed key")
	if !bytes.Equal(storage, expected) {
		t.Fatalf("Put changed caller key storage: expected %q, received %q", expected, storage)
	}
}
