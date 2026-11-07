package ensemblekv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func singleFileHeaderBytes(t *testing.T, store *SingleFileLSM) []byte {
	t.Helper()
	header := make([]byte, singleFileHeaderSize)
	if _, err := store.file.ReadAt(header, 0); err != nil {
		t.Fatal(err)
	}
	return header
}

func writeSingleFileBytes(t *testing.T, store *SingleFileLSM, data []byte, offset int64) {
	t.Helper()
	if _, err := store.file.WriteAt(data, offset); err != nil {
		t.Fatal(err)
	}
	if err := store.file.Sync(); err != nil {
		t.Fatal(err)
	}
}

func TestSingleFileHeaderPublicationRecovery(t *testing.T) {
	phases := []string{"table-torn", "index-torn", "before-header", "first-torn", "first-durable", "second-torn", "both-durable", "wal-partly-cleared", "wal-cleared"}
	for _, phase := range phases {
		t.Run(phase, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store")
			store := openSingleFileRegressionStore(t, path)
			assertSFPut(t, store, []byte("updated"), []byte("old"), "initial batch")
			assertSFPut(t, store, []byte("deleted"), []byte("old"), "initial batch")
			assertSFFlush(t, store, "initial batch")
			oldHeader := singleFileHeaderBytes(t, store)
			firstSlot := 1 - store.headerSlot
			assertSFPut(t, store, []byte("updated"), []byte("new"), "replacement batch")
			assertSFDelete(t, store, []byte("deleted"), "replacement batch")
			assertSFPut(t, store, []byte("added"), []byte("value"), "replacement batch")
			wal := make([]byte, store.walSize+walEntryHeaderSize)
			if _, err := store.file.ReadAt(wal, store.walOffset); err != nil {
				t.Fatal(err)
			}
			assertSFFlush(t, store, "replacement batch")
			newHeader := singleFileHeaderBytes(t, store)
			writeSingleFileBytes(t, store, oldHeader, 0)
			writeSingleFileBytes(t, store, wal, store.walOffset)
			firstOffset := firstSlot * singleFileHeaderSlotSize
			secondOffset := (1 - firstSlot) * singleFileHeaderSlotSize
			if phase == "first-torn" {
				writeSingleFileBytes(t, store, newHeader[firstOffset:firstOffset+29], int64(firstOffset))
			} else if phase != "before-header" && phase != "table-torn" && phase != "index-torn" {
				writeSingleFileBytes(t, store, newHeader[firstOffset:firstOffset+singleFileHeaderSlotSize], int64(firstOffset))
			}
			if phase == "second-torn" {
				writeSingleFileBytes(t, store, newHeader[secondOffset:secondOffset+43], int64(secondOffset))
			} else if phase == "both-durable" || phase == "wal-partly-cleared" || phase == "wal-cleared" {
				writeSingleFileBytes(t, store, newHeader[secondOffset:secondOffset+singleFileHeaderSlotSize], int64(secondOffset))
			}
			if phase == "wal-partly-cleared" {
				writeSingleFileBytes(t, store, make([]byte, 5), store.walOffset)
			} else if phase == "wal-cleared" {
				writeSingleFileBytes(t, store, make([]byte, walEntryHeaderSize), store.walOffset)
			}
			if phase == "table-torn" || phase == "index-torn" {
				lastTable := store.sstables[len(store.sstables)-1]
				end := lastTable.offset + lastTable.size/2
				if phase == "index-torn" {
					end = store.indexOffset + 5
				}
				if err := store.file.Truncate(end); err != nil {
					t.Fatal(err)
				}
			}
			closeSingleFileWithoutFlush(t, store)
			store = openSingleFileRegressionStore(t, path)
			requireSingleFileValue(t, store, "updated", []byte("new"))
			requireSingleFileValue(t, store, "added", []byte("value"))
			if _, err := store.Get([]byte("deleted")); !errors.Is(err, errKeyNotFound) {
				t.Fatalf("recovered deletion: %v", err)
			}
			assertSFPut(t, store, []byte("after-recovery"), []byte("durable"), "repaired header")
			closeSingleFileWithoutFlush(t, store)
			store = openSingleFileRegressionStore(t, path)
			requireSingleFileValue(t, store, "updated", []byte("new"))
			requireSingleFileValue(t, store, "after-recovery", []byte("durable"))
		})
	}
}

func TestSingleFileHeaderSurvivesEitherSlotDamageAfterWALReuse(t *testing.T) {
	for slot := 0; slot < 2; slot++ {
		for _, damage := range []string{"magic", "generation", "zeroed", "torn", "stale"} {
			t.Run(string(rune('0'+slot))+"/"+damage, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "store")
				store := openSingleFileRegressionStore(t, path)
				assertSFPut(t, store, []byte("kept"), []byte("one"), "first table")
				assertSFFlush(t, store, "first table")
				staleHeader := singleFileHeaderBytes(t, store)
				assertSFPut(t, store, []byte("kept"), []byte("two"), "second table")
				assertSFFlush(t, store, "second table")
				assertSFPut(t, store, []byte("later"), []byte("wal"), "reused WAL")
				offset := int64(slot * singleFileHeaderSlotSize)
				switch damage {
				case "magic":
					writeSingleFileBytes(t, store, []byte{0}, offset)
				case "generation":
					writeSingleFileBytes(t, store, []byte{0xff}, offset+40)
				case "zeroed":
					writeSingleFileBytes(t, store, make([]byte, singleFileHeaderSlotSize), offset)
				case "torn":
					writeSingleFileBytes(t, store, staleHeader[slot*singleFileHeaderSlotSize:slot*singleFileHeaderSlotSize+45], offset)
				case "stale":
					writeSingleFileBytes(t, store, staleHeader[slot*singleFileHeaderSlotSize:(slot+1)*singleFileHeaderSlotSize], offset)
				}
				closeSingleFileWithoutFlush(t, store)
				store = openSingleFileRegressionStore(t, path)
				requireSingleFileValue(t, store, "kept", []byte("two"))
				requireSingleFileValue(t, store, "later", []byte("wal"))
				_, _, repair, err := store.readPublishedHeader()
				if err != nil || repair {
					t.Fatalf("header redundancy was not repaired: repair=%v error=%v", repair, err)
				}
			})
		}
	}
}

func TestSingleFileHeaderTornAtEveryFieldByte(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store")
	store := openSingleFileRegressionStore(t, path)
	assertSFPut(t, store, []byte("updated"), []byte("old"), "first table")
	assertSFFlush(t, store, "first table")
	oldHeader := singleFileHeaderBytes(t, store)
	firstSlot := 1 - store.headerSlot
	assertSFPut(t, store, []byte("updated"), []byte("new"), "next WAL batch")
	wal := make([]byte, store.walSize+walEntryHeaderSize)
	if _, err := store.file.ReadAt(wal, store.walOffset); err != nil {
		t.Fatal(err)
	}
	assertSFFlush(t, store, "second table")
	newHeader := singleFileHeaderBytes(t, store)
	for stage := 0; stage < 2; stage++ {
		for cut := 0; cut <= 52; cut++ {
			writeSingleFileBytes(t, store, oldHeader, 0)
			writeSingleFileBytes(t, store, wal, store.walOffset)
			firstOffset := firstSlot * singleFileHeaderSlotSize
			damagedOffset := firstOffset
			if stage == 1 {
				writeSingleFileBytes(t, store, newHeader[firstOffset:firstOffset+singleFileHeaderSlotSize], int64(firstOffset))
				damagedOffset = (1 - firstSlot) * singleFileHeaderSlotSize
			}
			writeSingleFileBytes(t, store, newHeader[damagedOffset:damagedOffset+cut], int64(damagedOffset))
			closeSingleFileWithoutFlush(t, store)
			var err error
			store, err = NewSingleFileLSM(path, 4096, 1<<20)
			if err != nil {
				t.Fatalf("header publication stage=%d torn byte=%d: %v", stage, cut, err)
			}
			requireSingleFileValue(t, store, "updated", []byte("new"))
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSingleFileHeaderTruncationAndBothInvalid(t *testing.T) {
	for _, size := range []int64{40, 51, 52, singleFileHeaderSlotSize + 17, singleFileHeaderSlotSize + 51} {
		t.Run(string(rune('a'+size%26)), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store")
			store := openSingleFileRegressionStore(t, path)
			if err := store.file.Truncate(size); err != nil {
				t.Fatal(err)
			}
			closeSingleFileWithoutFlush(t, store)
			reopened, err := NewSingleFileLSM(path, 4096, 1<<20)
			if size < 52 {
				if err == nil {
					reopened.Close()
					t.Fatalf("accepted both truncated header slots at file size %d", size)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err := reopened.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestSingleFileLegacyHeaderUpgradePreservesWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store")
	store := openSingleFileRegressionStore(t, path)
	assertSFPut(t, store, []byte("legacy"), []byte("table"), "legacy table")
	assertSFFlush(t, store, "legacy table")
	assertSFPut(t, store, []byte("pending"), []byte("wal"), "legacy WAL")
	header := singleFileHeaderBytes(t, store)
	binary.LittleEndian.PutUint32(header[4:8], 1)
	binary.LittleEndian.PutUint32(header[36:40], crc32.ChecksumIEEE(header[:36]))
	clear(header[40:])
	writeSingleFileBytes(t, store, header, 0)
	closeSingleFileWithoutFlush(t, store)
	store = openSingleFileRegressionStore(t, path)
	requireSingleFileValue(t, store, "legacy", []byte("table"))
	requireSingleFileValue(t, store, "pending", []byte("wal"))
	header = singleFileHeaderBytes(t, store)
	if !bytes.Equal(header[:singleFileHeaderSlotSize], header[singleFileHeaderSlotSize:]) {
		t.Fatal("legacy upgrade did not publish identical redundant slots")
	}
	assertSFDelete(t, store, []byte("legacy"), "upgraded store")
	assertSFFlush(t, store, "upgraded store")
	closeSingleFileWithoutFlush(t, store)
	store = openSingleFileRegressionStore(t, path)
	requireSingleFileValue(t, store, "pending", []byte("wal"))
	if _, err := store.Get([]byte("legacy")); !errors.Is(err, errKeyNotFound) {
		t.Fatalf("legacy key deletion after upgrade: %v", err)
	}
}

func TestSingleFileLegacyUpgradeInterrupted(t *testing.T) {
	for _, phase := range []string{"backup-torn", "backup-durable", "primary-torn"} {
		t.Run(phase, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store")
			store := openSingleFileRegressionStore(t, path)
			assertSFPut(t, store, []byte("pending"), []byte("durable"), "legacy WAL")
			header := singleFileHeaderBytes(t, store)
			versionTwo := bytes.Clone(header)
			binary.LittleEndian.PutUint32(header[4:8], 1)
			binary.LittleEndian.PutUint32(header[36:40], crc32.ChecksumIEEE(header[:36]))
			clear(header[40:])
			writeSingleFileBytes(t, store, header, 0)
			backupSize := singleFileHeaderSlotSize
			if phase == "backup-torn" {
				backupSize = 47
			}
			writeSingleFileBytes(t, store, versionTwo[singleFileHeaderSlotSize:singleFileHeaderSlotSize+backupSize], singleFileHeaderSlotSize)
			if phase == "primary-torn" {
				writeSingleFileBytes(t, store, versionTwo[:45], 0)
			}
			closeSingleFileWithoutFlush(t, store)
			store = openSingleFileRegressionStore(t, path)
			requireSingleFileValue(t, store, "pending", []byte("durable"))
			assertSFPut(t, store, []byte("next"), []byte("value"), "after interrupted upgrade")
			assertSFFlush(t, store, "after interrupted upgrade")
		})
	}
}

func TestSingleFileWALExpansionRepairsStaleHeaderBeforeWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store")
	store := openSingleFileRegressionStore(t, path)
	store.sstableOffset = store.walOffset + 128
	if err := store.syncHeader(); err != nil {
		t.Fatal(err)
	}
	assertSFPut(t, store, []byte("retained"), []byte("table"), "initial table")
	assertSFFlush(t, store, "initial table")
	oldHeader := singleFileHeaderBytes(t, store)
	value := bytes.Repeat([]byte("large"), 100)
	assertSFPut(t, store, []byte("expanded"), value, "expanded WAL")
	if store.walOffset == singleFileHeaderSize {
		t.Fatal("WAL did not move to a new reservation")
	}
	writeSingleFileBytes(t, store, oldHeader[:singleFileHeaderSlotSize], 0)
	closeSingleFileWithoutFlush(t, store)
	store = openSingleFileRegressionStore(t, path)
	requireSingleFileValue(t, store, "retained", []byte("table"))
	requireSingleFileValue(t, store, "expanded", value)
	assertSFPut(t, store, []byte("after-repair"), []byte("value"), "repaired WAL reservation")
	writeSingleFileBytes(t, store, make([]byte, singleFileHeaderSlotSize), singleFileHeaderSlotSize)
	closeSingleFileWithoutFlush(t, store)
	store = openSingleFileRegressionStore(t, path)
	requireSingleFileValue(t, store, "expanded", value)
	requireSingleFileValue(t, store, "after-repair", []byte("value"))
}

func TestSingleFileHeaderPublicationErrorStopsWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store")
	store, err := NewSingleFileLSM(path, 4096, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	assertSFPut(t, store, []byte("retained"), []byte("wal"), "before write failure")
	if err := store.file.Close(); err != nil {
		t.Fatal(err)
	}
	store.file, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.syncHeader(); err == nil {
		t.Fatal("header publication on read-only file succeeded")
	}
	if err := store.Put([]byte("unacknowledged"), []byte("value")); !errors.Is(err, store.writeFailure) {
		t.Fatalf("write after failed publication: %v", err)
	}
	if err := store.Close(); !errors.Is(err, store.writeFailure) {
		t.Fatalf("close after failed publication: %v", err)
	}
	store = openSingleFileRegressionStore(t, path)
	requireSingleFileValue(t, store, "retained", []byte("wal"))
	if _, err := store.Get([]byte("unacknowledged")); !errors.Is(err, errKeyNotFound) {
		t.Fatalf("failed write became visible: %v", err)
	}
}

func TestSingleFileWALResetErrorStopsWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store")
	store, err := NewSingleFileLSM(path, 4096, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	assertSFPut(t, store, []byte("retained"), []byte("value"), "before WAL reset")
	if err := store.file.Close(); err != nil {
		t.Fatal(err)
	}
	store.file, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.clearWAL(); err == nil {
		t.Fatal("WAL invalidation on read-only file succeeded")
	}
	if err := store.Put([]byte("later"), []byte("value")); !errors.Is(err, store.writeFailure) {
		t.Fatalf("write after failed WAL invalidation: %v", err)
	}
	if err := store.Close(); !errors.Is(err, store.writeFailure) {
		t.Fatalf("close after failed WAL invalidation: %v", err)
	}
	store = openSingleFileRegressionStore(t, path)
	requireSingleFileValue(t, store, "retained", []byte("value"))
}
