package ensemblekv

import (
	"bytes"
	"testing"
)

func TestMmapSingleFlushAcrossUnalignedMegabyte(t *testing.T) {
	directory := t.TempDir()
	store, err := NewMmapSingleKV(directory, 4096, 2*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close mapped store: %v", err)
		}
	})
	value := bytes.Repeat([]byte("v"), 1024*1024+7)
	if err := store.Put([]byte("large"), value); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(); err != nil {
		t.Fatalf("flush %d written bytes: %v", store.offset, err)
	}
	persisted := make([]byte, len(value))
	if _, err := store.file.ReadAt(persisted, headerSize+int64(len("large"))); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(persisted, value) {
		t.Fatal("flushed file does not contain the complete value")
	}
	reopened, err := NewMmapSingleKV(directory, 4096, 2*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered, err := reopened.Get([]byte("large"))
	if err != nil || !bytes.Equal(recovered, value) {
		t.Fatalf("reopen after flush: expected %d bytes, received %d bytes, error %v", len(value), len(recovered), err)
	}
}
