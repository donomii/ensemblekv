package ensemblekv

import (
	"bytes"
	"errors"
	bolt "go.etcd.io/bbolt"
	"os"
	"path/filepath"
	"testing"
)

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
