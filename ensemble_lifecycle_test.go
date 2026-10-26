package ensemblekv

import (
	"errors"
	bolt "go.etcd.io/bbolt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestEnsembleInitializationFailureClosesOpenedSubstores(t *testing.T) {
	directory := t.TempDir()
	blockedPath := filepath.Join(directory, "0", "1")
	if err := os.MkdirAll(filepath.Dir(blockedPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blockedPath, []byte("not a directory"), 0644); err != nil {
		t.Fatal(err)
	}
	var opened []*BoltDbShim
	creator := func(path string, blockSize, fileSize int64) (KvLike, error) {
		backing, err := NewBoltDbShim(path, blockSize, fileSize)
		if err == nil {
			opened = append(opened, backing)
		}
		return backing, err
	}
	store, err := NewEnsembleKv(directory, 2, 4096, 1000, 1<<20, creator)
	if err == nil || store != nil {
		t.Fatalf("initialization with blocked second substore returned store %v, error %v", store, err)
	}
	if len(opened) != 1 {
		t.Fatalf("opened %d substores before initialization failed; expected 1", len(opened))
	}
	err = opened[0].boltHandle.View(func(transaction *bolt.Tx) error { return nil })
	if !errors.Is(err, bolt.ErrDatabaseNotOpen) {
		t.Fatalf("first substore remained open after initialization failed: %v", err)
	}
}

func TestEnsembleRejectsNonpositiveSubstoreCount(t *testing.T) {
	for _, count := range []int64{0, -1} {
		store, err := NewEnsembleKv(t.TempDir(), count, 4096, 1000, 1<<20, BoltDbCreator)
		if err == nil || store != nil {
			t.Fatalf("initialization with %d substores returned store %v, error %v", count, store, err)
		}
	}
}

func requireCompactionValue(t *testing.T, store KvLike, key, expected string) {
	t.Helper()
	value, err := store.Get([]byte(key))
	if err != nil || string(value) != expected {
		t.Fatalf("Get(%q)=%q, error=%v; expected %q", key, value, err, expected)
	}
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
