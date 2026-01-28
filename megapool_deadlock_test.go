package ensemblekv

import (
	"path/filepath"
	"testing"
	"time"
)

func TestMegaPool_MapFunc_Modification(t *testing.T) {
	path := filepath.Join(t.TempDir(), "megapool_deadlock.db")
	pool, err := OpenMegaPool(path, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to open pool: %v", err)
	}
	defer pool.Close()

	// 1. Initial Data
	if err := pool.Put([]byte("key1"), []byte("val1")); err != nil {
		t.Fatal(err)
	}
	if err := pool.Put([]byte("key2"), []byte("val2")); err != nil {
		t.Fatal(err)
	}

	// 2. MapFunc with modification
	done := make(chan bool)
	go func() {
		_, err := pool.MapFunc(func(k, v []byte) error {
			// Try to modify
			newKey := []byte(string(k) + "_new")
			if err := pool.Put(newKey, []byte("new_val")); err != nil {
				return err
			}
			if err := pool.Delete(k); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Error(err)
		}
		done <- true
	}()

	select {
	case <-done:
		// Passed
	case <-time.After(2 * time.Second):
		t.Fatal("Deadlock detected in MapFunc modification")
	}

	// verify modification
	if pool.Exists([]byte("key1")) {
		t.Error("key1 should represent deleted")
	}
	if !pool.Exists([]byte("key1_new")) {
		t.Error("key1_new should exist")
	}
}
