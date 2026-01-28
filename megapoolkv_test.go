package ensemblekv

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestMegaPool_Basic(t *testing.T) {
	path := "test_mega.db"
	defer os.Remove(path) // Cleanup

	// 1. Create new pool
	pool, err := OpenMegaPool(path, 1024*1024) // 1MB
	if err != nil {
		t.Fatalf("Failed to open pool: %v", err)
	}

	// 2. Wrap Put/Get
	key := []byte("hello")
	val := []byte("world")
	if err := pool.Put(key, val); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := pool.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("Get returned %q, want %q", got, val)
	}

	// 3. Test persistence (Close and Reopen)
	if err := pool.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	pool2, err := OpenMegaPool(path, 0) // Reopen with existing size
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer pool2.Close()

	got2, err := pool2.Get(key)
	if err != nil {
		t.Fatalf("Get after reopen failed: %v", err)
	}
	if !bytes.Equal(got2, val) {
		t.Errorf("Get after reopen returned %q, want %q", got2, val)
	}
}

func TestMegaPool_Update(t *testing.T) {
	path := "test_mega_update.db"
	defer os.Remove(path)

	pool, err := OpenMegaPool(path, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to open pool: %v", err)
	}
	defer pool.Close()

	key := []byte("key1")
	val1 := []byte("value1")
	val2 := []byte("value2_updated")

	if err := pool.Put(key, val1); err != nil {
		t.Fatal(err)
	}

	got1, err := pool.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got1, val1) {
		t.Errorf("First Get: got %q, want %q", got1, val1)
	}

	if err := pool.Put(key, val2); err != nil {
		t.Fatal(err)
	}

	got2, err := pool.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got2, val2) {
		t.Errorf("Second Get: got %q, want %q", got2, val2)
	}
}

func TestMegaPool_MultipleKeys(t *testing.T) {
	path := "test_mega_multi.db"
	defer os.Remove(path)

	pool, err := OpenMegaPool(path, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to open pool: %v", err)
	}
	defer pool.Close()

	keys := []string{"apple", "banana", "cherry", "date"}

	for _, k := range keys {
		if err := pool.Put([]byte(k), []byte("value_"+k)); err != nil {
			t.Fatalf("Put %s failed: %v", k, err)
		}
	}

	for _, k := range keys {
		got, err := pool.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get %s failed: %v", k, err)
		}
		want := []byte("value_" + k)
		if !bytes.Equal(got, want) {
			t.Errorf("Get %s: got %q, want %q", k, got, want)
		}
	}
}

func TestMegaPool_Delete(t *testing.T) {
	path := "test_mega_delete.db"
	defer os.Remove(path)

	pool, err := OpenMegaPool(path, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to open pool: %v", err)
	}
	defer pool.Close()

	keys := []string{"key1", "key2", "key3"}
	for _, k := range keys {
		if err := pool.Put([]byte(k), []byte("val_"+k)); err != nil {
			t.Fatal(err)
		}
	}

	// Delete key2
	if err := pool.Delete([]byte("key2")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify key2 is gone
	if pool.Exists([]byte("key2")) {
		t.Errorf("key2 should not exist")
	}

	// Verify others still exist
	if !pool.Exists([]byte("key1")) {
		t.Errorf("key1 should exist")
	}
	if !pool.Exists([]byte("key3")) {
		t.Errorf("key3 should exist")
	}

	// Verify tree integrity by dumping or getting
	got1, _ := pool.Get([]byte("key1"))
	if string(got1) != "val_key1" {
		t.Errorf("key1 corrupt")
	}
}

func TestMegaPool_AutoResize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "megapool_resize.db")
	initialSize := int64(4096) // Small initial size
	pool, err := OpenMegaPool(path, initialSize)
	if err != nil {
		t.Fatalf("Failed to open pool: %v", err)
	}
	defer pool.Close()

	// Insert data larger than initial size to force resize
	// Initial overhead is ~40 bytes (header).
	// We insert 100 entries of ~100 bytes each > 10KB > 4KB.

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		val := bytes.Repeat([]byte{byte(i)}, 100)
		if err := pool.Put(key, val); err != nil {
			t.Fatalf("Put failed at index %d: %v", i, err)
		}
	}

	// Verify all data exists
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		expectedVal := bytes.Repeat([]byte{byte(i)}, 100)
		gotVal, err := pool.Get(key)
		if err != nil {
			t.Fatalf("Get failed for key %s: %v", key, err)
		}
		if !bytes.Equal(gotVal, expectedVal) {
			t.Errorf("Value mismatch for key %s", key)
		}
	}

	// Verify size increased
	if pool.Size() <= initialSize {
		t.Errorf("Pool size did not increase. Got: %d, Initial: %d", pool.Size(), initialSize)
	}
}
