package ensemblekv

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"
)

func TestConcurrentCompactCorrectness(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "ensemble_compact_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	s, err := NewEnsembleKv(tempDir, 3, 1024, 1000, 1024*1024, BoltDbCreator)
	if err != nil {
		t.Fatalf("Failed to create ensemble: %v", err)
	}
	defer s.Close()

	// 1. Fill with initial data
	initialKeys := 100
	for i := 0; i < initialKeys; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		val := []byte(fmt.Sprintf("val_%d", i))
		if err := s.Put(key, val); err != nil {
			t.Fatalf("Initial Put failed: %v", err)
		}
	}

	// 2. Start compaction and concurrent writes
	wg := sync.WaitGroup{}
	wg.Add(2)

	// Compaction goroutine
	compactErrChan := make(chan error, 1)
	go func() {
		defer wg.Done()
		if err := s.ConcurrentCompact(); err != nil {
			compactErrChan <- err
		}
	}()

	// Concurrent writes goroutine
	newKeys := 100
	go func() {
		defer wg.Done()
		for i := 0; i < newKeys; i++ {
			key := []byte(fmt.Sprintf("new_key_%d", i))
			val := []byte(fmt.Sprintf("new_val_%d", i))
			if err := s.Put(key, val); err != nil {
				t.Errorf("Concurrent Put failed: %v", err)
			}
			// Overwrite some initial keys
			if i%10 == 0 {
				key := []byte(fmt.Sprintf("key_%d", i))
				val := []byte(fmt.Sprintf("val_%d_updated", i))
				if err := s.Put(key, val); err != nil {
					t.Errorf("Concurrent Update failed: %v", err)
				}
			}
		}
	}()

	wg.Wait()
	select {
	case err := <-compactErrChan:
		t.Fatalf("Compaction failed: %v", err)
	default:
	}

	// 3. Verify data
	for i := 0; i < initialKeys; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		val, err := s.Get(key)
		if err != nil {
			t.Errorf("Missing key %s after compaction", string(key))
			continue
		}
		expected := fmt.Sprintf("val_%d", i)
		if i%10 == 0 {
			expected = fmt.Sprintf("val_%d_updated", i)
		}
		if string(val) != expected {
			t.Errorf("Wrong value for key %s: expected %s, got %s", string(key), expected, string(val))
		}
	}

	for i := 0; i < newKeys; i++ {
		key := []byte(fmt.Sprintf("new_key_%d", i))
		val, err := s.Get(key)
		if err != nil {
			t.Errorf("Missing new key %s after compaction", string(key))
			continue
		}
		expected := fmt.Sprintf("new_val_%d", i)
		if string(val) != expected {
			t.Errorf("Wrong value for new key %s: expected %s, got %s", string(key), expected, string(val))
		}
	}
}

func TestConcurrentCompactSetupCleanup(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "ensemble_cleanup_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create mock stale files
	// gen 0 is default
	genPath := tempDir + "/0"
	os.MkdirAll(genPath+"/0", 0755)

	// Stale compacting
	os.MkdirAll(genPath+"/0.compacting", 0755)
	// Stale old
	os.MkdirAll(genPath+"/1.old", 0755)
	// Primary for 1 also exists
	os.MkdirAll(genPath+"/1", 0755)

	s, err := NewEnsembleKv(tempDir, 3, 1024, 1000, 1024*1024, BoltDbCreator)
	if err != nil {
		t.Fatalf("Failed to create ensemble: %v", err)
	}
	s.Close()

	if _, err := os.Stat(genPath + "/0.compacting"); err == nil {
		t.Errorf("Stale .compacting should have been cleaned up")
	}
	if _, err := os.Stat(genPath + "/1.old"); err == nil {
		t.Errorf("Stale .old should have been cleaned up")
	}
}
