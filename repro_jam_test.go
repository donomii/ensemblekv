package ensemblekv

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestMegaPool_SequentialInserts_Performance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "megapool_perf.db")
	pool, err := OpenMegaPool(path, 10*1024*1024)
	if err != nil {
		t.Fatalf("Failed to open pool: %v", err)
	}
	defer pool.Close()

	count := 50000 // Increase to see O(N^2) effect
	// If O(N^2), 5000 inserts:
	// 5000 * 5000 / 2 = 12,500,000 steps.
	// If balanced O(N log N):
	// 5000 * 12 = 60,000 steps.

	start := time.Now()
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key_%06d", i))
		val := []byte("value")
		if err := pool.Put(key, val); err != nil {
			t.Fatalf("Put failed at %d: %v", i, err)
		}
		if i%1000 == 0 {
			fmt.Printf("Inserted %d\n", i)
		}
	}
	duration := time.Since(start)
	t.Logf("Inserted %d keys in %v", count, duration)
}
