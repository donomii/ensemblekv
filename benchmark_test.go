package ensemblekv

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"
)

// BenchConfig holds configuration for benchmark runs
type BenchConfig struct {
	NumItems       int
	MinKeySize     int
	MaxKeySize     int
	MinValueSize   int
	MaxValueSize   int
	ReadWriteRatio float64 // ratio of reads to writes (0.7 = 70% reads)
	BatchSize      int     // number of operations per batch
	Seed           int64   // random seed for reproducibility
}

// Default benchmark configurations
var (
	SmallDataset = &BenchConfig{
		NumItems:       1_000,
		MinKeySize:     16,
		MaxKeySize:     32,
		MinValueSize:   64,
		MaxValueSize:   256,
		ReadWriteRatio: 0.7,
		BatchSize:      100,
		Seed:           42,
	}

	MediumDataset = &BenchConfig{
		NumItems:       100_000,
		MinKeySize:     16,
		MaxKeySize:     64,
		MinValueSize:   256,
		MaxValueSize:   1024,
		ReadWriteRatio: 0.7,
		BatchSize:      1000,
		Seed:           42,
	}

	LargeDataset = &BenchConfig{
		NumItems:       1_000_000,
		MinKeySize:     16,
		MaxKeySize:     128,
		MinValueSize:   1024,
		MaxValueSize:   4096,
		ReadWriteRatio: 0.7,
		BatchSize:      1000,
		Seed:           42,
	}
)

// StoreCreator is a function type that creates a KvLike store
type StoreCreator struct {
	Name    string
	Creator CreatorFunc
}

var testFileCapacity int64 = 12000000000 // 100MB

// Define all our store creators
var StoreCreators = []StoreCreator{
	{"BoltDB", BoltDbCreator},
	{"ExtentKV", ExtentCreator},
	{"ExtentMmapKV", ExtentMmapCreator},
	{"MmapSingle", MmapSingleCreator},
	{"SQLiteKV", SQLiteCreator},
	StoreCreator{"SingleFileKV", SingleFileKVCreator},
	//{"Pudge", PudgeCreator},
	{"JsonKV", JsonKVCreator},
	/*{"EnsembleJsonKV", func(d string, b, c int64) (KvLike, error) {
		return EnsembleCreator(d, b, testFileCapacity, JsonKVCreator)
	}},
	{"TreeLSMJsonKV", func(d string, b, c int64) (KvLike, error) {
		return NewTreeLSM(d, b, testFileCapacity, 0, JsonKVCreator)
	}},
	{"StarLSMJsonKV", func(d string, b, c int64) (KvLike, error) {
		return NewStarLSM(d, b, testFileCapacity, JsonKVCreator)
	}},*/

	{"EnsembleBolt", func(d string, b, c int64) (KvLike, error) {
		return EnsembleCreator(d, b, testFileCapacity, BoltDbCreator)
	}},
	{"EnsembleExtent", func(d string, b, c int64) (KvLike, error) {
		return EnsembleCreator(d, b, testFileCapacity, ExtentCreator)
	}},
	{"EnsembleExtentMmap", func(d string, b, c int64) (KvLike, error) {
		return EnsembleCreator(d, b, testFileCapacity, ExtentMmapCreator)
	}},
	{"EnsembleMmapSingle", func(d string, b, c int64) (KvLike, error) {
		return EnsembleCreator(d, b, testFileCapacity, MmapSingleCreator)
	}},
	{"EnsembleSQLite", func(d string, b, c int64) (KvLike, error) {
		return EnsembleCreator(d, b, testFileCapacity, SQLiteCreator)
	}},
	{"TreeLSMBolt", func(d string, b, c int64) (KvLike, error) {
		return NewTreeLSM(d, b, testFileCapacity, 0, BoltDbCreator)
	}},
	{"TreeLSMExtent", func(d string, b, c int64) (KvLike, error) {
		return NewTreeLSM(d, b, testFileCapacity, 0, ExtentCreator)
	}},
	{"TreeLSMExtentMmap", func(d string, b, c int64) (KvLike, error) {
		return NewTreeLSM(d, b, testFileCapacity, 0, ExtentMmapCreator)
	}},
	{"TreeLSMSQLite", func(d string, b, c int64) (KvLike, error) {
		return NewTreeLSM(d, b, testFileCapacity, 0, SQLiteCreator)
	}},
	{"StarLSMBolt", func(d string, b, c int64) (KvLike, error) {
		return NewStarLSM(d, b, testFileCapacity, BoltDbCreator)
	}},
	{"StarLSMExtent", func(d string, b, c int64) (KvLike, error) {
		return NewStarLSM(d, b, testFileCapacity, ExtentCreator)
	}},
	{"StarLSMExtentMmap", func(d string, b, c int64) (KvLike, error) {
		return NewStarLSM(d, b, testFileCapacity, ExtentMmapCreator)
	}},
	{"StarLSMSQLite", func(d string, b, c int64) (KvLike, error) {
		return NewStarLSM(d, b, testFileCapacity, SQLiteCreator)
	}},
}

// BenchmarkResult holds the results of a benchmark run
type BenchmarkResult struct {
	StoreName    string
	Dataset      string
	Operations   int
	Duration     time.Duration
	OpsPerSecond float64
	Errors       int
}

func (r *BenchmarkResult) String() string {
	return fmt.Sprintf(
		"Store: %-15s Dataset: %-6s Ops: %-8d Duration: %-12s Ops/sec: %-10.2f Errors: %d",
		r.StoreName,
		r.Dataset,
		r.Operations,
		r.Duration.Round(time.Millisecond),
		r.OpsPerSecond,
		r.Errors,
	)
}

// runBenchmark executes a single benchmark configuration
func runBenchmark(b *testing.B, creator StoreCreator, config *BenchConfig) *BenchmarkResult {
	b.Helper()

	// Create temporary directory for the store
	dir := b.TempDir()
	storePath := filepath.Join(dir, "store")

	// Initialize store
	store, err := creator.Creator(storePath, 4096, testFileCapacity) // 4KB block size
	if err != nil {
		fatalf(b, "action=Create store=%s path=%s err=%v", creator.Name, storePath, err)
	}
	defer store.Close()

	// Initialize random number generator
	rand.Seed(config.Seed)

	// Pre-generate test data
	keys := make([][]byte, config.NumItems)
	values := make([][]byte, config.NumItems)
	for i := 0; i < config.NumItems; i++ {
		keys[i] = randomBytes(config.MinKeySize, config.MaxKeySize)
		values[i] = randomBytes(config.MinValueSize, config.MaxValueSize)
	}

	// Populate initial data
	errors := 0
	start := time.Now()

	for i := 0; i < config.NumItems; i++ {
		if err := store.Put(keys[i], values[i]); err != nil {
			errors++
		}
	}

	// Run mixed workload
	operations := 0
	b.ResetTimer()

	for i := 0; i < config.BatchSize; i++ {
		// Determine operation based on read/write ratio
		if rand.Float64() < config.ReadWriteRatio {
			// Read operation
			idx := rand.Intn(config.NumItems)
			_, err := store.Get(keys[idx])
			if err != nil {
				errors++
			}
		} else {
			// Write operation
			idx := rand.Intn(config.NumItems)
			newValue := randomBytes(config.MinValueSize, config.MaxValueSize)
			if err := store.Put(keys[idx], newValue); err != nil {
				errors++
			}
		}
		operations++
	}

	duration := time.Since(start)
	opsPerSecond := float64(operations) / duration.Seconds()

	return &BenchmarkResult{
		StoreName:    creator.Name,
		Dataset:      fmt.Sprintf("%dK", config.NumItems/1000),
		Operations:   operations,
		Duration:     duration,
		OpsPerSecond: opsPerSecond,
		Errors:       errors,
	}
}

// Benchmark functions for different dataset sizes
func BenchmarkStoresSmall(b *testing.B) {
	var results []*BenchmarkResult

	for _, creator := range StoreCreators {
		b.Run(creator.Name, func(b *testing.B) {
			result := runBenchmark(b, creator, SmallDataset)
			results = append(results, result)
		})
	}

	// Print results table
	b.Log("\nSmall Dataset Results (10K items):")
	for _, r := range results {
		b.Log(r)
	}
}

func BenchmarkStoresMedium(b *testing.B) {
	var results []*BenchmarkResult

	for _, creator := range StoreCreators {
		b.Run(creator.Name, func(b *testing.B) {
			result := runBenchmark(b, creator, MediumDataset)
			results = append(results, result)
		})
	}

	b.Log("\nMedium Dataset Results (100K items):")
	for _, r := range results {
		b.Log(r)
	}
}

func BenchmarkStoresLarge(b *testing.B) {
	var results []*BenchmarkResult

	for _, creator := range StoreCreators {
		b.Run(creator.Name, func(b *testing.B) {
			result := runBenchmark(b, creator, LargeDataset)
			results = append(results, result)
		})
	}

	b.Log("\nLarge Dataset Results (1M items):")
	for _, r := range results {
		b.Log(r)
	}
}

// Additional focused benchmarks for specific scenarios

// BenchmarkSequentialWrites tests sequential write performance
func BenchmarkSequentialWrites(b *testing.B) {
	config := &BenchConfig{
		NumItems:     100_000,
		MinKeySize:   16,
		MaxKeySize:   16,
		MinValueSize: 1024,
		MaxValueSize: 1024,
		BatchSize:    100_000,
		Seed:         42,
	}

	var results []*BenchmarkResult

	for _, creator := range StoreCreators {
		b.Run(creator.Name, func(b *testing.B) {
			result := runBenchmark(b, creator, config)
			results = append(results, result)
		})
	}

	b.Log("\nSequential Write Results:")
	for _, r := range results {
		b.Log(r)
	}
}

// BenchmarkRandomReads tests random read performance
func BenchmarkRandomReads(b *testing.B) {
	config := &BenchConfig{
		NumItems:       100_000,
		MinKeySize:     16,
		MaxKeySize:     16,
		MinValueSize:   1024,
		MaxValueSize:   1024,
		ReadWriteRatio: 1.0, // 100% reads
		BatchSize:      100_000,
		Seed:           42,
	}

	var results []*BenchmarkResult

	for _, creator := range StoreCreators {
		b.Run(creator.Name, func(b *testing.B) {
			result := runBenchmark(b, creator, config)
			results = append(results, result)
		})
	}

	b.Log("\nRandom Read Results:")
	for _, r := range results {
		b.Log(r)
	}
}

// BenchmarkHotspotAccess tests performance under hot spot access patterns
func BenchmarkHotspotAccess(b *testing.B) {
	config := &BenchConfig{
		NumItems:       100_000,
		MinKeySize:     16,
		MaxKeySize:     16,
		MinValueSize:   1024,
		MaxValueSize:   1024,
		ReadWriteRatio: 0.8,
		BatchSize:      100_000,
		Seed:           42,
	}

	var results []*BenchmarkResult

	// Modify the benchmark to focus on a small subset of keys
	hotspotKeys := make([][]byte, 100)
	for i := range hotspotKeys {
		hotspotKeys[i] = randomBytes(16, 16)
	}

	for _, creator := range StoreCreators {
		b.Run(creator.Name, func(b *testing.B) {
			dir := b.TempDir()
			store, err := creator.Creator(filepath.Join(dir, "store"), 4096, testFileCapacity)
			if err != nil {
				fatalf(b, "action=Create store=%s path=%s err=%v", creator.Name, filepath.Join(dir, "store"), err)
			}
			defer store.Close()

			// Insert hot spot keys
			for _, key := range hotspotKeys {
				if err := store.Put(key, randomBytes(1024, 1024)); err != nil {
					fatalf(b, "action=Put key=%s valueSize=%d err=%v", trimTo40(key), 1024, err)
				}
			}

			start := time.Now()
			errors := 0
			operations := 0

			// Run hot spot access pattern
			for i := 0; i < config.BatchSize; i++ {
				key := hotspotKeys[rand.Intn(len(hotspotKeys))]
				if rand.Float64() < config.ReadWriteRatio {
					_, err := store.Get(key)
					if err != nil {
						errors++
					}
				} else {
					err := store.Put(key, randomBytes(1024, 1024))
					if err != nil {
						errors++
					}
				}
				operations++
			}

			duration := time.Since(start)
			results = append(results, &BenchmarkResult{
				StoreName:    creator.Name,
				Dataset:      "HotSpot",
				Operations:   operations,
				Duration:     duration,
				OpsPerSecond: float64(operations) / duration.Seconds(),
				Errors:       errors,
			})
		})
	}

	b.Log("\nHot Spot Access Results:")
	for _, r := range results {
		b.Log(r)
	}
}
