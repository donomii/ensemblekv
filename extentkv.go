package ensemblekv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"internal/diff"
	"io"
	"os"
	"sync"

	"github.com/donomii/goof"
)

var EnableIndexCaching bool = false // Feature flag for index caching

/*
=======================================================================
EXTENT KEY-VALUE STORE: FORMAT & OPERATION OVERVIEW
=======================================================================

The ExtentKeyValStore is an append-only, file-based key–value store designed
for high-throughput writes and efficient lookups. It separates data storage
from index management and leverages optional in-memory caching to speed up
accesses to index information. This document describes the file formats,
data layouts, caching mechanisms, and overall operational workflow for the
store, serving as a guide for future maintainers.

-----------------------------------------------------------------------
File Organization & Data Layout
-----------------------------------------------------------------------

There are four primary files managed by the store, all created in a given
directory:

1. **Data Files**
   - **keys.dat**: Holds the raw bytes of all keys, appended sequentially.
   - **values.dat**: Holds the raw bytes of all values, appended sequentially.

   *How Data Files Work:*
   - Every `Put()` operation appends the key to *keys.dat* and the
     corresponding value to *values.dat*.
   - The actual position (byte offset) where a key or value is written is
     determined by the current end-of-file pointer.

2. **Index Files**
   - **keys.index**: Maintains a sequence of 8-byte (int64) BigEndian numbers
     that indicate cumulative end offsets in *keys.dat*.
   - **values.index**: Maintains a similar sequence for *values.dat*.

   *Index File Format & Purpose:*
   - **Initialization:**
     - When first created, an index file is seeded with a single value
       `0`, representing the initial offset.
   - **Record Boundaries:**
     - Each time data is appended, the store writes a new cumulative
       offset into the corresponding index file.
     - The length of a given record is computed by subtracting the previous
       index entry from the current one. For example, if a key starts at byte
       offset 10 and the next index entry is 15, the key's length is 5 bytes.
   - **Tombstones (Deletions):**
     - Deletions are handled logically by marking records as “deleted”.
     - A deletion is recorded by writing a negative offset into the index file.
     - When reading an index entry, a negative value indicates a tombstone,
       and its absolute value is used to locate the original data in the data file.

-----------------------------------------------------------------------
Caching Mechanism & In-Memory Structures
-----------------------------------------------------------------------

To reduce disk I/O and improve performance, the store incorporates two levels
of caching:

1. **Index Caching:**
   - Controlled by the global flag `EnableIndexCaching`.
   - When enabled, the entire contents of *keys.index* and *values.index*
     are read into memory (stored in `keysIndexCache` and `valuesIndexCache`).
   - Helper functions (e.g., `readIndexAt`) use the in-memory cache if available,
     falling back to file I/O if caching is disabled or not yet loaded.
   - The caches are flushed (set to `nil`) on write operations (e.g., `Put()`
     or `Delete()`) to ensure consistency.

2. **Key Existence Map:**
   - An in-memory map (`map[string]bool`) is maintained to track the presence
     of keys.
   - A key mapped to `true` indicates that it exists; `false` indicates that it
     has been deleted (a tombstone).
   - If a key is not present, then the status is unknown and the disk is queried.
   - This map is built via `loadKeyCache()`, which scans the key index and data
     files using a lock-free mapping function (`LockFreeMapFunc`).
   - This cache allows for a fast existence check in methods such as `Exists()`
     and also aids in the `Get()` operation.

3. **Cache Statistics:**
   - The store tracks metrics including cache hits, misses, flushes, and loads.
   - The helper `maybePrintCacheStats()` increments a request counter and prints
     these statistics periodically, assisting maintainers in monitoring cache
     performance and diagnosing issues.

-----------------------------------------------------------------------
Operational Workflow
-----------------------------------------------------------------------

1. **Initialization (`NewExtentKeyValueStore`):**
   - The store creates or opens the four files. If the index files do not exist,
     they are created and initialized with a `0`.
   - It validates index integrity by reading the last index entry and comparing
     it to the actual size of the corresponding data file. If mismatches are
     detected, the index file is truncated to the expected size.
   - Finally, the key existence map is loaded into memory via `loadKeyCache()`,
     optionally using the in-memory index cache if enabled.

2. **Insertion/Update (`Put()`):**
   - Acquires a global lock to ensure thread safety.
   - **Writing Data:**
     - Seeks to the end of *keys.dat* and appends the key bytes.
     - Immediately writes the new cumulative offset (key end position) to
       *keys.index*.
     - Repeats the process for the value using *values.dat* and *values.index*.
   - **Cache Management:**
     - Flushes (invalidates) the index caches to ensure subsequent reads
       reload fresh data.
     - Updates the in-memory key map by marking the inserted key as existing.
   - **Monitoring:**
     - Increments the cache flush counter.

3. **Retrieval (`Get()` & `LockFreeGet()`):**
   - Acquires the global lock and invokes `maybePrintCacheStats()` to update
     cache metrics.
   - **Cache Check:**
     - Checks the in-memory key map for the existence of the requested key.
     - Updates hit/miss counters accordingly.
   - **Disk Lookup:**
     - Despite a positive cache hit, the actual value is always read from disk.
     - `LockFreeGet()` resets file pointers, ensures the index caches are loaded,
       and then searches the *keys.index* from the end backward (to get the latest
       update) for a matching key.
     - When found (and if not marked as deleted), it retrieves the corresponding
       value from *values.dat* using the matching offsets from *values.index*.

4. **Deletion (`Delete()`):**
   - Acquires the global lock and flushes the index caches.
   - Marks the key as deleted by:
     - Writing a tombstone (negative offset) into the index files.
     - Updating the in-memory key map (setting the key’s value to `false`).
   - The underlying data is not removed from the data files; it remains as part
     of the append-only log, but subsequent lookups will consider the key deleted.

5. **Index Dumping & Listing:**
   - **DumpIndex():** Iterates through the index files, printing each entry’s
     byte position. Useful for debugging and verifying the integrity of index files.
   - **List() & MapFunc():** Provide mechanisms to scan and retrieve a list of
     keys or perform operations on all key–value pairs by iterating from the end
     of the index files backward.

6. **Flush & Close:**
   - **Flush():** Explicitly synchronizes all open file descriptors to disk,
     ensuring that all writes are durable.
   - **Close():** Gracefully closes all files, releasing the global lock to
     ensure no operations are mid-flight.

-----------------------------------------------------------------------
Key Design Considerations & Benefits
-----------------------------------------------------------------------

- **Append-Only Log Structure:**
  - Simplifies writes and enhances durability since data is always appended.
  - Simplifies recovery and consistency checks via the index files.

- **Index Files & Record Boundaries:**
  - The use of cumulative offsets stored in index files allows for simple
    computation of record lengths without embedding size headers in the data.

- **Tombstone-Based Deletion:**
  - Enables logical deletion without expensive file rewrites, preserving the
    sequential, append-only nature of the data files.

- **In-Memory Caching:**
  - Reduces disk I/O by caching the index files and maintaining a key–existence
    map.
  - Provides immediate feedback on cache efficiency through built-in counters,
    helping identify potential performance bottlenecks.

- **Concurrency & Locking:**
  - A single global lock (`globalLock`) ensures that operations modifying the
    store are thread-safe, while the “lock-free” read functions assume that
    file positions are reset appropriately on each call.

=======================================================================
*/

// --- Add these new fields to ExtentKeyValStore struct:
type ExtentKeyValStore struct {
	DefaultOps
	keysFile    *os.File
	valuesFile  *os.File
	keysIndex   *os.File
	valuesIndex *os.File
	blockSize   int
	globalLock  sync.Mutex
	cache       map[string]bool

	// New fields for index caching
	keysIndexCache   []byte
	valuesIndexCache []byte

	// New fields for monitoring cache efficiency
	cacheHits    int
	cacheMisses  int
	cacheWrites  int
	cacheLoads   int
	requestCount int
}

// --- Add this helper method to increment the request counter and print stats every 100 requests.
func (s *ExtentKeyValStore) maybePrintCacheStats() {
	s.requestCount++
	if s.requestCount%100 == 0 {
		fmt.Printf("Cache stats: hits=%d, misses=%d, writes=%d, loads=%d\n",
			s.cacheHits, s.cacheMisses, s.cacheWrites, s.cacheLoads)
	}
}

func NewExtentKeyValueStore(directory string, blockSize int) (*ExtentKeyValStore, error) {
	fmt.Println("Creating new ExtentKeyValueStore with enhanced caching at directory", directory)
	os.MkdirAll(directory, 0755)
	keysFilePath := directory + "/keys.dat"
	valuesFilePath := directory + "/values.dat"
	keysIndexFilePath := directory + "/keys.index"
	valuesIndexFilePath := directory + "/values.index"

	if !goof.Exists(keysIndexFilePath) {
		//Write a single 0 to the keys index file
		keysIndex, err := os.OpenFile(keysIndexFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			panic(err)
		}
		err = binary.Write(keysIndex, binary.BigEndian, int64(0))
		if err != nil {
			panic(err)
		}
		keysIndex.Close()
	}

	if !goof.Exists(valuesIndexFilePath) {
		//Write a single 0 to the values index file
		valuesIndex, err := os.OpenFile(valuesIndexFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			panic(err)
		}
		err = binary.Write(valuesIndex, binary.BigEndian, int64(0))
		if err != nil {
			panic(err)
		}
		valuesIndex.Close()
	}

	keysFile, err := os.OpenFile(keysFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}

	keysIndex, err := os.OpenFile(keysIndexFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}

	//Read the last index item to get the end of the file.  If this is different to the file size, then the file is corrupt and we should truncate it to size
	_, err = keysIndex.Seek(-8, 2)
	if err != nil {
		panic(err)
	}
	var keysFileEnd int64
	err = binary.Read(keysIndex, binary.BigEndian, &keysFileEnd)
	if err != nil {
		panic(err)
	}
	if keysFileEnd < 0 {
		keysFileEnd = -keysFileEnd
	}

	stat, _ := keysFile.Stat()
	if keysFileEnd != stat.Size() {
		// Get the difference between the expected size and the actual size
		diff := keysFileEnd - stat.Size()
		if diff < 0 {
			diff = -diff
		}
		if diff > 8 {
			fmt.Printf("Difference between recorded end of file and actual end of file is too large: %d, refusing to truncate, cannot recover from corrupted file\n", diff)
			os.Exit(1)
		}
		fmt.Printf("Truncating keys index file %s to %d\n", keysIndexFilePath, keysFileEnd)
		keysIndex.Truncate(keysFileEnd)
	}

	valuesIndex, err := os.OpenFile(valuesIndexFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}

	valuesFile, err := os.OpenFile(valuesFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}

	//Read the last index item to get the end of the file.  If this is different to the file size, then the file is corrupt and we should truncate it to size
	_, err = valuesIndex.Seek(-8, 2)
	if err != nil {
		panic(err)
	}
	var valuesFileEnd int64
	err = binary.Read(valuesIndex, binary.BigEndian, &valuesFileEnd)
	if err != nil {
		panic(err)
	}
	if valuesFileEnd < 0 {
		valuesFileEnd = -valuesFileEnd
	}
	stat, _ = valuesFile.Stat()
	if valuesFileEnd != stat.Size() {
		// Get the difference between the expected size and the actual size
		diff := valuesFileEnd - stat.Size()
		if diff < 0 {
			diff = -diff
		}
		if diff > 8 {
			fmt.Printf("Difference between recorded end of file and actual end of file is too large: %d, refusing to truncate, cannot recover from corrupted file\n", diff)
			os.Exit(1)
		}
		fmt.Printf("Truncating values index file %s to %d\n", valuesIndexFilePath, valuesFileEnd)
		valuesIndex.Truncate(valuesFileEnd)
	}

	s := &ExtentKeyValStore{
		keysFile:    keysFile,
		valuesFile:  valuesFile,
		keysIndex:   keysIndex,
		valuesIndex: valuesIndex,
		blockSize:   blockSize,
	}

	if EnableIndexCaching {
		s.cache = make(map[string]bool)
		s.loadKeyCache()
	}

	return s, nil
}

func (s *ExtentKeyValStore) loadKeysIndexCache() error {
	if !EnableIndexCaching {
		return nil
	}

	if s.keysIndexCache != nil {
		return nil // Cache already loaded
	}

	// Get file size
	stat, err := s.keysIndex.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat keys index: %w", err)
	}

	// Read entire index file
	s.keysIndexCache = make([]byte, stat.Size())
	_, err = s.keysIndex.ReadAt(s.keysIndexCache, 0)
	if err != nil {
		s.keysIndexCache = nil // Clear cache on error
		return fmt.Errorf("failed to read keys index: %w", err)
	}

	return nil
}

func (s *ExtentKeyValStore) loadValuesIndexCache() error {
	if !EnableIndexCaching {
		return nil
	}

	if s.valuesIndexCache != nil {
		return nil // Cache already loaded
	}

	// Get file size
	stat, err := s.valuesIndex.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat values index: %w", err)
	}

	// Read entire index file
	s.valuesIndexCache = make([]byte, stat.Size())
	_, err = s.valuesIndex.ReadAt(s.valuesIndexCache, 0)
	if err != nil {
		s.valuesIndexCache = nil // Clear cache on error
		return fmt.Errorf("failed to read values index: %w", err)
	}

	return nil
}

// loadKeyCache loads the entire keys.index and keys.dat files into memory,
// processing records in forward order. This routine should only be called
// when EnableIndexCaching is true.
func (s *ExtentKeyValStore) loadKeyCache() error {
	// This routine should only be called when caching is enabled.
	if !EnableIndexCaching {
		return fmt.Errorf("loadKeyCache should not be called when caching is disabled")
	}

	// Reinitialize the in‑memory cache.
	s.cache = make(map[string]bool)

	// Ensure the keys.index cache is loaded.
	if s.keysIndexCache == nil {
		if err := s.loadKeysIndexCache(); err != nil {
			return fmt.Errorf("loadKeyCache: failed to load keys index cache: %w", err)
		}
	}
	keyIndexData := s.keysIndexCache

	// Ensure the keys.index file length is a multiple of 8 bytes.
	if len(keyIndexData)%8 != 0 {
		return fmt.Errorf("loadKeyCache: keys index file corrupt: length (%d) is not a multiple of 8", len(keyIndexData))
	}

	// Parse the entire keys.index into an array of int64 offsets.
	numOffsets := len(keyIndexData) / 8
	if numOffsets < 1 {
		return fmt.Errorf("loadKeyCache: keys index file empty")
	}
	offsets := make([]int64, numOffsets)
	buf := bytes.NewReader(keyIndexData)
	for i := 0; i < numOffsets; i++ {
		var off int64
		if err := binary.Read(buf, binary.BigEndian, &off); err != nil {
			return fmt.Errorf("loadKeyCache: failed to read offset %d: %w", i, err)
		}
		offsets[i] = off
	}

	// Read the entire keys.dat file into memory.
	if _, err := s.keysFile.Seek(0, 0); err != nil {
		return fmt.Errorf("loadKeyCache: failed to seek keys file: %w", err)
	}
	keysData, err := io.ReadAll(s.keysFile)
	if err != nil {
		return fmt.Errorf("loadKeyCache: failed to read keys file: %w", err)
	}

	// Use a variable 'prev' to always hold the positive start offset.
	prev := offsets[0] // This should be 0.
	for i := 1; i < numOffsets; i++ {
		cur := offsets[i]
		deleted := false
		if cur < 0 {
			deleted = true
			cur = -cur // Use the absolute value for this record's end.
		}
		if cur > int64(len(keysData)) {
			return fmt.Errorf("loadKeyCache: keys index entry %d (%d) exceeds keys file length (%d)", i, cur, len(keysData))
		}
		// Use the positive 'prev' offset as the start.
		keyBytes := keysData[prev:cur]
		// Later records override earlier ones.
		s.cache[string(keyBytes)] = !deleted

		// Always update 'prev' to the current absolute end.
		prev = cur
	}

	s.cacheLoads++ // update the load counter
	return nil
}

// forwardScanForKey performs a sequential (forward) scan of the keys.index and
// values.index files on disk. For every record it reads from the keys file, it
// checks whether the key matches the requested key. Each record in the index files
// is represented by two int64 values (one from keys.index and one from values.index),
// where the absolute value indicates the end of the record (the first record’s start
// is the first offset in the file, which is expected to be 0). A negative offset indicates
// that the record is a deletion (a tombstone).
//
// As the file is scanned forward, any matching record for the given key is “remembered”;
// later records overwrite earlier ones. At the end of the scan, if a matching record was found,
// its deletion flag and value boundaries are used to either report that the key is deleted
// or to load its corresponding value from values.dat.
func (s *ExtentKeyValStore) forwardScanForKey(key []byte) ([]byte, error) {
	// Reset file positions for a fresh sequential scan.
	if _, err := s.keysIndex.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("forwardScanForKey: seek keys index: %w", err)
	}
	if _, err := s.valuesIndex.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("forwardScanForKey: seek values index: %w", err)
	}
	if _, err := s.keysFile.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("forwardScanForKey: seek keys file: %w", err)
	}
	if _, err := s.valuesFile.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("forwardScanForKey: seek values file: %w", err)
	}

	// Read the first (initial) offset from keys.index and values.index.
	// This should be 0.
	var prevKeyOffset int64
	if err := binary.Read(s.keysIndex, binary.BigEndian, &prevKeyOffset); err != nil {
		return nil, fmt.Errorf("forwardScanForKey: reading initial keys index offset: %w", err)
	}
	var prevValueOffset int64
	if err := binary.Read(s.valuesIndex, binary.BigEndian, &prevValueOffset); err != nil {
		return nil, fmt.Errorf("forwardScanForKey: reading initial values index offset: %w", err)
	}

	// These variables will track the most recent matching record.
	var found bool
	var latestDeleted bool
	var latestValueStart, latestValueEnd int64

	// Now, loop over each record in the index files.
	// Each iteration reads the next key index entry (8 bytes) and its corresponding value index entry.
	for {
		// Read the next key offset.
		var curKeyOffset int64
		if err := binary.Read(s.keysIndex, binary.BigEndian, &curKeyOffset); err == io.EOF {
			// End of file reached.
			break
		} else if err != nil {
			return nil, fmt.Errorf("forwardScanForKey: reading keys index: %w", err)
		}

		// Read the next value offset.
		var curValueOffset int64
		if err := binary.Read(s.valuesIndex, binary.BigEndian, &curValueOffset); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("forwardScanForKey: reading values index: %w", err)
		}

		// Determine whether this key record represents a deletion.
		deleted := false
		effectiveKeyOffset := curKeyOffset
		if curKeyOffset < 0 {
			deleted = true
			effectiveKeyOffset = -curKeyOffset
		}

		// Compute the length of the key record in keys.dat.
		keyLen := effectiveKeyOffset - prevKeyOffset
		if keyLen < 0 {
			return nil, fmt.Errorf("forwardScanForKey: invalid key length computed")
		}
		keyBuf := make([]byte, keyLen)
		// Seek to the beginning of the current key record.
		if _, err := s.keysFile.Seek(prevKeyOffset, io.SeekStart); err != nil {
			return nil, fmt.Errorf("forwardScanForKey: seeking keys file: %w", err)
		}
		if _, err := io.ReadFull(s.keysFile, keyBuf); err != nil {
			return nil, fmt.Errorf("forwardScanForKey: reading key record: %w", err)
		}

		// If the key matches the one we're looking for, update our “latest record” info.
		if bytes.Equal(key, keyBuf) {
			found = true
			latestDeleted = deleted
			// For the corresponding value record, the boundaries come from values.index.
			effectiveValueOffset := curValueOffset
			if curValueOffset < 0 {
				effectiveValueOffset = -curValueOffset
			}
			latestValueStart = prevValueOffset
			latestValueEnd = effectiveValueOffset
		}

		// Update the previous offsets for the next iteration.
		if curKeyOffset < 0 {
			prevKeyOffset = -curKeyOffset
		} else {
			prevKeyOffset = curKeyOffset
		}
		if curValueOffset < 0 {
			prevValueOffset = -curValueOffset
		} else {
			prevValueOffset = curValueOffset
		}
	}

	if !found {
		return nil, fmt.Errorf("forwardScanForKey: key not found")
	}
	if latestDeleted {
		return nil, fmt.Errorf("forwardScanForKey: key has been deleted")
	}

	// With the most recent (non-deleted) record determined, read the corresponding value from values.dat.
	valueLen := latestValueEnd - latestValueStart
	if valueLen < 0 {
		return nil, fmt.Errorf("forwardScanForKey: invalid value length computed")
	}
	valueBuf := make([]byte, valueLen)
	if _, err := s.valuesFile.Seek(latestValueStart, io.SeekStart); err != nil {
		return nil, fmt.Errorf("forwardScanForKey: seeking values file: %w", err)
	}
	if _, err := io.ReadFull(s.valuesFile, valueBuf); err != nil {
		return nil, fmt.Errorf("forwardScanForKey: reading value record: %w", err)
	}

	return valueBuf, nil
}

// Helper to read from index cache or file
func (s *ExtentKeyValStore) readIndexAt(indexFile *os.File, cache []byte, offset int64, data interface{}) error {
	if EnableIndexCaching && cache != nil {
		if offset < 0 || offset+8 > int64(len(cache)) {
			return fmt.Errorf("index cache read out of bounds: offset=%d, len=%d", offset, len(cache))
		}
		return binary.Read(bytes.NewReader(cache[offset:offset+8]), binary.BigEndian, data)
	}

	// Fall back to file read if caching disabled or cache not loaded
	_, err := indexFile.Seek(offset, 0)
	if err != nil {
		return err
	}
	return binary.Read(indexFile, binary.BigEndian, data)
}

// --- In Put(), flush the index caches and count the flush.
func (s *ExtentKeyValStore) Put(key, value []byte) error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

	if EnableIndexCaching {
		s.cacheWrites++ // count this flush
	}

	// Move to end of data file for keys.
	keyPos, err := s.keysFile.Seek(0, 2)
	if err != nil {
		return err
	}
	keySize, err := s.keysFile.Write(key)
	if err != nil {
		return err
	}
	if keySize != len(key) {
		panic("Key size mismatch")
	}
	err = binary.Write(s.keysIndex, binary.BigEndian, keyPos+int64(keySize))
	if err != nil {
		return err
	}

	// Move to end of data file for values.
	valuePos, err := s.valuesFile.Seek(0, 2)
	if err != nil {
		return err
	}
	valueSize, err := s.valuesFile.Write(value)
	if err != nil {
		return err
	}
	if valueSize != len(value) {
		panic("Value size mismatch")
	}
	err = binary.Write(s.valuesIndex, binary.BigEndian, valuePos+int64(valueSize))
	if err != nil {
		return err
	}

	if EnableIndexCaching {
		s.cache[string(key)] = true
	}
	return nil
}

func readNbytes(file *os.File, n int) ([]byte, error) {
	buffer := make([]byte, n)
	totalRead := 0
	countReads := 0
	for totalRead < n {
		n, err := file.Read(buffer[totalRead:])
		if err != nil {
			return nil, err
		}
		totalRead += n
		countReads++
		if countReads > 100 {
			fmt.Println("Too many reads")
		}
	}
	return buffer, nil
}

// --- Modify Get() similarly to update the counters.
func (s *ExtentKeyValStore) Get(key []byte) ([]byte, error) {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

	s.maybePrintCacheStats() // Increment request counter and print stats every 100th call

	if EnableIndexCaching {
		state, exists := s.cache[string(key)]
		if exists {
			s.cacheHits++ // key was found in the cache map (even though we still go to disk for value)
			if !state {
				return nil, errors.New("key marked as not present in cache")
			}
		} else {
			s.cacheMisses++
		}
	}

	val, err := s.forwardScanForKey(key)
	if err != nil {
		return nil, err
	}

	if EnableIndexCaching {
		s.cache[string(key)] = true
	}
	return val, nil
}

// 1. Read the index file at position indexPosition to get dataPos
// 2. Read the next index file at position (indexPosition + 8) to get the end of the block in the data file
// 3. Read the data file from dataPos to end of block

// Modify readDataAtIndexPos to use caching
func readDataAtIndexPos(indexPosition int64, indexFile *os.File, dataFile *os.File, cache []byte) ([]byte, bool, error) {
	deleted := false
	var dataPos int64 = 0

	// Read from cache or file
	var err error
	if EnableIndexCaching && cache != nil {
		err = binary.Read(bytes.NewReader(cache[indexPosition:indexPosition+8]), binary.BigEndian, &dataPos)
	} else {
		_, err = indexFile.Seek(indexPosition, 0)
		if err != nil {
			return nil, false, err
		}
		err = binary.Read(indexFile, binary.BigEndian, &dataPos)
	}
	if err != nil {
		return nil, false, err
	}

	// Rest of the function remains the same...
	if dataPos < 0 {
		deleted = true
		dataPos = -dataPos
	}

	_, err = dataFile.Seek(dataPos, 0)
	if err != nil {
		return nil, false, err
	}

	// Read next index position
	var nextDataPos int64
	if EnableIndexCaching && cache != nil {
		if indexPosition+8 >= int64(len(cache)) {
			return nil, false, fmt.Errorf("invalid index position")
		}
		err = binary.Read(bytes.NewReader(cache[indexPosition+8:indexPosition+16]), binary.BigEndian, &nextDataPos)
	} else {
		err = binary.Read(indexFile, binary.BigEndian, &nextDataPos)
	}
	if err != nil {
		return nil, false, err
	}

	if nextDataPos < 0 {
		nextDataPos = -nextDataPos
	}

	size := nextDataPos - dataPos
	buffer := make([]byte, size)
	_, err = dataFile.Read(buffer)
	if err != nil {
		return nil, false, err
	}

	return buffer, deleted, nil
}

func (s *ExtentKeyValStore) Close() error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()
	fmt.Println("Closing ExtentKeyValueStore at", s.keysFile.Name())
	if err := s.keysFile.Close(); err != nil {
		return err
	}
	if err := s.valuesFile.Close(); err != nil {
		return err
	}
	if err := s.keysIndex.Close(); err != nil {
		return err
	}
	if err := s.valuesIndex.Close(); err != nil {
		return err
	}
	return nil
}

func (s *ExtentKeyValStore) List() ([]string, error) {
	keyMap, err := s.MapFunc(func(key []byte, value []byte) error {
		return nil
	})
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(keyMap))
	for k, _ := range keyMap {
		keys = append(keys, k)
	}
	return keys, nil
}

func (s *ExtentKeyValStore) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()
	return s.LockFreeMapFunc(f)
}

func (s *ExtentKeyValStore) Delete(key []byte) error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

	if EnableIndexCaching {
		s.cacheWrites++ // count this flush
	}

	// If the in‑memory cache is being used, check whether the key is already marked deleted.
	if s.cache != nil {
		if state, exists := s.cache[string(key)]; exists && !state {
			return errors.New("key not found (already deleted)")
		}
	}

	// --- Process the keys file & keys.index ---
	// Append the key bytes to the keys data file.
	keyPos, err := s.keysFile.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("Delete: seeking keys file: %w", err)
	}
	written, err := s.keysFile.Write(key)
	if err != nil {
		return fmt.Errorf("Delete: writing key data: %w", err)
	}
	if written != len(key) {
		return errors.New("Delete: key size mismatch")
	}

	// Overwrite the last 8-byte index entry in keys.index with a tombstone.
	// A tombstone is written as the negative of the file offset (keyPos).
	tombstone := -keyPos
	eofKeyIndex, err := s.keysIndex.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("Delete: seeking keys index: %w", err)
	}
	// Compute the position of the last index entry.
	keyIndexPos := eofKeyIndex - 8
	if _, err := s.keysIndex.Seek(keyIndexPos, io.SeekStart); err != nil {
		return fmt.Errorf("Delete: seeking to last key index entry: %w", err)
	}
	if err := binary.Write(s.keysIndex, binary.BigEndian, tombstone); err != nil {
		return fmt.Errorf("Delete: writing tombstone to keys index: %w", err)
	}
	// NOTE: We do NOT append a new pointer afterward.

	// --- Process the values file & values.index ---
	// For values, write the key bytes (or a placeholder) to the values data file.
	valuePos, err := s.valuesFile.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("Delete: seeking values file: %w", err)
	}
	written, err = s.valuesFile.Write(key)
	if err != nil {
		return fmt.Errorf("Delete: writing value data: %w", err)
	}
	if written != len(key) {
		return errors.New("Delete: value size mismatch")
	}

	// Overwrite the last 8-byte index entry in values.index with a tombstone.
	tombstoneVal := -valuePos
	eofValueIndex, err := s.valuesIndex.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("Delete: seeking values index: %w", err)
	}
	valueIndexPos := eofValueIndex - 8
	if _, err := s.valuesIndex.Seek(valueIndexPos, io.SeekStart); err != nil {
		return fmt.Errorf("Delete: seeking to last values index entry: %w", err)
	}
	if err := binary.Write(s.valuesIndex, binary.BigEndian, tombstoneVal); err != nil {
		return fmt.Errorf("Delete: writing tombstone to values index: %w", err)
	}
	// Again, we do NOT append a new pointer afterward.

	// Update the in‑memory cache if it's used.
	if s.cache != nil {
		s.cache[string(key)] = false
	}

	return nil
}

func (s *ExtentKeyValStore) Flush() error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()
	err := s.keysFile.Sync()
	if err != nil {
		return err
	}
	err = s.valuesFile.Sync()
	if err != nil {
		return err
	}
	err = s.keysIndex.Sync()
	if err != nil {
		return err
	}
	err = s.valuesIndex.Sync()

	return nil
}

func (s *ExtentKeyValStore) DumpIndex() error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()
	_, err := s.keysIndex.Seek(0, 0)
	if err != nil {
		panic(err)
	}
	_, err = s.valuesIndex.Seek(0, 0)
	if err != nil {
		panic(err)
	}
	entry := 0
	for {
		var keyPos int64
		err = binary.Read(s.keysIndex, binary.BigEndian, &keyPos)
		if err != nil {
			break
		}
		fmt.Printf("KEY Entry: %d, BytePosition: %d\n", entry, keyPos)
		var valuePos int64
		err = binary.Read(s.valuesIndex, binary.BigEndian, &valuePos)
		if err != nil {
			break
		}
		fmt.Printf("VALUE Entry: %d, BytePosition: %d\n", entry, valuePos)
		entry++
	}
	return nil
}

func (s *ExtentKeyValStore) Size() int64 {
	var count int64
	_, err := s.MapFunc(func(k, v []byte) error {
		count++
		return nil
	})
	if err != nil {
		return 0
	}
	return count
}

func (s *ExtentKeyValStore) LockFreeMapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	_, err := s.keysFile.Seek(0, 0)
	if err != nil {
		panic(err)
	}
	_, err = s.keysIndex.Seek(0, 0)
	if err != nil {
		panic(err)
	}
	_, err = s.valuesFile.Seek(0, 0)
	if err != nil {
		panic(err)
	}
	_, err = s.valuesIndex.Seek(0, 0)
	if err != nil {
		panic(err)
	}

	var validKeys = make(map[string]bool)
	//start at the end of the keysIndex file
	keyIndexPosStart, err := s.keysIndex.Seek(-8, 2)
	if err != nil {
		panic(err)
	}

	for {
		keyIndexPosStart = keyIndexPosStart - 8
		if keyIndexPosStart < 0 {
			return validKeys, nil
		}

		keyData, deleted, err := readDataAtIndexPos(keyIndexPosStart, s.keysIndex, s.keysFile, s.keysIndexCache)
		if err != nil {
			return nil, err
		}

		// Have we seen this key before?
		_, seen := validKeys[string(keyData)]
		if seen {
			continue
		}

		// check if this is a tombstone.  A position of -1 indicates a deleted key
		if deleted {
			validKeys[string(keyData)] = false
			continue
		} else {
			validKeys[string(keyData)] = true
		}

		valueBuffer, _, err := readDataAtIndexPos(keyIndexPosStart, s.valuesIndex, s.valuesFile, s.valuesIndexCache)
		if err != nil {
			return nil, err
		}
		err = f(keyData, valueBuffer)
		if err != nil {
			return nil, err
		}
	}
}

// searchDbForKeyExists performs a forward scan of the keys index and keys data files.
// For each record, it extracts the key from keys.dat and, if it matches the provided key,
// it updates the “latest” state. In the end, if at least one matching record was found,
// the final state (insertion or deletion) is returned.
func searchDbForKeyExists(key []byte, keysIndex *os.File, keysFile *os.File, keysIndexCache []byte) (bool, error) {
	// Reset file positions for a fresh sequential scan.
	if _, err := keysIndex.Seek(0, io.SeekStart); err != nil {
		return false, fmt.Errorf("searchDbForKeyExists: seeking keys index: %w", err)
	}
	if _, err := keysFile.Seek(0, io.SeekStart); err != nil {
		return false, fmt.Errorf("searchDbForKeyExists: seeking keys file: %w", err)
	}

	// Read the initial offset from keys.index.
	// This offset should be 0 and serves as the beginning of the first record.
	var prevOffset int64
	if err := binary.Read(keysIndex, binary.BigEndian, &prevOffset); err != nil {
		return false, fmt.Errorf("searchDbForKeyExists: reading initial offset: %w", err)
	}

	// Initialize our found flag and a variable to record the latest deletion status.
	found := false
	latestDeleted := false

	// Iterate over the remaining index records sequentially.
	// Each record boundary is determined by a pair of consecutive offsets.
	for {
		var curOffset int64
		if err := binary.Read(keysIndex, binary.BigEndian, &curOffset); err == io.EOF {
			break // End of index file reached.
		} else if err != nil {
			return false, fmt.Errorf("searchDbForKeyExists: reading next offset: %w", err)
		}

		// A negative offset indicates that this record is a deletion (a tombstone).
		deleted := false
		effectiveCur := curOffset
		if curOffset < 0 {
			deleted = true
			effectiveCur = -curOffset
		}

		// Compute the length of this key record.
		recordLen := effectiveCur - prevOffset
		if recordLen < 0 {
			return false, errors.New("searchDbForKeyExists: computed negative record length")
		}

		// Read the key record from keys.dat.
		keyRecord := make([]byte, recordLen)
		n, err := io.ReadFull(keysFile, keyRecord)
		if err != nil {
			return false, fmt.Errorf("searchDbForKeyExists: reading key record: %w", err)
		}
		if int64(n) != recordLen {
			return false, fmt.Errorf("searchDbForKeyExists: incomplete key record: expected %d bytes, got %d", recordLen, n)
		}

		// If this record matches the provided key, record its state.
		if bytes.Equal(key, keyRecord) {
			found = true
			latestDeleted = deleted
		}

		// Update prevOffset to the end of the current record for the next iteration.
		prevOffset = effectiveCur
	}

	// If no record for the key was encountered, report that it was not found.
	if !found {
		return false, errors.New("key not found")
	}

	// Otherwise, return true if the last occurrence was an insertion (not deleted).
	return !latestDeleted, nil
}

// --- Modify Exists() to use and update the cache counters.
func (s *ExtentKeyValStore) Exists(key []byte) bool {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

	s.maybePrintCacheStats() // Increment request counter and print stats every 100th call

	if EnableIndexCaching {
		// If the in‑memory cache is empty, load it completely.
		if len(s.cache) == 0 {
			if err := s.loadKeyCache(); err != nil {
				// If the cache cannot be loaded, fall back to the previous behavior.
				found, err := searchDbForKeyExists(key, s.keysIndex, s.keysFile, s.keysIndexCache)
				return err == nil && found
			}
		}

		state, exists := s.cache[string(key)]
		if exists {
			s.cacheHits++ // found in cache
			return state
		}
		s.cacheMisses++ // not found in cache
	} else {
		found, err := searchDbForKeyExists(key, s.keysIndex, s.keysFile, s.keysIndexCache)
		return err == nil && found
	}
	return false
}

func (s *ExtentKeyValStore) LockFreeGet(key []byte) ([]byte, error) {
	// Reset file positions
	_, err := s.keysFile.Seek(0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to seek keys file: %w", err)
	}
	_, err = s.keysIndex.Seek(0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to seek keys index: %w", err)
	}
	_, err = s.valuesFile.Seek(0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to seek values file: %w", err)
	}
	_, err = s.valuesIndex.Seek(0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to seek values index: %w", err)
	}

	// Load index caches if enabled
	if EnableIndexCaching {
		if err := s.loadKeysIndexCache(); err != nil {
			return nil, fmt.Errorf("failed to load keys index cache: %w", err)
		}
		if err := s.loadValuesIndexCache(); err != nil {
			return nil, fmt.Errorf("failed to load values index cache: %w", err)
		}
	}

	// Get end of keys index file
	keyIndexPosEndFile, err := s.keysIndex.Seek(0, 2)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to end of keys index: %w", err)
	}

	keyIndexPosStart := keyIndexPosEndFile - 8
	if keyIndexPosStart < 0 {
		return nil, fmt.Errorf("corrupt index file: %s", s.keysIndex.Name())
	}

	// Search for key from end to beginning
	found := false
	for {
		keyIndexPosStart = keyIndexPosStart - 8
		if keyIndexPosStart < 0 {
			return nil, fmt.Errorf("key not found after searching to start of file")
		}

		// Read key data using cache if available
		data, deleted, err := readDataAtIndexPos(
			keyIndexPosStart,
			s.keysIndex,
			s.keysFile,
			s.keysIndexCache,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to read key data: %w", err)
		}

		// Check if we found the key
		if bytes.Equal(key, data) {
			if deleted {
				return nil, fmt.Errorf("key has been deleted at entry %d", keyIndexPosStart/8)
			}
			found = true
			break
		}
	}

	if !found {
		return nil, fmt.Errorf("key not found after searching to start of file")
	}

	// Read value data using cache if available
	data, _, err := readDataAtIndexPos(
		keyIndexPosStart,
		s.valuesIndex,
		s.valuesFile,
		s.valuesIndexCache,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to read value data: %w", err)
	}

	return data, nil
}
