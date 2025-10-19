package ensemblekv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/donomii/clusterF/syncmap"
	"github.com/donomii/goof"
	"github.com/sasha-s/go-deadlock"
)

var EnableIndexCaching bool = true // Feature flag for index caching
var ExtraChecks bool = false

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

// Like printf, but only if debugging is enabled
func debugf(format string, args ...interface{}) {
	return // if !Debug {
	fmt.Printf(format, args...)

}

func debugln(format string, args ...interface{}) {
	return // if !Debug {
	debugln(format, args...)

}

// --- Add these new fields to ExtentKeyValStore struct:
type ExtentKeyValStore struct {
	DefaultOps
	keysFile    *os.File
	valuesFile  *os.File
	keysIndex   *os.File
	valuesIndex *os.File
	blockSize   int64
	globalLock  deadlock.Mutex
	cache       *syncmap.SyncMap[string, bool]

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
		debugf("Cache stats: hits=%d, misses=%d, writes=%d, loads=%d\n",
			s.cacheHits, s.cacheMisses, s.cacheWrites, s.cacheLoads)
	}
}

func panicOnError(reason string, err error) {
	if err != nil {
		panic(fmt.Sprintf("%v:%v", reason, err))
	}
}

func checkLastIndexEntry(indexFile *os.File, dataFile *os.File) {
	if !ExtraChecks {
		return
	}
	_, err := indexFile.Seek(-8, 2)
	panicOnError("Seek to last entry in index file", err)
	var lastEntry int64
	err = binary.Read(indexFile, binary.BigEndian, &lastEntry)
	panicOnError("Read last entry in index file", err)
	debugln("Last entry in index file is", lastEntry)
	if lastEntry < 0 {
		panic("Last entry in index file is negative")
	}

	//Get length of data file
	dataFileLength, err := dataFile.Seek(0, 2)
	panicOnError("Seek to end of data file", err)
	if lastEntry != dataFileLength {
		panic(fmt.Sprintf("Last entry in index file does not match data file length: %d != %d", lastEntry, dataFileLength))
	}
}

func checkIndexSigns(keyIndex *os.File, valueIndex *os.File) {
	if !ExtraChecks {
		return
	}
	_, err := keyIndex.Seek(0, 0)
	panicOnError("Seek to start of index file", err)
	_, err = valueIndex.Seek(0, 0)
	panicOnError("Seek to start of index file", err)
	for {

		var keyOffset int64
		err = binary.Read(keyIndex, binary.BigEndian, &keyOffset)
		if err == io.EOF {
			break
		}
		panicOnError("Read entry in index file", err)
		var valueOffset int64
		err = binary.Read(valueIndex, binary.BigEndian, &valueOffset)
		if err == io.EOF {
			break
		}
		panicOnError("Read entry in data file", err)
		if keyOffset == 0 && valueOffset != 0 {
			panic("Key offset is 0 but value offset is not")
		}
		if keyOffset < 0 && valueOffset > 0 {
			panic("Key offset is negative but value offset is positive")
		}
		if keyOffset > 0 && valueOffset < 0 {
			panic("Key offset is positive but value offset is negative")
		}
		if keyOffset != 0 && valueOffset == 0 {
			panic("Key offset is negative but value offset is 0")
		}
		if keyOffset == 0 && valueOffset != 0 {
			panic("Key offset is 0 and value offset is not 0")
		}

	}
}

func NewExtentKeyValueStore(directory string, blockSize, filesize int64) (*ExtentKeyValStore, error) {
	//debugln("Creating new ExtentKeyValueStore with enhanced caching at directory", directory)
	os.MkdirAll(directory, 0755)
	keysFilePath := directory + "/keys.dat"
	valuesFilePath := directory + "/values.dat"
	keysIndexFilePath := directory + "/keys.index"
	valuesIndexFilePath := directory + "/values.index"

	if !goof.Exists(keysIndexFilePath) {
		//Write a single 0 to the keys index file
		keysIndex, err := os.OpenFile(keysIndexFilePath, os.O_CREATE|os.O_RDWR, 0644)
		panicOnError("Open keys index file for writing", err)
		err = binary.Write(keysIndex, binary.BigEndian, int64(0))
		panicOnError("Write a 0 to init an index file", err)
		keysIndex.Close()
	}

	if !goof.Exists(valuesIndexFilePath) {
		//Write a single 0 to the values index file
		valuesIndex, err := os.OpenFile(valuesIndexFilePath, os.O_CREATE|os.O_RDWR, 0644)
		panicOnError("Open values index file for writing", err)
		err = binary.Write(valuesIndex, binary.BigEndian, int64(0))
		panicOnError("Write a 0 to init an index file", err)
		valuesIndex.Close()
	}

	keysFile, err := os.OpenFile(keysFilePath, os.O_CREATE|os.O_RDWR, 0644)
	panicOnError("Open keys data file", err)

	keysIndex, err := os.OpenFile(keysIndexFilePath, os.O_CREATE|os.O_RDWR, 0644)
	panicOnError("Open keys index file", err)

	// Keys index file length must be a multiple of 8
	stat, _ := keysIndex.Stat()
	if stat.Size()%8 != 0 {
		panic("Keys index file length is not a multiple of 8")
	}

	// Keys index file must contain at least one entry
	if stat.Size() < 8 {
		panic("Keys index file is empty")
	}

	//Read the last index item to get the end of the file.  If this is different to the file size, then the file is corrupt and we should truncate it to size
	_, err = keysIndex.Seek(-8, 2)
	panicOnError("Seek to last entry in keys index file", err)
	var keysFileEnd int64
	err = binary.Read(keysIndex, binary.BigEndian, &keysFileEnd)
	panicOnError("Read last entry in keys index file", err)
	if keysFileEnd < 0 {
		keysFileEnd = -keysFileEnd
	}

	stat, _ = keysFile.Stat()
	if keysFileEnd != stat.Size() {
		// Get the difference between the expected size and the actual size
		diff := keysFileEnd - stat.Size()
		if diff < 0 {
			debugf("Index position is larger than actual keys file size: %d, cannot recover automatically\n", diff)
		}
		if diff > 1024 {
			debugf("Difference between recorded end of keys file(in index) and actual end of file is too large: %d bytes.  Refusing to truncate, cannot recover from corrupted file\n", diff)
			os.Exit(1)
		}
		debugf("Keys file is larger than recorded in keys index.  Truncating keys index file %s to %d\n", keysFilePath, keysFileEnd)
		keysFile.Truncate(keysFileEnd)
	}

	valuesIndex, err := os.OpenFile(valuesIndexFilePath, os.O_CREATE|os.O_RDWR, 0644)
	panicOnError("Open values index file", err)

	valuesFile, err := os.OpenFile(valuesFilePath, os.O_CREATE|os.O_RDWR, 0644)
	panicOnError("Open values data file", err)

	// Values index file length must be a multiple of 8
	stat, _ = valuesIndex.Stat()
	if stat.Size()%8 != 0 {
		panic("Values index file length is not a multiple of 8")
	}

	// Values index file must contain at least one entry
	if stat.Size() < 8 {
		panic("Values index file is empty")
	}

	//Read the last index item to get the end of the file.  If this is different to the file size, then the file is corrupt and we should truncate it to size
	_, err = valuesIndex.Seek(-8, 2)
	panicOnError("Seek to last entry in values index file", err)
	var valuesFileEnd int64
	err = binary.Read(valuesIndex, binary.BigEndian, &valuesFileEnd)
	panicOnError("Read last entry in values index file", err)
	if valuesFileEnd < 0 {
		valuesFileEnd = -valuesFileEnd
	}
	stat, _ = valuesFile.Stat()
	if valuesFileEnd != stat.Size() {
		// Get the difference between the expected size and the actual size
		diff := valuesFileEnd - stat.Size()
		if diff < 0 {
			debugf("Index position is larger than actual values file size: %d, cannot recover automatically\n", diff)
		}
		if diff > 8 {
			debugf("Difference between recorded end of file and actual end of file is too large: %d, refusing to truncate, cannot recover from corrupted file\n", diff)
			os.Exit(1)
		}
		debugf("Values file is larger than recorded in values index.  Truncating values index file %s to %d\n", valuesFilePath, valuesFileEnd)
		valuesFile.Truncate(valuesFileEnd)
	}

	s := &ExtentKeyValStore{
		keysFile:    keysFile,
		valuesFile:  valuesFile,
		keysIndex:   keysIndex,
		valuesIndex: valuesIndex,
		blockSize:   blockSize,
	}

	if EnableIndexCaching {
		s.ClearCache()
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
	s.globalLock.Lock()
	defer s.globalLock.Unlock()
	// This routine should only be called when caching is enabled.
	if !EnableIndexCaching {
		return fmt.Errorf("loadKeyCache should not be called when caching is disabled")
	}

	if s.cache == nil {
		s.cache = (*syncmap.SyncMap[string, bool])(syncmap.NewSyncMap[string, bool]())
	}

	keyIndexFileLength, err := s.keysIndex.Seek(0, 2)
	panicOnError("Get key index file length", err)

	_, err = s.keysIndex.Seek(0, 0)
	panicOnError("Seek to start of keys index file", err)
	_, err = s.valuesIndex.Seek(0, 0)
	panicOnError("Seek to start of values index file", err)
	entry := int64(0)

	for {
		if entry*8+8 >= int64(keyIndexFileLength) {
			break
		}
		var keyPos int64
		_, err := s.keysIndex.Seek(int64(entry)*8, 0)
		panicOnError("Seek to current entry in keys index file", err)
		err = binary.Read(s.keysIndex, binary.BigEndian, &keyPos)
		if err != nil {
			break
		}
		//debugf("KEY   Entry: %d, BytePosition: %d\n", entry, keyPos)
		keyData, deleted, err := s.readDataAtIndexPos(int64(entry*8), s.keysIndex, s.keysFile, s.keysIndexCache)
		panicOnError("Read key data", err)
		//debugf("searchDbForKeyExists KEY   Entry: %d, BytePosition: %d, Deleted: %t, Key: %s\n", entry, keyPos, deleted, trimTo40(keyData))

		var valuePos int64
		_, err = s.valuesIndex.Seek(int64(entry)*8, 0)
		panicOnError("Seek to current entry in values index file", err)
		err = binary.Read(s.valuesIndex, binary.BigEndian, &valuePos)
		if err != nil {
			break
		}

		//debugf("searchDbForKeyExists VALUE Entry: %d, BytePosition: %d\n", entry, valuePos)

		s.cache.Store(string(keyData), !deleted)

		entry++

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
	s.globalLock.Lock()
	defer s.globalLock.Unlock()
	checkLastIndexEntry(s.keysIndex, s.keysFile)
	checkLastIndexEntry(s.valuesIndex, s.valuesFile)
	checkIndexSigns(s.keysIndex, s.valuesIndex)

	keysIndexFileLength, err := s.keysFile.Seek(0, 2)
	panicOnError("Seek to end of keys index file", err)

	found, offset, err := s.searchDbForKeyExists(key, s.keysIndex, s.keysFile, s.keysIndexCache)
	if err != nil {
		if EnableIndexCaching {
			s.cache.Store(string(key), false)
		}
		return nil, fmt.Errorf("forwardScanForKey: not found: %v", err)
	}

	if !found {
		if EnableIndexCaching {
			s.cache.Store(string(key), false)
		}
		return nil, fmt.Errorf("forwardScanForKey: key not found or deleted")
	}

	if keysIndexFileLength == offset {
		if EnableIndexCaching {
			s.cache.Store(string(key), false)
		}
		return nil, fmt.Errorf("forwardScanForKey: key not found or deleted because offset is at end of file")
	}

	debugln("forwardScanForKey: Retrieving value for key", trimTo40(key), "at offset", offset)
	checkIndexSigns(s.keysIndex, s.valuesIndex)
	value, _, err := s.readDataAtIndexPos(offset, s.valuesIndex, s.valuesFile, s.valuesIndexCache)
	if err != nil {
		return nil, fmt.Errorf("forwardScanForKey: failed to read value: %v", err)
	}

	/* FIXME - something is writing the value positions as negative and flagging the values as deleted, when they should not be
	if deleted {
		return nil, fmt.Errorf("forwardScanForKey: key marked as deleted")
	}
	*/

	if EnableIndexCaching {
		s.cache.Store(string(key), true)
	}
	return value, nil
}

func (s *ExtentKeyValStore) KeyHistory(key []byte) ([][]byte, error) {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

	// For ExtentKeyValStore, we need to scan all entries in the keys and values files
	history := make([][]byte, 0)

	// Reset file positions
	s.keysFile.Seek(0, 0)
	s.keysIndex.Seek(0, 0)
	s.valuesFile.Seek(0, 0)
	s.valuesIndex.Seek(0, 0)

	// Get key index file length
	keyIndexFileLength, _ := s.keysIndex.Seek(0, 2)
	s.keysIndex.Seek(0, 0)

	// Load index caches if enabled
	if EnableIndexCaching {
		s.loadKeysIndexCache()
		s.loadValuesIndexCache()
	}

	// Scan through all entries
	entry := int64(0)
	for {
		if entry*8+16 > int64(keyIndexFileLength) {
			break
		}

		// Read the key at this entry
		keyData, deleted, err := s.readDataAtIndexPos(int64(entry*8), s.keysIndex, s.keysFile, s.keysIndexCache)
		if err != nil {
			entry++
			continue
		}

		// If this is the key we're looking for and not deleted
		if bytes.Equal(keyData, key) && !deleted {
			// Get the value
			valueData, _, err := s.readDataAtIndexPos(int64(entry*8), s.valuesIndex, s.valuesFile, s.valuesIndexCache)
			if err != nil {
				entry++
				continue
			}

			history = append(history, valueData)
		}

		entry++
	}

	return history, nil
}

// Helper to read from index cache or file
func (s *ExtentKeyValStore) readIndexAt(indexFile *os.File, cache []byte, offset int64, data interface{}) error {
	if EnableIndexCaching && cache != nil {
		if offset < 0 || offset+8 > int64(len(cache)) {
			panic(fmt.Sprintf("index cache read out of bounds: offset=%d, len=%d", offset, len(cache)))
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

func (s *ExtentKeyValStore) ClearCache() {
	s.keysIndexCache = nil
	s.valuesIndexCache = nil
	s.cache = syncmap.NewSyncMap[string, bool]()
}

func (s *ExtentKeyValStore) Put(key, value []byte) error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

	// Move to end of data file for values.
	valuePos, err := s.valuesFile.Seek(0, 2)
	panicOnError("Seek to end of values file", err)
	valueSize, err := s.valuesFile.Write(value)
	panicOnError("Write value to values file", err)

	if valueSize != len(value) {
		panic("Wrote wrong number of bytes")
	}

	// Get the end of file position for the values file
	endOfValuesFile, err := s.valuesFile.Seek(0, 2)
	panicOnError("Seek to end of values file", err)

	// Check this value matches the calculated end of file
	if valuePos+int64(valueSize) != endOfValuesFile {
		panic(fmt.Errorf("valuePos+valueSize (%d) != endOfValuesFile (%d)", valuePos+int64(valueSize), endOfValuesFile))
	}

	// Write the end of the file position to the values index.
	_, err = s.valuesIndex.Seek(0, 2)
	panicOnError("Seek to end of values index file", err)

	err = binary.Write(s.valuesIndex, binary.BigEndian, endOfValuesFile)
	panicOnError("Write end of values file to values index file", err)

	// Move to end of data file for keys.
	keyDataPos, err := s.keysFile.Seek(0, 2)
	panicOnError("Seek to end of keys file", err)

	// Write the key to the keys file.
	keySize, err := s.keysFile.Write(key)
	panicOnError("Write key to keys file", err)

	// Check we wrote the correct number of bytes
	if keySize != len(key) {
		panic("Key size mismatch")
	}

	// Get the end of file position for the keys file
	endOfKeysFile, err := s.keysFile.Seek(0, 2)
	panicOnError("Seek to end of keys file", err)

	// Check this key matches the calculated end of file
	if keyDataPos+int64(keySize) != endOfKeysFile {
		panic(fmt.Errorf("keyDataPos+keySize (%d) != endOfKeysFile (%d)", keyDataPos+int64(keySize), endOfKeysFile))
	}

	// Seek to the end of the keys index
	_, err = s.keysIndex.Seek(0, 2)
	panicOnError("Seek to end of keys index file", err)

	// Write the end of the file position to the keys index.
	err = binary.Write(s.keysIndex, binary.BigEndian, endOfKeysFile)
	panicOnError("Write to keys index file", err)

	checkLastIndexEntry(s.keysIndex, s.keysFile)
	checkLastIndexEntry(s.valuesIndex, s.valuesFile)
	checkIndexSigns(s.keysIndex, s.valuesIndex)
	if EnableIndexCaching {
		s.cacheWrites++
		s.cache.Store(string(key), true)
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
			debugln("Too many reads")
		}
	}
	return buffer, nil
}

// --- Modify Get() similarly to update the counters.
func (s *ExtentKeyValStore) Get(key []byte) ([]byte, error) {
	s.maybePrintCacheStats() // Increment request counter and print stats every 100th call

	if EnableIndexCaching {
		state, exists := s.cache.Load(string(key))
		if exists {
			s.cacheHits++ // key was found in the cache map (even though we still go to disk for value)
			if !state {
				return nil, errors.New("key marked as not present in cache")
			}
		} else {
			s.cacheMisses++
		}
	}

	val, err := s.forwardScanForKey(key) // We should add a found return value
	if err != nil {
		return nil, fmt.Errorf("Get: %w", err)
	}

	return val, nil
}

// 1. Read the index file at position indexPosition to get dataPos
// 2. Read the next index file at position (indexPosition + 8) to get the end of the block in the data file
// 3. Read the data file from dataPos to end of block

// Modify readDataAtIndexPos to use caching
func (s *ExtentKeyValStore) readDataAtIndexPos(indexPosition int64, indexFile *os.File, dataFile *os.File, cache []byte) ([]byte, bool, error) {
	dataFileLength, err := dataFile.Seek(0, 2)
	indexFileLength, err := indexFile.Seek(0, 2)
	deleted := false
	var dataPos int64 = 0

	if indexFileLength == indexPosition+8 {
		panic(fmt.Errorf("readDataAtIndexPos: invalid index position, data file length is %d, and position is %d (this is the end of file marker, you read one too far)", indexFileLength, indexPosition))
	}

	if indexPosition+16 > indexFileLength {
		panic(fmt.Sprintf("attempt to read past end of file: offset=%d, index file length len=%d.  Last index is the seocnd last pointer(len-16), not the last pointer(len-8)", indexPosition, indexFileLength))
		return nil, false, fmt.Errorf("readDataAtIndexPos: invalid index position %v greater than indexfile of length %v", indexPosition, indexFileLength)
	}

	// Read from cache or file
	if EnableIndexCaching && cache != nil {
		//Read the offset to the data
		err = binary.Read(bytes.NewReader(cache[indexPosition:indexPosition+8]), binary.BigEndian, &dataPos)
	} else {
		_, err = indexFile.Seek(indexPosition, 0)
		if err != nil {
			return nil, false, fmt.Errorf("failed to seek to index position: %w", err)
		}
		//Read the offset to the data
		err = binary.Read(indexFile, binary.BigEndian, &dataPos)
	}
	if err != nil {
		return nil, false, fmt.Errorf("failed to read data offset at index position: %v, length %v, file length %v, %v", indexPosition, 8, indexFileLength, err)
	}

	if dataPos < 0 {
		deleted = true
		dataPos = -dataPos
	}

	// Read next index position
	var nextDataPos int64
	if EnableIndexCaching && cache != nil {
		if indexPosition+16 > int64(len(cache)) {
			return nil, false, fmt.Errorf("invalid index position %v for cache of length %v.  Probable attempted read on the last entry, but the last entry is always the closing entry", indexPosition, len(cache))
		}
		//Read the offset to the next data block (or EOF offset i.e. file length.  The last index entry always points to the end of the data file)
		err = binary.Read(bytes.NewReader(cache[indexPosition+8:indexPosition+16]), binary.BigEndian, &nextDataPos)
	} else {
		_, err = indexFile.Seek(indexPosition+8, 0)
		if err != nil {
			return nil, false, fmt.Errorf("failed to seek to data position: %w", err)
		}
		// Read the offset to the next data block (or EOF offset i.e. file length.  The last index entry always points to the end of the data file)
		err = binary.Read(indexFile, binary.BigEndian, &nextDataPos)
	}
	if err != nil {
		return nil, false, fmt.Errorf("readDataAtIndexPos: failed to read next index position: %w", err)
	}

	if nextDataPos < 0 {
		nextDataPos = -nextDataPos
	}

	if nextDataPos > dataFileLength {
		return nil, false, fmt.Errorf("invalid next data position %v for datafile of length %v", nextDataPos, dataFileLength)
	}

	size := nextDataPos - dataPos
	if size < 0 {
		return nil, false, fmt.Errorf("invalid size %v for dataPos %v and nextDataPos %v", size, dataPos, nextDataPos)
	}

	//Don't read if the size is 0, we already know what the contents will be :D
	if size == 0 {
		return nil, deleted, nil
	}

	buffer := make([]byte, size)
	_, err = dataFile.Seek(dataPos, 0)
	if err != nil {
		return nil, false, fmt.Errorf("failed to seek to data position: %w", err)
	}
	_, err = dataFile.Read(buffer)
	if err != nil {
		return nil, false, fmt.Errorf("failed to read data: %w", err)
	}

	return buffer, deleted, nil
}

func (s *ExtentKeyValStore) Close() error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()
	//debugln("Closing ExtentKeyValueStore at", s.keysFile.Name())
	if err := s.keysFile.Close(); err != nil {
		return fmt.Errorf("failed to close keys file: %w", err)
	}
	if err := s.valuesFile.Close(); err != nil {
		return fmt.Errorf("failed to close values file: %w", err)
	}
	if err := s.keysIndex.Close(); err != nil {
		return fmt.Errorf("failed to close keys index: %w", err)
	}
	if err := s.valuesIndex.Close(); err != nil {
		return fmt.Errorf("failed to close values index: %w", err)
	}
	return nil
}

func (s *ExtentKeyValStore) List() ([]string, error) {
	s.loadKeyCache()
	keys := s.cache.Keys()
	return keys, nil
}

func (s *ExtentKeyValStore) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	s.loadKeyCache()
	keys := s.cache.Keys()

	out := make(map[string]bool)
	for _, key := range keys {
		value, err := s.Get([]byte(key))
		if err == nil {
			f([]byte(key), value)
			out[key] = true
		}
	}

	return out, nil
}

func (s *ExtentKeyValStore) Delete(key []byte) error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

	checkIndexSigns(s.keysIndex, s.valuesIndex)

	if EnableIndexCaching {
		s.cacheWrites++ // count this flush
	}
	s.ClearCache()
	//Seek to the end of the keys data file
	keyPos, err := s.keysFile.Seek(0, 2)
	if err != nil {
		return err
	}
	//Write the key to the keys data file
	keySize, err := s.keysFile.Write(key)
	if err != nil {
		return err
	}

	//Check the written key size is the same as the key size
	if keySize != len(key) {
		panic("Key size mismatch")
	}

	// Create the tombstone.  This is a negative of the key data position
	keyTombstone := -keyPos

	// Seek to the end of the keys index file
	eofKeyIndex, err := s.keysIndex.Seek(0, 2)
	panicOnError("Seek to end of keys index file", err)

	//Step back 8 bytes to get the last key index, which should also be the keyPos
	keyIndexStart := eofKeyIndex - 8

	// Seek to the last key index
	_, err = s.keysIndex.Seek(keyIndexStart, 0)
	panicOnError("Seek to last key index", err)

	// Write the tombstone
	err = binary.Write(s.keysIndex, binary.BigEndian, keyTombstone)
	//debugf("Wrote tombstone at index %d, position is now %d\n", keyIndexStart/8, keyTombstone)

	// Get the next key index (the end of the key data file).  This should be the same as the length of the key file
	nextKeyPos := keyPos + int64(keySize)
	if nextKeyPos != keyPos+int64(keySize) {
		panic("Key file is corrupt")
	}

	// Get the length of the key data file
	keyFileLength, err := s.keysFile.Seek(0, 2)
	if err != nil {
		return err
	}

	// Check the next key index is the same as the key file length
	if nextKeyPos != keyFileLength {
		panic("Key file is corrupt")
	}

	// Now write the next key index (the end of the key data file)
	s.keysIndex.Seek(0, 2)
	err = binary.Write(s.keysIndex, binary.BigEndian, nextKeyPos)
	if err != nil {
		return err
	}

	//Write the value to the values data file
	valuePos, err := s.valuesFile.Seek(0, 2)
	panicOnError("Seek to end of values file", err)

	tombStoneValue := []byte("DELETED")
	valueSize, err := s.valuesFile.Write(tombStoneValue)
	panicOnError("Write tombstone value", err)

	//Overwrite the current key with a tombstone
	valueTombstone := -valuePos
	debugf("Value tombstone is %d\n", valueTombstone)
	eofValueIndex, err := s.valuesIndex.Seek(0, 2)
	panicOnError("Seek to end of values index file", err)

	valueIndexStart := eofValueIndex - 8
	pos, err := s.valuesIndex.Seek(valueIndexStart, 0)
	panicOnError("Seek to last value index", err)

	if pos != valueIndexStart {
		panic("Seek to last value index failed")
	}

	// Overwrite the last value index with the tombstone
	debugf("Writing value tombstone at index %d, position is now %d\n", valueIndexStart/8, valueTombstone)
	err = binary.Write(s.valuesIndex, binary.BigEndian, valueTombstone)
	panicOnError("Write value tombstone", err)

	// Calculate the end of the value data file
	nextValuePos := valuePos + int64(valueSize)

	// Get the length of the value data file from seek
	valueFileLength, err := s.valuesFile.Seek(0, 2)
	panicOnError("Seek to end of values file", err)

	// Check the next value index is the same as the value file length
	if nextValuePos != valueFileLength {
		panic("Value file is corrupt")
	}

	// Seek to the end of the values index file
	_, err = s.valuesIndex.Seek(0, 2)
	panicOnError("Seek to end of values index file", err)

	// Write the next value index (the end of the value data file)
	debugf("Writing next value at index %d, position is now %d\n", eofValueIndex/8, nextValuePos)
	err = binary.Write(s.valuesIndex, binary.BigEndian, valueFileLength)
	panicOnError("Write to values index file", err)

	debugln("Deleted key", trimTo40(key))

	checkLastIndexEntry(s.keysIndex, s.keysFile)
	checkLastIndexEntry(s.valuesIndex, s.valuesFile)
	checkIndexSigns(s.keysIndex, s.valuesIndex)

	if EnableIndexCaching {
		s.cache.Store(string(key), false)
	}
	return nil
}

func (s *ExtentKeyValStore) doFlush() error {
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
	s.keysIndexCache = nil
	s.valuesIndexCache = nil

	return nil
}

func (s *ExtentKeyValStore) Flush() error {

	return s.doFlush()
}

func (s *ExtentKeyValStore) DumpIndex() error {

	return s.DumpIndexLockFree()
}

func (s *ExtentKeyValStore) DumpIndexLockFree() error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

	s.doFlush()
	debugln("==========DUMPING INDEX==========")
	keyIndexFileLength, err := s.keysIndex.Seek(0, 2)
	panicOnError("Get key index file length", err)

	_, err = s.keysIndex.Seek(0, 0)
	panicOnError("Seek to start of keys index file", err)
	_, err = s.valuesIndex.Seek(0, 0)
	panicOnError("Seek to start of values index file", err)
	entry := int64(0)
	for {
		if entry*8+16 > int64(keyIndexFileLength) {
			break
		}
		var keyPos int64
		_, err := s.keysIndex.Seek(int64(entry)*8, 0)
		panicOnError("Seek to current entry in keys index file", err)
		err = binary.Read(s.keysIndex, binary.BigEndian, &keyPos)
		if err != nil {
			break
		}
		debugf("KEY   Entry: %d, BytePosition: %d, cache length: %d, keyIndexFileLength: %d\n", entry, keyPos, len(s.keysIndexCache), keyIndexFileLength)
		key, deleted, err := s.readDataAtIndexPos(int64(entry*8), s.keysIndex, s.keysFile, s.keysIndexCache)
		panicOnError("Read key data", err)
		debugf("KEY   Entry: %d, BytePosition: %d, Deleted: %t, Key: %s\n", entry, keyPos, deleted, trimTo40(key))

		var valuePos int64
		_, err = s.valuesIndex.Seek(int64(entry)*8, 0)
		panicOnError("Seek to current entry in values index file", err)
		err = binary.Read(s.valuesIndex, binary.BigEndian, &valuePos)
		if err != nil {
			break
		}

		debugf("VALUE Entry: %d, BytePosition: %d\n", entry, valuePos)
		entry++

	}
	debugln("==========END DUMPING INDEX==========")
	checkIndexSigns(s.keysIndex, s.valuesIndex)
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

// searchDbForKeyExists performs a forward scan of the keys index and keys data files.
// For each record, it extracts the key from keys.dat and, if it matches the provided key,
// it updates the “latest” state. In the end, if at least one matching record was found,
// the final state (insertion or deletion) is returned.
func (s *ExtentKeyValStore) searchDbForKeyExists(searchKey []byte, keysIndex *os.File, keysFile *os.File, keysIndexCache []byte) (bool, int64, error) {

	checkLastIndexEntry(s.keysIndex, s.keysFile)
	checkLastIndexEntry(s.valuesIndex, s.valuesFile)
	checkIndexSigns(s.keysIndex, s.valuesIndex)

	keyIndexFileLength, err := s.keysIndex.Seek(0, 2)
	panicOnError("Get key index file length", err)

	_, err = s.keysIndex.Seek(0, 0)
	panicOnError("Seek to start of keys index file", err)
	_, err = s.valuesIndex.Seek(0, 0)
	panicOnError("Seek to start of values index file", err)
	entry := int64(0)
	outFound := false
	outPos := int64(0)

	for {
		if entry*8+8 >= int64(keyIndexFileLength) {
			break
		}
		var keyPos int64
		_, err := s.keysIndex.Seek(int64(entry)*8, 0)
		panicOnError("Seek to current entry in keys index file", err)
		err = binary.Read(s.keysIndex, binary.BigEndian, &keyPos)
		if err != nil {
			break
		}
		//debugf("KEY   Entry: %d, BytePosition: %d\n", entry, keyPos)
		keyData, deleted, err := s.readDataAtIndexPos(int64(entry*8), s.keysIndex, s.keysFile, s.keysIndexCache)
		panicOnError("Read key data", err)
		//debugf("searchDbForKeyExists KEY   Entry: %d, BytePosition: %d, Deleted: %t, Key: %s\n", entry, keyPos, deleted, trimTo40(keyData))

		var valuePos int64
		_, err = s.valuesIndex.Seek(int64(entry)*8, 0)
		panicOnError("Seek to current entry in values index file", err)
		err = binary.Read(s.valuesIndex, binary.BigEndian, &valuePos)
		if err != nil {
			break
		}

		//debugf("searchDbForKeyExists VALUE Entry: %d, BytePosition: %d\n", entry, valuePos)

		if bytes.Equal(keyData, searchKey) {
			//debugf("searchDbForKeyExists: Matched keydata %v to search key %v\n", trimTo40(keyData), trimTo40(searchKey))
			if !deleted {
				outFound = true
				outPos = entry * 8
				//debugf("searchDbForKeyExists: Selected key %s at position %d\n", trimTo40(keyData), outPos)
			} else {
				outFound = false
				outPos = -1
				//debugf("searchDbForKeyExists: Selected key %s at position %d is deleted\n", trimTo40(keyData), outPos)
			}
		}

		entry++

	}
	return outFound, outPos, nil
}

// --- Modify Exists() to use and update the cache counters.
func (s *ExtentKeyValStore) Exists(key []byte) bool {

	s.maybePrintCacheStats() // Increment request counter and print stats every 100th call

	if EnableIndexCaching {
		// If the in‑memory cache is empty, load it completely.
		if s.cache.Len() == 0 {
			if err := s.loadKeyCache(); err != nil {
				// If the cache cannot be loaded, fall back to the previous behavior.
				s.globalLock.Lock()
				defer s.globalLock.Unlock()
				found, _, err := s.searchDbForKeyExists(key, s.keysIndex, s.keysFile, s.keysIndexCache)
				return err == nil && found
			}
		}

		state, exists := s.cache.Load(string(key))
		if exists {
			s.cacheHits++ // found in cache
			return state
		}
		s.cacheMisses++ // not found in cache
	} else {
		s.globalLock.Lock()
		defer s.globalLock.Unlock()
		found, _, err := s.searchDbForKeyExists(key, s.keysIndex, s.keysFile, s.keysIndexCache)
		debugf("err: %v and found: %v\n", err, found)
		return err == nil && found

	}
	return false
}

func (s *ExtentKeyValStore) LockFreeGet(key []byte) ([]byte, error) {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

	checkLastIndexEntry(s.keysIndex, s.keysFile)
	checkLastIndexEntry(s.valuesIndex, s.valuesFile)
	checkIndexSigns(s.keysIndex, s.valuesIndex)

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
		data, deleted, err := s.readDataAtIndexPos(
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
	data, _, err := s.readDataAtIndexPos(
		keyIndexPosStart,
		s.valuesIndex,
		s.valuesFile,
		s.valuesIndexCache,
	)
	if err != nil {
		return nil, fmt.Errorf("LockFreeGetfailed to read value data: %w", err)
	}

	return data, nil
}
