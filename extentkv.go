package ensemblekv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/donomii/clusterF/syncmap"
)

var EnableIndexCaching bool = true // Feature flag for index caching
var ExtraChecks bool = false

/*
=======================================================================
EXTENT KEY-VALUE STORE: FORMAT & OPERATION OVERVIEW
=======================================================================

The ExtentKeyValStore is an append-only, file-based key–value store designed for high-throughput writes and efficient lookups. It separates data storage from index management and leverages optional in-memory caching to speed up accesses to index information. This document describes the file formats, data layouts, caching mechanisms, and overall operational workflow for the store, serving as a guide for future maintainers.

-----------------------------------------------------------------------
File Organization & Data Layout
-----------------------------------------------------------------------

There are four primary files managed by the store, all created in a given
directory:

1. **Data Files**
   - **keys.dat**: Holds the raw bytes of all keys, appended sequentially.
   - **values.dat**: Holds the raw bytes of all values, appended sequentially.

   *How Data Files Work:*
   - Every `Put()` operation appends the key to *keys.dat* and then the corresponding value to *values.dat*.
   - The actual position (byte offset) where a key or value is written is determined by the current end-of-file pointer.

2. **Index Files**
   - **keys.index**: Maintains a sequence of 8-byte (int64) BigEndian numbers that indicate absolute end offsets in *keys.dat*.
   - **values.index**: Maintains a similar sequence for *values.dat*.

   *Index File Format & Purpose:*
   - **Initialization:**
     - When first created, an index file is seeded with a single value `0`, representing the initial offset.
   - **Record Boundaries:**
     - Each time data is appended, the store writes a new absolute offset into the corresponding index file.
     - The length of a given record is computed by subtracting the previous index entry from the current one. For example, if a key starts at byte offset 10 and the next index entry is 15, the key's length is 5 bytes.
   - **Tombstones (Deletions):**
     - Deletions are handled logically by marking records as “deleted”.
     - A deletion is recorded by writing a negative offset into the index file.
     - When reading an index entry, a negative value indicates a tombstone, and its absolute value is used to locate the original data in the data file.
	 - The first index in the index file must always point to the 0(zero) offset in the data file.
	 - The last index in the index file must always point to the end of the data file.
	 - If the final index points outside the data file, the index file is assumed corrupt, and the index file is truncated until the last index is within the data file, then the data file is truncated.
	 - If the final index points inside the data file, the crash probably occurred after writing the data and before updating the index, so we truncate the data file to the last index.

-----------------------------------------------------------------------
Caching Mechanism & In-Memory Structures
-----------------------------------------------------------------------

To reduce disk I/O and improve performance, the store incorporates two levels
of caching:

1. **Index Caching:**
   - Controlled by the global flag `EnableIndexCaching`.
   - When enabled, the entire contents of *keys.index* and *values.index* are read into memory (stored in `keysIndexCache` and `valuesIndexCache`).
   - Helper functions (e.g., `readIndexAt`) use the in-memory cache if available, falling back to file I/O if caching is disabled or not yet loaded.
   - The caches are dumped (set to `nil`) on write operations (e.g., `Put()` or `Delete()`) to ensure consistency.

2. **Key Existence Map:**
   - An in-memory map (`map[string]bool`) is maintained to track the presence of keys.
   - A key mapped to `true` indicates that it exists; `false` indicates that it has been deleted (a tombstone).
   - If a key is not present, then the status is unknown and the disk is queried.
   - This map is built via `loadKeyAndOffsetCache()`, which scans the key index and data files using a lock-free mapping function (`LockFreeMapFunc`).
   - This cache allows for a fast existence check in methods such as `Exists()` and also aids in the `Get()` operation.
2.5 **Data Offset Map:**
  - An in-memory map (`map[string][2]int64`) is maintained to track the start and end positions of data in the data file.
  - This map is built mainly from the Get routine, and failures simply incure the standard disk search (which still may hit the other caches).
  - Used to speed up gets, by reducing the disk access to one seek.
3. **Cache Statistics:**
   - The store tracks metrics including cache hits, misses, flushes, and loads.
   - The helper `maybePrintCacheStats()` increments a request counter and prints these statistics periodically, assisting maintainers in monitoring cache performance and diagnosing issues.

-----------------------------------------------------------------------
Operational Workflow
-----------------------------------------------------------------------

1. **Initialization (`NewExtentKeyValueStore`):**
   - The store creates or opens the four files. If the index files do not exist, they are created and initialized with a `0`.
   - It validates index integrity by reading the last index entry and comparing
     it to the actual size of the corresponding data file. If mismatches are
     detected, the index file is truncated to the expected size.
   - Finally, the key existence map is loaded into memory via `loadKeyAndOffsetCache()`,
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
	// if !Debug {
	//fmt.Printf(format, args...)

}

func debugln(args ...interface{}) {
	// if !Debug {
	//fmt.Println(args...)

}

// trimTo40 truncates a byte slice to 40 characters for display purposes
func trimTo40(data []byte) string {
	s := string(data)
	if len(s) > 40 {
		return s[:40] + "..."
	}
	return s
}

// --- Add these new fields to ExtentKeyValStore struct:
type ExtentKeyValStore struct {
	DefaultOps
	keysFile    *os.File   // the keys file holds the key data, concatenated without any delimiters
	valuesFile  *os.File   // the values file holds the value data, concatenated without any delimiters
	keysIndex   *os.File   // the keys index file holds the absolute start offsets of each key in the keys file
	valuesIndex *os.File   // the values index file holds the absolute start offsets of each value in the values file
	blockSize   int64      // Currently unused, required to support interface
	globalLock  sync.Mutex // lock for the entire store

	existsCache      *syncmap.SyncMap[string, bool]     // cache for fast existence checks
	valueOffsetCache *syncmap.SyncMap[string, [2]int64] // cache for fast value offset lookups

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
		if keyOffset == 0 && valueOffset != 0 {
			panic("Key offset is 0 and value offset is not 0")
		}

	}
}

func NewExtentKeyValueStore(directory string, blockSize, filesize int64) (*ExtentKeyValStore, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, fmt.Errorf("create Extent directory %s: %w", directory, err)
	}
	var files [4]*os.File
	opened := false
	defer func() {
		if !opened {
			for _, file := range files {
				if file != nil {
					file.Close()
				}
			}
		}
	}()
	for index, name := range []string{"keys.dat", "values.dat", "keys.index", "values.index"} {
		file, err := os.OpenFile(directory+"/"+name, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, fmt.Errorf("open Extent file %s/%s: %w", directory, name, err)
		}
		files[index] = file
	}
	s := &ExtentKeyValStore{
		keysFile: files[0], valuesFile: files[1], keysIndex: files[2], valuesIndex: files[3], blockSize: blockSize,
	}
	if err := s.recoverRecords(); err != nil {
		return nil, err
	}
	if err := s.loadKeyAndOffsetCache(); err != nil {
		return nil, err
	}
	opened = true
	return s, nil
}

func (s *ExtentKeyValStore) recoverRecords() error {
	indexes := [2]*os.File{s.keysIndex, s.valuesIndex}
	dataFiles := [2]*os.File{s.keysFile, s.valuesFile}
	var indexSizes, dataSizes [2]int64
	for index, file := range indexes {
		stat, err := file.Stat()
		if err != nil {
			return fmt.Errorf("stat Extent index %s: %w", file.Name(), err)
		}
		indexSizes[index] = stat.Size()
		stat, err = dataFiles[index].Stat()
		if err != nil {
			return fmt.Errorf("stat Extent data %s: %w", dataFiles[index].Name(), err)
		}
		dataSizes[index] = stat.Size()
	}
	if indexSizes == [2]int64{} && dataSizes == [2]int64{} {
		for index, file := range indexes {
			if _, err := file.WriteAt(make([]byte, 8), 0); err != nil {
				return fmt.Errorf("initialize Extent index %s: %w", file.Name(), err)
			}
			indexSizes[index] = 8
		}
	}
	for index, file := range indexes {
		if indexSizes[index] < 8 {
			return fmt.Errorf("Extent index %s has %d bytes; expected an initial 8-byte zero offset", file.Name(), indexSizes[index])
		}
	}
	entries := min(indexSizes[0], indexSizes[1]) / 8
	var ends [2]int64
	validEntries := int64(0)
	for entry := int64(0); entry < entries; entry++ {
		var positions [2]int64
		valid := true
		for index, file := range indexes {
			var encoded [8]byte
			if _, err := file.ReadAt(encoded[:], entry*8); err != nil {
				return fmt.Errorf("read Extent index %s at byte %d: %w", file.Name(), entry*8, err)
			}
			position := int64(binary.BigEndian.Uint64(encoded[:]))
			if position < 0 {
				position = -position
			}
			if entry == 0 && position != 0 {
				return fmt.Errorf("Extent index %s starts at %d; expected zero", file.Name(), position)
			}
			if position < 0 || (entry+1 < entries && position < ends[index]) {
				return fmt.Errorf("Extent index %s has invalid offset %d at entry %d after %d", file.Name(), position, entry, ends[index])
			}
			if position < ends[index] || position > dataSizes[index] {
				valid = false
			}
			positions[index] = position
		}
		if !valid {
			break
		}
		ends = positions
		validEntries++
	}
	for index, file := range indexes {
		if err := file.Truncate(validEntries * 8); err != nil {
			return fmt.Errorf("recover Extent index %s to %d entries: %w", file.Name(), validEntries, err)
		}
		// An interrupted deletion can leave a negative EOF boundary for the next insertion.
		end := binary.BigEndian.AppendUint64(nil, uint64(ends[index]))
		if _, err := file.WriteAt(end, (validEntries-1)*8); err != nil {
			return fmt.Errorf("restore Extent EOF in %s: %w", file.Name(), err)
		}
		if err := dataFiles[index].Truncate(ends[index]); err != nil {
			return fmt.Errorf("recover Extent data %s to %d bytes: %w", dataFiles[index].Name(), ends[index], err)
		}
	}
	return nil
}

func (s *ExtentKeyValStore) Keys() [][]byte {
	keysstr, err := s.List()
	panicOnError("List Extent keys", err)
	keys := make([][]byte, len(keysstr))
	for i, k := range keysstr {
		keys[i] = []byte(k)
	}
	return keys
}

// loadKeyAndOffsetCache loads the entire keys.index and keys.dat files into memory,
// processing records in forward order when EnableIndexCaching is true.
func (s *ExtentKeyValStore) loadKeyAndOffsetCache() error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()
	return s.loadKeyAndOffsetCacheLocked()
}

func (s *ExtentKeyValStore) loadKeyAndOffsetCacheLocked() error {
	if !EnableIndexCaching {
		return nil
	}

	if s.existsCache != nil && s.valueOffsetCache != nil {
		//Cache exists, don't reload it unless it is cleared
		return nil
	}

	if s.existsCache == nil {
		s.existsCache = (*syncmap.SyncMap[string, bool])(syncmap.NewSyncMap[string, bool]())
	}

	if s.valueOffsetCache == nil {
		s.valueOffsetCache = (*syncmap.SyncMap[string, [2]int64])(syncmap.NewSyncMap[string, [2]int64]())
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
		keyData, deleted, _, _, err := s.readDataAtIndexPos(int64(entry*8), s.keysIndex, s.keysFile)
		panicOnError("Read key data", err)
		//debugf("searchDbForKeyExists KEY   Entry: %d, BytePosition: %d, Deleted: %t, Key: %s\n", entry, keyPos, deleted, trimTo40(keyData))

		//debugf("searchDbForKeyExists VALUE Entry: %d, BytePosition: %d\n", entry, valuePos)

		s.existsCache.Store(string(keyData), !deleted)

		var valuePos int64
		_, err = s.valuesIndex.Seek(int64(entry)*8, 0)
		panicOnError("Seek to current entry in values index file", err)
		err = binary.Read(s.valuesIndex, binary.BigEndian, &valuePos)
		if err != nil {
			break
		}

		var nextValuePos int64
		_, err = s.valuesIndex.Seek(int64(entry+1)*8, 0)
		panicOnError("Seek to next entry in values index file", err)
		err = binary.Read(s.valuesIndex, binary.BigEndian, &nextValuePos)
		if err != nil {
			break
		}

		if nextValuePos < 0 {
			nextValuePos = -nextValuePos
		}
		if !deleted && valuePos >= 0 {
			s.valueOffsetCache.Store(string(keyData), [2]int64{valuePos, nextValuePos})
		}

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
func (s *ExtentKeyValStore) forwardScanForKey(key []byte) ([]byte, bool, error) {
	checkLastIndexEntry(s.keysIndex, s.keysFile)
	checkLastIndexEntry(s.valuesIndex, s.valuesFile)
	checkIndexSigns(s.keysIndex, s.valuesIndex)

	keysIndexFileLength, err := s.keysIndex.Seek(0, 2)
	panicOnError("Seek to end of keys index file", err)

	found, offset, err := s.searchDbForKeyExists(key, s.keysIndex, s.keysFile)
	if err != nil {
		if EnableIndexCaching {
			s.existsCache.Store(string(key), false)
		}
		return nil, false, fmt.Errorf("extentkv.forwardScanForKey: not found: %w", err)
	}

	if !found {
		if EnableIndexCaching {
			s.existsCache.Store(string(key), false)
		}
		return nil, false, fmt.Errorf("extentkv.forwardScanForKey: key not found or deleted")
	}

	if keysIndexFileLength == offset {
		if EnableIndexCaching {
			s.existsCache.Store(string(key), false)
		}
		return nil, false, fmt.Errorf("extentkv.forwardScanForKey: key not found or deleted because offset is at end of file")
	}

	debugln("forwardScanForKey: Retrieving value for key", trimTo40(key), "at offset", offset)
	checkIndexSigns(s.keysIndex, s.valuesIndex)
	value, deleted, startPos, endPos, err := s.readDataAtIndexPos(offset, s.valuesIndex, s.valuesFile)
	if err != nil {
		return nil, false, fmt.Errorf("extentkv.forwardScanForKey: failed to read value: %w", err)
	}

	if deleted {
		return nil, false, nil
	}

	/* FIXME - something is writing the value positions as negative and flagging the values as deleted, when they should not be
	if deleted {
		return nil, fmt.Errorf("forwardScanForKey: key marked as deleted")
	}
	*/

	if EnableIndexCaching {
		s.existsCache.Store(string(key), true)
		s.valueOffsetCache.Store(string(key), [2]int64{startPos, endPos})
	}
	return value, true, nil
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

	// Scan through all entries
	entry := int64(0)
	for {
		if entry*8+16 > int64(keyIndexFileLength) {
			break
		}

		// Read the key at this entry
		keyData, deleted, _, _, err := s.readDataAtIndexPos(int64(entry*8), s.keysIndex, s.keysFile)
		if err != nil {
			entry++
			continue
		}

		// If this is the key we're looking for and not deleted
		if bytes.Equal(keyData, key) && !deleted {
			// Get the value
			valueData, _, _, _, err := s.readDataAtIndexPos(int64(entry*8), s.valuesIndex, s.valuesFile)
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
	s.globalLock.Lock()
	defer s.globalLock.Unlock()
	s.clearCacheLocked()
}

func (s *ExtentKeyValStore) clearCacheLocked() {
	s.existsCache = nil
	s.valueOffsetCache = nil
}

func (s *ExtentKeyValStore) Put(key, value []byte) error {
	if len(key) == 0 {
		return errors.New("extentkv.Put: empty keys cannot represent deletion offsets")
	}
	err := func() error {
		s.globalLock.Lock()
		defer s.globalLock.Unlock()
		if err := s.loadKeyAndOffsetCacheLocked(); err != nil {
			return err
		}

		checkLastIndexEntry(s.keysIndex, s.keysFile)
		checkLastIndexEntry(s.valuesIndex, s.valuesFile)
		checkIndexSigns(s.keysIndex, s.valuesIndex)

		//Write the value to the values data file
		// Move to end of data file for values.
		origEndOfValuesFile, err := s.valuesFile.Seek(0, 2)
		panicOnError("Seek to end of values file", err)

		//fmt.Printf("Writing value of length %v to values file at offset %v\n", len(value), origEndOfValuesFile)

		err = binary.Write(s.valuesFile, binary.BigEndian, value)
		panicOnError("Write value to values file", err)

		// Get the end of file position for the values file
		newEndOfValuesFile, err := s.valuesFile.Seek(0, 2)
		panicOnError("Seek to end of values data file", err)

		// Check this value matches the calculated end of file
		if origEndOfValuesFile+int64(len(value)) != newEndOfValuesFile {
			panic(fmt.Errorf("valuePos+valueSize (%d) != endOfValuesFile (%d)", origEndOfValuesFile+int64(len(value)), newEndOfValuesFile))
		}

		//
		//
		//
		//
		//
		//
		//
		//
		//
		//
		//
		//
		//
		//
		//
		//
		//
		//
		//

		// Seek to the end of the values index file
		origEndOfValuesIndexFile, err := s.valuesIndex.Seek(0, 2)
		panicOnError("Seek to end of values index file", err)

		//fmt.Printf("Writing end of values file to values index file at offset %v, index %v\n", origEndOfValuesIndexFile, origEndOfValuesIndexFile/8)

		//Write end of value file offset to values index file
		err = binary.Write(s.valuesIndex, binary.BigEndian, newEndOfValuesFile)
		panicOnError("Write end of values file to values index file", err)

		newEndOfValuesIndexFile, err := s.valuesIndex.Seek(0, 2)
		panicOnError("Seek to end of values index file", err)

		if origEndOfValuesIndexFile+8 != newEndOfValuesIndexFile {
			panic(fmt.Errorf("valuesIndexPos+8 != newEndOfValuesIndexFile (%d != %d)", origEndOfValuesIndexFile+8, newEndOfValuesIndexFile))
		}

		// --- Now we write the key

		// Seek to the end of the keys data file
		origEndOfKeysFile, err := s.keysFile.Seek(0, 2)
		panicOnError("Seek to end of keys data file", err)

		// Write the key to the keys data file.
		keySize, err := s.keysFile.Write(key)
		panicOnError("Write key to keys data file", err)

		// Check we wrote the correct number of bytes
		if keySize != len(key) {
			panic("Key size mismatch")
		}

		// Get the end of file position for the keydata file
		newEndOfKeyDataFile, err := s.keysFile.Seek(0, 2)
		panicOnError("Seek to end of keys data file", err)

		// Check this key matches the calculated end of file
		if origEndOfKeysFile+int64(keySize) != newEndOfKeyDataFile {
			panic(fmt.Errorf("origEndOfKeysFile+keySize (%d) != newEndOfKeyDataFile (%d)", origEndOfKeysFile+int64(keySize), newEndOfKeyDataFile))
		}

		//
		//
		//
		//
		//
		//
		//
		//
		//
		//
		//
		//
		//
		//
		//
		// Seek to the end of the keys index
		origEndOfKeysIndexFile, err := s.keysIndex.Seek(0, 2)
		panicOnError("Seek to end of keys index file", err)

		// Write the end of the file position to the keys index.
		err = binary.Write(s.keysIndex, binary.BigEndian, newEndOfKeyDataFile)
		panicOnError("Write to keys index file", err)

		newEndOfKeysIndexFile, err := s.keysIndex.Seek(0, 2)
		panicOnError("Seek to end of keys index file", err)

		if origEndOfKeysIndexFile+8 != newEndOfKeysIndexFile {
			panic(fmt.Errorf("keysIndexPos+8 != newEndOfKeysIndexFile (%d != %d)", origEndOfKeysIndexFile+8, newEndOfKeysIndexFile))
		}

		checkLastIndexEntry(s.keysIndex, s.keysFile)
		checkLastIndexEntry(s.valuesIndex, s.valuesFile)
		checkIndexSigns(s.keysIndex, s.valuesIndex)
		if EnableIndexCaching {
			s.existsCache.Store(string(key), true)
			s.valueOffsetCache.Store(string(key), [2]int64{origEndOfValuesFile, newEndOfValuesFile})
		}
		return nil
	}()
	if err != nil {
		return err
	}
	/*
		data, err := s.Get(key)
		panicOnError("get", err)
		if !bytes.Equal(data, value) {
			goof.Panicf("put failed: expected %v, got %v", value, data)
		}*/
	return nil
}

// Attempt to read n bytes from a file, fail if we can't read them all within 100 attempts
func readNbytes(file *os.File, n int) ([]byte, error) {
	buffer := make([]byte, n)
	totalRead := 0
	countReads := 0
	for totalRead < n {
		n, err := file.Read(buffer[totalRead:])
		if err != nil {
			pos, _ := file.Seek(0, 1)
			size, _ := file.Seek(0, 2)
			return nil, fmt.Errorf("Reading at buffer offset %v, bytes read %v, file pos: %v, file size: %v, buffer size: %v, error: %v", totalRead, n, pos, size, len(buffer), err)
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
	s.globalLock.Lock()
	defer s.globalLock.Unlock()
	if err := s.loadKeyAndOffsetCacheLocked(); err != nil {
		return nil, err
	}
	s.maybePrintCacheStats() // Increment request counter and print stats every 100th call
	checkLastIndexEntry(s.keysIndex, s.keysFile)
	checkLastIndexEntry(s.valuesIndex, s.valuesFile)
	checkIndexSigns(s.keysIndex, s.valuesIndex)

	if EnableIndexCaching {

		state, exists := s.existsCache.Load(string(key))
		if exists {
			s.cacheHits++ // key was found in the cache map (even though we still go to disk for value)
			if !state {
				return nil, errors.New("key marked as not present in cache")
			}
		} else {
			s.cacheMisses++
		}

		if s.valueOffsetCache == nil {
			s.valueOffsetCache = (*syncmap.SyncMap[string, [2]int64])(syncmap.NewSyncMap[string, [2]int64]())
		}

		valueOffsets, exists := s.valueOffsetCache.Load(string(key))
		if exists {
			s.cacheHits++ // key was found in the cache map (even though we still go to disk for value)
			start := valueOffsets[0]
			end := valueOffsets[1]
			if end < 0 {
				end = end * -1
			}
			if start < 0 {
				return nil, errors.New("key marked as not present in cache")
			}
			if _, err := s.valuesFile.Seek(start, 0); err != nil {
				return nil, fmt.Errorf("seek Extent value for key %x in %s at %d: %w", key, s.valuesFile.Name(), start, err)
			}
			data, err := readNbytes(s.valuesFile, int(end-start))
			if err != nil {
				return nil, fmt.Errorf("read Extent value for key %x in %s from %d to %d: %w", key, s.valuesFile.Name(), start, end, err)
			}
			return data, nil
		}
	}

	val, found, err := s.forwardScanForKey(key) // We should add a found return value
	if err != nil {
		return nil, fmt.Errorf("extentkv.Get: %w", err)
	}
	if !found {
		return nil, fmt.Errorf("extentkv.Get: key not found")
	}

	return val, nil
}

// 1. Read the index file at position indexPosition to get dataPos
// 2. Read the next index file at position (indexPosition + 8) to get the end of the block in the data file
// 3. Read the data file from dataPos to end of block

// Modify readDataAtIndexPos to use caching
// Returns:
// data: the data read from the data file
// deleted: true if the key was deleted
// dataPos: the absolute position of the data in the data file
// nextDataPos: the absolute position of the next data in the data file
// err: any error that occurred
func (s *ExtentKeyValStore) readDataAtIndexPos(indexPosition int64, indexFile *os.File, dataFile *os.File) ([]byte, bool, int64, int64, error) {
	//fmt.Printf("readDataAtIndexPos %v: index is %d\n", time.Now(), indexPosition/8)
	dataFileLength, err := dataFile.Seek(0, 2)
	if err != nil {
		return nil, false, 0, 0, fmt.Errorf("read Extent data size for %s: %w", dataFile.Name(), err)
	}
	indexFileLength, err := indexFile.Seek(0, 2)
	if err != nil {
		return nil, false, 0, 0, fmt.Errorf("read Extent index size for %s: %w", indexFile.Name(), err)
	}
	deleted := false
	var dataPos int64 = 0

	if indexFileLength == indexPosition+8 {
		panic(fmt.Errorf("extentkv.readDataAtIndexPos: invalid index position, data file length is %d, and position is %d (this is the end of file marker, you read one too far)", indexFileLength, indexPosition))
	}

	if indexPosition+16 > indexFileLength {
		return nil, false, 0, 0, fmt.Errorf("extentkv.readDataAtIndexPos: invalid index position %v greater than indexfile of length %v", indexPosition, indexFileLength)
	}

	_, err = indexFile.Seek(indexPosition, 0)
	if err != nil {
		return nil, false, 0, 0, fmt.Errorf("extentkv.readDataAtIndexPos: failed to seek to index position: %w", err)
	}
	//Read the offset to the data
	err = binary.Read(indexFile, binary.BigEndian, &dataPos)
	//fmt.Printf("readDataAtIndexPos: fromt disk dataPos is %d or %v\n", dataPos, dataPos)

	if err != nil {
		return nil, false, 0, 0, fmt.Errorf("extentkv.readDataAtIndexPos: failed to read data offset at index position: %v, length %v, file length %v, %v", indexPosition, 8, indexFileLength, err)
	}

	if dataPos < 0 {
		deleted = true
		dataPos = -dataPos
	}

	// Read next index position
	var nextDataPos int64

	_, err = indexFile.Seek(indexPosition+8, 0)
	if err != nil {
		return nil, false, 0, 0, fmt.Errorf("extentkv.readDataAtIndexPos: failed to seek to data position: %w", err)
	}
	// Read the offset to the next data block (or EOF offset i.e. file length.  The last index entry always points to the end of the data file)
	err = binary.Read(indexFile, binary.BigEndian, &nextDataPos)
	//fmt.Printf("readDataAtIndexPos: fromt disk nextDataPos is %d or %v\n", nextDataPos, nextDataPos)

	if err != nil {
		return nil, false, 0, 0, fmt.Errorf("extentkv.readDataAtIndexPos: failed to read next index position: %w", err)
	}

	if nextDataPos < 0 {
		nextDataPos = -nextDataPos
	}

	if nextDataPos > dataFileLength {
		return nil, false, 0, 0, fmt.Errorf("extentkv.readDataAtIndexPos: invalid next data position %v for datafile of length %v", nextDataPos, dataFileLength)
	}

	size := nextDataPos - dataPos
	if size < 0 {
		return nil, false, 0, 0, fmt.Errorf("extentkv.readDataAtIndexPos: invalid size %v for dataPos %v and nextDataPos %v (indexPos=%d dataLen=%d)", size, dataPos, nextDataPos, indexPosition, dataFileLength)
	}

	//fmt.Printf("readDataAtIndexPos: index: %d, size is %d, dataPos is %d, nextDataPos is %d, deleted %v\n", indexPosition/8, size, dataPos, nextDataPos, deleted)

	//Don't read if the size is 0, we already know what the contents will be :D
	if size == 0 {
		//fmt.Printf("readDataAtIndexPos: size is 0, returning nil (indexPos=%d position=%d nextPos=%d deleted=%v)\n", indexPosition, dataPos, nextDataPos, deleted)
		return nil, deleted, dataPos, nextDataPos, nil
	}

	buffer := make([]byte, size)
	_, err = dataFile.Seek(dataPos, 0)
	if err != nil {
		return nil, false, 0, 0, fmt.Errorf("extentkv.readDataAtIndexPos: failed to seek to data position: %w", err)
	}
	_, err = io.ReadFull(dataFile, buffer)
	if err != nil {
		return nil, false, 0, 0, fmt.Errorf("extentkv.readDataAtIndexPos: failed to read data: %w", err)
	}

	return buffer, deleted, dataPos, nextDataPos, nil
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
	s.globalLock.Lock()
	defer s.globalLock.Unlock()
	if err := s.loadKeyAndOffsetCacheLocked(); err != nil {
		return nil, err
	}
	keys := make([]string, 0)
	if EnableIndexCaching {
		for _, key := range s.existsCache.Keys() {
			if exists, _ := s.existsCache.Load(key); exists {
				keys = append(keys, key)
			}
		}
		return keys, nil
	}
	stat, err := s.keysIndex.Stat()
	if err != nil {
		return nil, fmt.Errorf("list Extent keys in %s: %w", s.keysIndex.Name(), err)
	}
	states := make(map[string]bool)
	for offset := int64(0); offset+16 <= stat.Size(); offset += 8 {
		key, deleted, _, _, err := s.readDataAtIndexPos(offset, s.keysIndex, s.keysFile)
		if err != nil {
			return nil, err
		}
		states[string(key)] = !deleted
	}
	for key, exists := range states {
		if exists {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (s *ExtentKeyValStore) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	out := make(map[string]bool)
	keys, err := s.List()
	if err != nil {
		return out, err
	}
	for _, key := range keys {
		value, err := s.Get([]byte(key))
		if err == nil {
			if err := f([]byte(key), value); err != nil {
				return out, err
			}
			out[key] = true
		} else {
			return out, err
		}
	}

	return out, nil
}

func (s *ExtentKeyValStore) Delete(key []byte) error {
	if len(key) == 0 {
		return errors.New("extentkv.Delete: empty keys cannot represent deletion offsets")
	}
	s.globalLock.Lock()
	defer s.globalLock.Unlock()
	if err := s.loadKeyAndOffsetCacheLocked(); err != nil {
		return err
	}
	if EnableIndexCaching {
		if exists, _ := s.existsCache.Load(string(key)); !exists {
			return nil
		}
	} else {
		found, _, err := s.searchDbForKeyExists(key, s.keysIndex, s.keysFile)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
	}
	s.clearCacheLocked()

	checkLastIndexEntry(s.keysIndex, s.keysFile)
	checkLastIndexEntry(s.valuesIndex, s.valuesFile)
	checkIndexSigns(s.keysIndex, s.valuesIndex)

	//Write the value to the values data file
	// Move to end of data file for values.
	origEndofValuesFile, err := s.valuesFile.Seek(0, 2)
	panicOnError("Seek to end of values file", err)

	valueBytes := []byte("DELETED")
	err = binary.Write(s.valuesFile, binary.BigEndian, valueBytes)
	panicOnError("Write tombstone value to values file", err)

	// Get the end of file position for the values file
	newEndOfValuesFile, err := s.valuesFile.Seek(0, 2)
	panicOnError("Seek to end of values data file", err)

	// Check this value matches the calculated end of file
	if origEndofValuesFile+int64(len(valueBytes)) != newEndOfValuesFile {
		panic(fmt.Errorf("valuePos+valueSize (%d) != endOfValuesFile (%d)", origEndofValuesFile+int64(len(valueBytes)), newEndOfValuesFile))
	}

	//Overwrite the current key with a tombstone
	valueTombstone := -origEndofValuesFile
	debugf("Value tombstone is %d\n", valueTombstone)

	endOfValuesIndexFile, err := s.valuesIndex.Seek(0, 2)
	panicOnError("Seek to end of values index file", err)

	// Write the (negative) end of the file position to the values index.
	valueIndexStart := endOfValuesIndexFile - 8
	pos, err := s.valuesIndex.Seek(valueIndexStart, 0)
	panicOnError("Seek to last value index (end -8)", err)

	if pos != valueIndexStart {
		panic("Seek to last value index failed")
	}

	// Overwrite the last value index with the tombstone
	err = binary.Write(s.valuesIndex, binary.BigEndian, valueTombstone)
	panicOnError("Write value tombstone", err)

	// Seek to the end of the values index file
	_, err = s.valuesIndex.Seek(0, 2)
	panicOnError("Seek to end of values index file", err)

	// Write the next value index (the end of the value data file)
	err = binary.Write(s.valuesIndex, binary.BigEndian, newEndOfValuesFile)
	panicOnError("Write to values index file", err)

	// --- Now we write the key

	//Seek to the end of the keys data file
	origEndOfKeyDataFile, err := s.keysFile.Seek(0, 2)
	panicOnError("Seek to end of keys data file", err)

	//Write the key to the keys data file
	err = binary.Write(s.keysFile, binary.BigEndian, key)
	panicOnError("Write key to keys data file", err)

	newEndOfKeyDataFile, err := s.keysFile.Seek(0, 2)
	panicOnError("Seek to end of keys data file", err)

	// Check this key matches the calculated end of file
	if origEndOfKeyDataFile+int64(len(key)) != newEndOfKeyDataFile {
		panic(fmt.Errorf("keyDataPos+keySize (%d) != endOfKeysFile (%d)", origEndOfKeyDataFile+int64(len(key)), newEndOfKeyDataFile))
	}

	// Create the tombstone.  This is a negative of the key data position
	keyTombstoneOffset := -origEndOfKeyDataFile
	debugf("key tombstone is %v\n", keyTombstoneOffset)

	// Seek to the end of the keys index file
	eofKeyIndex, err := s.keysIndex.Seek(0, 2)
	panicOnError("Seek to end of keys index file", err)

	//Step back 8 bytes to get the last key index, which should also be the keyPos
	keyIndexStart := eofKeyIndex - 8

	// Seek to the last key index
	_, err = s.keysIndex.Seek(keyIndexStart, 0)
	panicOnError("Seek to last key index", err)

	// Overwrite the end of file index pointer with the tombstone
	err = binary.Write(s.keysIndex, binary.BigEndian, keyTombstoneOffset)

	// Now write the next key index (offset to the end of the key data file)
	// Seek to the end of the keys index
	_, err = s.keysIndex.Seek(0, 2)
	panicOnError("Seek to end of keys index file", err)
	// Write the end of the file position to the keys index.
	err = binary.Write(s.keysIndex, binary.BigEndian, newEndOfKeyDataFile)
	panicOnError("Write to keys index file", err)

	debugln("Deleted key", trimTo40(key))

	checkLastIndexEntry(s.keysIndex, s.keysFile)
	checkLastIndexEntry(s.valuesIndex, s.valuesFile)
	checkIndexSigns(s.keysIndex, s.valuesIndex)

	s.clearCacheLocked()
	return nil
}

func (s *ExtentKeyValStore) doFlush() error {

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

	return err
}

func (s *ExtentKeyValStore) Flush() error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()
	return s.doFlush()
}

func (s *ExtentKeyValStore) DumpIndex() error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()
	return s.DumpIndexLockFree()
}

func (s *ExtentKeyValStore) DumpIndexLockFree() error {

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
		debugf("KEY   Entry: %d, BytePosition: %d, keyIndexFileLength: %d\n", entry, keyPos, keyIndexFileLength)
		key, deleted, _, _, err := s.readDataAtIndexPos(int64(entry*8), s.keysIndex, s.keysFile)
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
func (s *ExtentKeyValStore) searchDbForKeyExists(searchKey []byte, keysIndex *os.File, keysFile *os.File) (bool, int64, error) {

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
		panicOnError("Read key position from keys index file", err)
		//debugf("KEY   Entry: %d, BytePosition: %d\n", entry, keyPos)
		keyData, deleted, _, _, err := s.readDataAtIndexPos(int64(entry*8), s.keysIndex, s.keysFile)
		panicOnError("Read key data from keys index file", err)
		//fmt.Printf("Read key data(%v-%v) deleted: %v : %v \n", start, end, deleted, err)
		//debugf("searchDbForKeyExists KEY   Entry: %d, BytePosition: %d, Deleted: %t, Key: %s\n", entry, keyPos, deleted, trimTo40(keyData))

		//fmt.Printf("searchDbForKeyExists: keyData: %v, searchKey: %v\n", trimTo40(keyData), trimTo40(searchKey))
		if bytes.Equal(keyData, searchKey) {
			//debugf("searchDbForKeyExists: Matched keydata %v to search key %v\n", trimTo40(keyData), trimTo40(searchKey))
			if !deleted {
				outFound = true
				outPos = entry * 8
				debugf("searchDbForKeyExists: Selected key %s at position %d found\n", trimTo40(keyData), outPos)
			} else {
				outFound = false
				outPos = -1
				debugf("searchDbForKeyExists: Selected key %s at position %d is deleted\n", trimTo40(keyData), outPos)
			}
		}

		entry++

	}
	return outFound, outPos, nil
}

// --- Modify Exists() to use and update the cache counters.
func (s *ExtentKeyValStore) Exists(key []byte) bool {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

	s.maybePrintCacheStats() // Increment request counter and print stats every 100th call

	if EnableIndexCaching {
		if err := s.loadKeyAndOffsetCacheLocked(); err != nil {
			return false
		}
		state, exists := s.existsCache.Load(string(key))
		if exists {
			s.cacheHits++ // found in cache
			return state
		}
		s.cacheMisses++ // not found in cache
	} else {
		found, _, err := s.searchDbForKeyExists(key, s.keysIndex, s.keysFile)
		debugf("err: %v and found: %v\n", err, found)
		return err == nil && found

	}
	return false
}

func (s *ExtentKeyValStore) LockFreeGet(key []byte) ([]byte, error) {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()
	if err := s.loadKeyAndOffsetCacheLocked(); err != nil {
		return nil, err
	}
	found := false

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

	for {
		keyIndexPosStart = keyIndexPosStart - 8
		if keyIndexPosStart < 0 {
			return nil, fmt.Errorf("key not found after searching to start of file")
		}

		// Read key data using cache if available
		data, deleted, _, _, err := s.readDataAtIndexPos(
			keyIndexPosStart,
			s.keysIndex,
			s.keysFile,
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
	data, _, startPos, endPos, err := s.readDataAtIndexPos(
		keyIndexPosStart,
		s.valuesIndex,
		s.valuesFile,
	)
	if err != nil {
		return nil, fmt.Errorf("LockFreeGetfailed to read value data: %w", err)
	}
	if EnableIndexCaching {
		s.valueOffsetCache.Store(string(key), [2]int64{startPos, endPos})
	}

	return data, nil
}

func (s *ExtentKeyValStore) MapPrefixFunc(prefix []byte, f func([]byte, []byte) error) (map[string]bool, error) {
	// ExtentKeyValStore doesn't have native prefix support, so filter through MapFunc
	keys := make(map[string]bool)
	_, err := s.MapFunc(func(k, v []byte) error {
		if bytes.HasPrefix(k, prefix) {
			keys[string(k)] = true
			return f(k, v)
		}
		return nil
	})
	return keys, err
}
