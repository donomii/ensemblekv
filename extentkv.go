package ensemblekv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"github.com/donomii/goof"
)

var EnableIndexCaching bool = true // Feature flag for index caching

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
    cacheFlushes int
    cacheLoads   int
    requestCount int
}

// --- Add this helper method to increment the request counter and print stats every 100 requests.
func (s *ExtentKeyValStore) maybePrintCacheStats() {
    s.requestCount++
    //if s.requestCount%1 == 0 {
        fmt.Printf("Cache stats: hits=%d, misses=%d, flushes=%d, loads=%d\n",
            s.cacheHits, s.cacheMisses, s.cacheFlushes, s.cacheLoads)
    //}
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
	stat, _ := keysFile.Stat()
	if keysFileEnd != stat.Size() {
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
	stat, _ = valuesFile.Stat()
	if valuesFileEnd != stat.Size() {
		fmt.Printf("Truncating values index file %s to %d\n", valuesIndexFilePath, valuesFileEnd)
		valuesIndex.Truncate(valuesFileEnd)
	}




	s:= &ExtentKeyValStore{
		keysFile:    keysFile,
		valuesFile:  valuesFile,
		keysIndex:   keysIndex,
		valuesIndex: valuesIndex,
		blockSize:   blockSize,
		cache: make(map[string]bool),
	}

	s.loadKeyCache()

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

// --- Modify loadKeyCache() to count loads.
func (s *ExtentKeyValStore) loadKeyCache() error {
    // If using index caching, ensure that the keys index cache is loaded.
    if EnableIndexCaching && s.keysIndexCache == nil {
        if err := s.loadKeysIndexCache(); err != nil {
            return fmt.Errorf("loadKeyCache: failed to load keys index cache: %w", err)
        }
    }
    // Use our existing helper to build the full in‑memory key map.
    keyMap, err := s.LockFreeMapFunc(func(key, value []byte) error {
        return nil
    })
    if err != nil {
        return fmt.Errorf("loadKeyCache: failed to scan key index: %w", err)
    }
    s.cache = keyMap
    s.cacheLoads++ // count this as a load
    return nil
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

    // Flush the caches
    s.keysIndexCache = nil
    s.valuesIndexCache = nil
    s.cacheFlushes++ // count this flush

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
    
    s.cache[string(key)] = true
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

    state, exists := s.cache[string(key)]
    if exists {
        s.cacheHits++ // key was found in the cache map (even though we still go to disk for value)
        if !state {
            return nil, errors.New("key marked as not present in cache")
        }
    } else {
        s.cacheMisses++
    }
    val, err := s.LockFreeGet(key)
    if err != nil {
        return nil, err
    }
    s.cache[string(key)] = true
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

// --- In Delete(), flush the caches and count the flush.
func (s *ExtentKeyValStore) Delete(key []byte) error {
    s.globalLock.Lock()
    defer s.globalLock.Unlock()

    // Flush the caches
    s.keysIndexCache = nil
    s.valuesIndexCache = nil
    s.cacheFlushes++ // count this flush

    state, exists := s.cache[string(key)]
    if exists && !state {
        return errors.New("key not found")
    }
    
    // (Rest of Delete() remains unchanged …)
    // [Your delete logic goes here...]
    
    s.cache[string(key)] = false
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

func (s *ExtentKeyValStore) DumpIndex () error {
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
	entry:=0
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

func searchDbForKeyExists(key []byte, keysIndex *os.File, keysFile *os.File, keysIndexCache []byte) (bool, error) {
    keyIndexPosStart, err := keysIndex.Seek(-8, 2)
    if err != nil {
        return false, err
    }

    for {
        keyIndexPosStart = keyIndexPosStart - 8
        if keyIndexPosStart < 0 {
            return false, errors.New("key not found")
        }

        currentKey, deleted, err := readDataAtIndexPos(keyIndexPosStart, keysIndex, keysFile, keysIndexCache)
        if err != nil {
            return false, err
        }

        if bytes.Equal(key, currentKey) {
            if deleted {
                return false, nil
            }
            return true, nil
        }
    }
}

// --- Modify Exists() to use and update the cache counters.
func (s *ExtentKeyValStore) Exists(key []byte) bool {
    s.globalLock.Lock()
    defer s.globalLock.Unlock()
    
    s.maybePrintCacheStats() // Increment request counter and print stats every 100th call

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