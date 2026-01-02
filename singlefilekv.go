package ensemblekv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
	"sync"
)

// *****************************************************************************
// SingleFileLSM
//
// Entirely vibe coded, I don't trust any part of this, but it /seems/ to work.
// *****************************************************************************

const (
	// File format constants
	singleFileHeaderSize = 4096
	singleFileMagic      = 0x4C534D46 // "LSMF"
	singleFileVersion    = 1

	// WAL constants
	walEntryHeaderSize = 28               // checksum(8) + keyLen(8) + valLen(8) + flags(4)
	walMaxSize         = 64 * 1024 * 1024 // 64MB

	// MemTable constants
	memTableMaxSize = 8 * 1024 * 1024 // 8MB before flush

	// SSTable constants
	sstableMaxSize = 256 * 1024 * 1024 // 256MB per SSTable

	// Size limits - supports very large keys and values
	maxKeySize   = 1024 * 1024 * 1024        // 1GB max key size
	maxValueSize = 1024 * 1024 * 1024 * 1024 // 1TB max value size

	// Tombstone marker
	tombstoneMarker = int64(-1)
)

// SingleFileLSM implements a crash-resistant LSM tree in a single file
type SingleFileLSM struct {
	DefaultOps

	// File management
	file     *os.File
	filePath string
	mutex    sync.RWMutex

	// Header information
	walOffset     int64
	sstableOffset int64
	indexOffset   int64

	// In-memory components
	memTable     map[string][]byte // Active write buffer (nil value = tombstone)
	memTableSize int64

	// SSTable metadata
	sstables []sstableMetadata

	// WAL tracking
	walSize int64

	// Shutdown
	closed bool
}

// sstableMetadata describes an immutable SSTable extent in the file
type sstableMetadata struct {
	offset     int64  // Start offset in file
	size       int64  // Total size in bytes
	minKey     []byte // Smallest key (for range checks)
	maxKey     []byte // Largest key (for range checks)
	numEntries int    // Number of key-value pairs
	checksum   uint32 // Checksum of entire SSTable
}

// walEntry represents a single write-ahead log entry
type walEntry struct {
	checksum uint64
	keyLen   int64
	valLen   int64  // -1 for tombstone
	flags    uint32 // Reserved for future use
	key      []byte
	value    []byte
}

// fileHeader is the fixed-size header at the start of the file
type fileHeader struct {
	magic         uint32
	version       uint32
	walOffset     int64
	sstableOffset int64
	indexOffset   int64
	numSSTables   int32
	checksum      uint32
}

// NewSingleFileLSM creates or opens a single-file LSM store
func NewSingleFileLSM(filePath string, blockSize int64, maxSize int64) (*SingleFileLSM, error) {
	lsm := &SingleFileLSM{
		filePath:      filePath,
		memTable:      make(map[string][]byte),
		memTableSize:  0,
		walOffset:     singleFileHeaderSize,
		sstableOffset: singleFileHeaderSize + walMaxSize,
		walSize:       0,
	}

	// Try to open existing file
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			// Create new file
			file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				return nil, fmt.Errorf("failed to create file: %w", err)
			}

			// Initialize new file
			if err := lsm.initializeNewFile(file); err != nil {
				file.Close()
				return nil, fmt.Errorf("failed to initialize file: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to open file: %w", err)
		}
	}

	lsm.file = file

	// Load existing data and recover from WAL
	if err := lsm.loadFromDisk(); err != nil {
		lsm.file.Close()
		return nil, fmt.Errorf("failed to load from disk: %w", err)
	}

	return lsm, nil
}

// initializeNewFile writes the initial header to a new file
func (lsm *SingleFileLSM) initializeNewFile(file *os.File) error {
	header := make([]byte, singleFileHeaderSize)

	binary.LittleEndian.PutUint32(header[0:4], singleFileMagic)
	binary.LittleEndian.PutUint32(header[4:8], singleFileVersion)
	binary.LittleEndian.PutUint64(header[8:16], uint64(lsm.walOffset))
	binary.LittleEndian.PutUint64(header[16:24], uint64(lsm.sstableOffset))
	binary.LittleEndian.PutUint64(header[24:32], 0) // indexOffset initially 0
	binary.LittleEndian.PutUint32(header[32:36], 0) // numSSTables initially 0

	// Write checksum
	checksum := crc32.ChecksumIEEE(header[0:36])
	binary.LittleEndian.PutUint32(header[36:40], checksum)

	if _, err := file.WriteAt(header, 0); err != nil {
		return err
	}

	return file.Sync()
}

// loadFromDisk loads the header and recovers from WAL
func (lsm *SingleFileLSM) loadFromDisk() error {
	// Read header
	header := make([]byte, singleFileHeaderSize)
	if _, err := lsm.file.ReadAt(header, 0); err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	// Validate header
	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic != singleFileMagic {
		return fmt.Errorf("invalid magic number: got %x, want %x", magic, singleFileMagic)
	}

	version := binary.LittleEndian.Uint32(header[4:8])
	if version != singleFileVersion {
		return fmt.Errorf("unsupported version: %d", version)
	}

	// Verify checksum
	storedChecksum := binary.LittleEndian.Uint32(header[36:40])
	computedChecksum := crc32.ChecksumIEEE(header[0:36])
	if storedChecksum != computedChecksum {
		return fmt.Errorf("header checksum mismatch")
	}

	// Load offsets
	lsm.walOffset = int64(binary.LittleEndian.Uint64(header[8:16]))
	lsm.sstableOffset = int64(binary.LittleEndian.Uint64(header[16:24]))
	lsm.indexOffset = int64(binary.LittleEndian.Uint64(header[24:32]))
	numSSTables := binary.LittleEndian.Uint32(header[32:36])

	// Load SSTable metadata from index
	if err := lsm.loadSSTables(int(numSSTables)); err != nil {
		return fmt.Errorf("failed to load SSTables: %w", err)
	}

	// Replay WAL
	if err := lsm.replayWAL(); err != nil {
		return fmt.Errorf("failed to replay WAL: %w", err)
	}

	// Clear WAL after successful recovery
	lsm.walSize = 0
	if err := lsm.syncHeader(); err != nil {
		return fmt.Errorf("failed to sync header after recovery: %w", err)
	}

	return nil
}

// loadSSTables loads SSTable metadata from the index section
func (lsm *SingleFileLSM) loadSSTables(count int) error {
	if count == 0 || lsm.indexOffset == 0 {
		return nil
	}

	lsm.sstables = make([]sstableMetadata, 0, count)
	offset := lsm.indexOffset

	for i := 0; i < count; i++ {
		// Read SSTable metadata entry
		// Format: offset(8) + size(8) + minKeyLen(4) + maxKeyLen(4) + numEntries(4) + checksum(4)
		metaBuf := make([]byte, 32)
		if _, err := lsm.file.ReadAt(metaBuf, offset); err != nil {
			// If we can't read metadata, skip this SSTable (crash resistance)
			continue
		}

		meta := sstableMetadata{
			offset:     int64(binary.LittleEndian.Uint64(metaBuf[0:8])),
			size:       int64(binary.LittleEndian.Uint64(metaBuf[8:16])),
			numEntries: int(binary.LittleEndian.Uint32(metaBuf[16:20])),
			checksum:   binary.LittleEndian.Uint32(metaBuf[20:24]),
		}

		minKeyLen := int(binary.LittleEndian.Uint32(metaBuf[24:28]))
		maxKeyLen := int(binary.LittleEndian.Uint32(metaBuf[28:32]))

		offset += 32

		// Read min key
		if minKeyLen > 0 {
			meta.minKey = make([]byte, minKeyLen)
			if _, err := lsm.file.ReadAt(meta.minKey, offset); err != nil {
				continue
			}
			offset += int64(minKeyLen)
		}

		// Read max key
		if maxKeyLen > 0 {
			meta.maxKey = make([]byte, maxKeyLen)
			if _, err := lsm.file.ReadAt(meta.maxKey, offset); err != nil {
				continue
			}
			offset += int64(maxKeyLen)
		}

		// Validate SSTable exists and has correct checksum
		if err := lsm.validateSSTable(&meta); err != nil {
			// Skip corrupt SSTables (crash resistance)
			continue
		}

		lsm.sstables = append(lsm.sstables, meta)
	}

	return nil
}

// validateSSTable checks if an SSTable is valid
func (lsm *SingleFileLSM) validateSSTable(meta *sstableMetadata) error {
	// Check if SSTable is within file bounds
	fileInfo, err := lsm.file.Stat()
	if err != nil {
		return err
	}

	if meta.offset+meta.size > fileInfo.Size() {
		return fmt.Errorf("SSTable extends beyond file size")
	}

	// Read and verify checksum
	data := make([]byte, meta.size)
	if _, err := lsm.file.ReadAt(data, meta.offset); err != nil {
		return err
	}

	checksum := crc32.ChecksumIEEE(data)
	if checksum != meta.checksum {
		return fmt.Errorf("SSTable checksum mismatch")
	}

	return nil
}

// replayWAL replays the write-ahead log to recover uncommitted writes
func (lsm *SingleFileLSM) replayWAL() error {
	offset := lsm.walOffset
	maxOffset := lsm.sstableOffset

	for offset < maxOffset {
		entry, bytesRead, err := lsm.readWALEntry(offset)
		if err != nil {
			// End of valid WAL entries
			break
		}

		// Apply entry to memtable
		if entry.valLen == tombstoneMarker {
			lsm.memTable[string(entry.key)] = nil
		} else {
			lsm.memTable[string(entry.key)] = entry.value
			lsm.memTableSize += int64(len(entry.key) + len(entry.value))
		}

		offset += int64(bytesRead)
		lsm.walSize += int64(bytesRead)
	}

	return nil
}

// readWALEntry reads a single WAL entry from the given offset
func (lsm *SingleFileLSM) readWALEntry(offset int64) (*walEntry, int, error) {
	// Read header
	headerBuf := make([]byte, walEntryHeaderSize)
	if _, err := lsm.file.ReadAt(headerBuf, offset); err != nil {
		return nil, 0, err
	}

	entry := &walEntry{
		checksum: binary.LittleEndian.Uint64(headerBuf[0:8]),
		keyLen:   int64(binary.LittleEndian.Uint64(headerBuf[8:16])),
		valLen:   int64(binary.LittleEndian.Uint64(headerBuf[16:24])),
		flags:    binary.LittleEndian.Uint32(headerBuf[24:28]),
	}

	// Validate lengths
	if entry.keyLen <= 0 || entry.keyLen > maxKeySize {
		return nil, 0, fmt.Errorf("invalid key length: %d", entry.keyLen)
	}
	if entry.valLen < -1 || entry.valLen > maxValueSize {
		return nil, 0, fmt.Errorf("invalid value length: %d", entry.valLen)
	}

	// Read key
	entry.key = make([]byte, entry.keyLen)
	if _, err := lsm.file.ReadAt(entry.key, offset+walEntryHeaderSize); err != nil {
		return nil, 0, err
	}

	dataOffset := offset + walEntryHeaderSize + int64(entry.keyLen)
	totalBytes := walEntryHeaderSize + int(entry.keyLen)

	// Read value if not tombstone
	if entry.valLen > 0 {
		entry.value = make([]byte, entry.valLen)
		if _, err := lsm.file.ReadAt(entry.value, dataOffset); err != nil {
			return nil, 0, err
		}
		totalBytes += int(entry.valLen)
	}

	// Verify checksum
	checksumData := append(entry.key, entry.value...)
	computedChecksum := uint64(crc32.ChecksumIEEE(checksumData))
	if computedChecksum != entry.checksum {
		return nil, 0, fmt.Errorf("WAL entry checksum mismatch")
	}

	return entry, totalBytes, nil
}

// syncHeader writes the current header to disk
func (lsm *SingleFileLSM) syncHeader() error {
	header := make([]byte, singleFileHeaderSize)

	binary.LittleEndian.PutUint32(header[0:4], singleFileMagic)
	binary.LittleEndian.PutUint32(header[4:8], singleFileVersion)
	binary.LittleEndian.PutUint64(header[8:16], uint64(lsm.walOffset))
	binary.LittleEndian.PutUint64(header[16:24], uint64(lsm.sstableOffset))
	binary.LittleEndian.PutUint64(header[24:32], uint64(lsm.indexOffset))
	binary.LittleEndian.PutUint32(header[32:36], uint32(len(lsm.sstables)))

	checksum := crc32.ChecksumIEEE(header[0:36])
	binary.LittleEndian.PutUint32(header[36:40], checksum)

	if _, err := lsm.file.WriteAt(header, 0); err != nil {
		return err
	}

	return lsm.file.Sync()
}

// Get retrieves a value for the given key
func (lsm *SingleFileLSM) Get(key []byte) ([]byte, error) {
	lsm.mutex.RLock()
	defer lsm.mutex.RUnlock()

	if lsm.closed {
		return nil, fmt.Errorf("store is closed")
	}

	// Check memtable first
	if val, exists := lsm.memTable[string(key)]; exists {
		if val == nil {
			return nil, fmt.Errorf("key deleted")
		}
		return append([]byte(nil), val...), nil
	}

	// Search SSTables from newest to oldest
	for i := len(lsm.sstables) - 1; i >= 0; i-- {
		meta := &lsm.sstables[i]

		// Check if key could be in this SSTable
		if len(meta.minKey) > 0 && bytes.Compare(key, meta.minKey) < 0 {
			continue
		}
		if len(meta.maxKey) > 0 && bytes.Compare(key, meta.maxKey) > 0 {
			continue
		}

		// Search within SSTable
		val, found, err := lsm.searchSSTable(meta, key)
		if err != nil {
			// Skip corrupt SSTables
			continue
		}
		if found {
			if val == nil {
				return nil, fmt.Errorf("key deleted")
			}
			return val, nil
		}
	}

	return nil, fmt.Errorf("key not found")
}

// searchSSTable searches for a key within a specific SSTable
func (lsm *SingleFileLSM) searchSSTable(meta *sstableMetadata, key []byte) ([]byte, bool, error) {
	offset := meta.offset
	endOffset := meta.offset + meta.size

	for offset < endOffset {
		// Read extent header (checksum(8) + keyLen(8) + valLen(8))
		headerBuf := make([]byte, 24)
		if _, err := lsm.file.ReadAt(headerBuf, offset); err != nil {
			return nil, false, err
		}

		checksum := binary.LittleEndian.Uint64(headerBuf[0:8])
		keyLen := int64(binary.LittleEndian.Uint64(headerBuf[8:16]))
		valLen := int64(binary.LittleEndian.Uint64(headerBuf[16:24]))

		if keyLen <= 0 || keyLen > maxKeySize {
			return nil, false, fmt.Errorf("invalid key length in SSTable")
		}

		// Read key
		entryKey := make([]byte, keyLen)
		if _, err := lsm.file.ReadAt(entryKey, offset+24); err != nil {
			return nil, false, err
		}

		// Check if this is our key
		if bytes.Equal(entryKey, key) {
			if valLen == tombstoneMarker {
				return nil, true, nil
			}

			// Read value
			entryValue := make([]byte, valLen)
			if _, err := lsm.file.ReadAt(entryValue, offset+24+keyLen); err != nil {
				return nil, false, err
			}

			// Verify checksum
			checksumData := append(entryKey, entryValue...)
			computedChecksum := uint64(crc32.ChecksumIEEE(checksumData))
			if computedChecksum != checksum {
				return nil, false, fmt.Errorf("extent checksum mismatch")
			}

			return entryValue, true, nil
		}

		// Move to next extent
		extentSize := int64(24) + keyLen
		if valLen > 0 {
			extentSize += valLen
		}
		offset += extentSize
	}

	return nil, false, nil
}

// Put stores a key-value pair
func (lsm *SingleFileLSM) Put(key []byte, value []byte) error {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()

	if lsm.closed {
		return fmt.Errorf("store is closed")
	}

	// Write to WAL first (crash resistance)
	if err := lsm.appendToWAL(key, value); err != nil {
		return fmt.Errorf("failed to write WAL: %w", err)
	}

	// Add to memtable
	lsm.memTable[string(key)] = append([]byte(nil), value...)
	lsm.memTableSize += int64(len(key) + len(value))

	// Flush if memtable is too large
	if lsm.memTableSize >= memTableMaxSize {
		if err := lsm.flushMemTable(); err != nil {
			return fmt.Errorf("failed to flush memtable: %w", err)
		}
	}

	return nil
}

// Delete marks a key as deleted
func (lsm *SingleFileLSM) Delete(key []byte) error {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()

	if lsm.closed {
		return fmt.Errorf("store is closed")
	}

	// Write tombstone to WAL
	if err := lsm.appendToWAL(key, nil); err != nil {
		return fmt.Errorf("failed to write WAL: %w", err)
	}

	// Add tombstone to memtable
	lsm.memTable[string(key)] = nil

	return nil
}

// appendToWAL appends an entry to the write-ahead log
func (lsm *SingleFileLSM) appendToWAL(key []byte, value []byte) error {
	entry := &walEntry{
		keyLen: int64(len(key)),
		key:    key,
	}

	if value == nil {
		entry.valLen = tombstoneMarker
	} else {
		entry.valLen = int64(len(value))
		entry.value = value
	}

	// Compute checksum
	checksumData := append(key, value...)
	entry.checksum = uint64(crc32.ChecksumIEEE(checksumData))

	// Write header
	headerBuf := make([]byte, walEntryHeaderSize)
	binary.LittleEndian.PutUint64(headerBuf[0:8], entry.checksum)
	binary.LittleEndian.PutUint64(headerBuf[8:16], uint64(entry.keyLen))
	binary.LittleEndian.PutUint64(headerBuf[16:24], uint64(entry.valLen))
	binary.LittleEndian.PutUint32(headerBuf[24:28], entry.flags)

	offset := lsm.walOffset + lsm.walSize
	if _, err := lsm.file.WriteAt(headerBuf, offset); err != nil {
		return err
	}

	// Write key
	if _, err := lsm.file.WriteAt(key, offset+walEntryHeaderSize); err != nil {
		return err
	}

	// Write value if not tombstone
	if value != nil {
		if _, err := lsm.file.WriteAt(value, offset+walEntryHeaderSize+int64(len(key))); err != nil {
			return err
		}
	}

	// Sync to disk (crash resistance)
	if err := lsm.file.Sync(); err != nil {
		return err
	}

	entrySize := int64(walEntryHeaderSize + len(key))
	if value != nil {
		entrySize += int64(len(value))
	}
	lsm.walSize += entrySize

	return nil
}

// flushMemTable writes the memtable to a new SSTable
func (lsm *SingleFileLSM) flushMemTable() error {
	if len(lsm.memTable) == 0 {
		return nil
	}

	// Sort keys
	keys := make([]string, 0, len(lsm.memTable))
	for k := range lsm.memTable {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Calculate SSTable offset
	sstableStart := lsm.sstableOffset
	if len(lsm.sstables) > 0 {
		lastSSTable := &lsm.sstables[len(lsm.sstables)-1]
		sstableStart = lastSSTable.offset + lastSSTable.size
	}

	// Write extents
	offset := sstableStart
	var minKey, maxKey []byte
	numEntries := 0

	buffer := &bytes.Buffer{}

	for _, k := range keys {
		v := lsm.memTable[k]
		keyBytes := []byte(k)

		if minKey == nil {
			minKey = append([]byte(nil), keyBytes...)
		}
		maxKey = append([]byte(nil), keyBytes...)

		// Build extent
		var valLen int64
		var checksumData []byte

		if v == nil {
			valLen = tombstoneMarker
			checksumData = keyBytes
		} else {
			valLen = int64(len(v))
			checksumData = append(keyBytes, v...)
		}

		checksum := uint64(crc32.ChecksumIEEE(checksumData))

		// Write extent header (checksum(8) + keyLen(8) + valLen(8))
		binary.Write(buffer, binary.LittleEndian, checksum)
		binary.Write(buffer, binary.LittleEndian, int64(len(keyBytes)))
		binary.Write(buffer, binary.LittleEndian, valLen)

		// Write key
		buffer.Write(keyBytes)

		// Write value if not tombstone
		if v != nil {
			buffer.Write(v)
		}

		numEntries++
	}

	// Write buffer to file
	sstableData := buffer.Bytes()
	if _, err := lsm.file.WriteAt(sstableData, offset); err != nil {
		return err
	}

	// Sync to disk
	if err := lsm.file.Sync(); err != nil {
		return err
	}

	// Create metadata
	meta := sstableMetadata{
		offset:     sstableStart,
		size:       int64(len(sstableData)),
		minKey:     minKey,
		maxKey:     maxKey,
		numEntries: numEntries,
		checksum:   crc32.ChecksumIEEE(sstableData),
	}

	lsm.sstables = append(lsm.sstables, meta)

	// Update index
	if err := lsm.writeIndex(); err != nil {
		return err
	}

	// Clear memtable and WAL
	lsm.memTable = make(map[string][]byte)
	lsm.memTableSize = 0
	lsm.walSize = 0

	// Update header
	if err := lsm.syncHeader(); err != nil {
		return err
	}

	return nil
}

// writeIndex writes SSTable metadata to the index section
func (lsm *SingleFileLSM) writeIndex() error {
	// Calculate index start
	indexStart := lsm.sstableOffset
	if len(lsm.sstables) > 0 {
		lastSSTable := &lsm.sstables[len(lsm.sstables)-1]
		indexStart = lastSSTable.offset + lastSSTable.size
	}

	lsm.indexOffset = indexStart

	buffer := &bytes.Buffer{}

	for _, meta := range lsm.sstables {
		// Write metadata
		binary.Write(buffer, binary.LittleEndian, uint64(meta.offset))
		binary.Write(buffer, binary.LittleEndian, uint64(meta.size))
		binary.Write(buffer, binary.LittleEndian, uint32(meta.numEntries))
		binary.Write(buffer, binary.LittleEndian, meta.checksum)
		binary.Write(buffer, binary.LittleEndian, uint32(len(meta.minKey)))
		binary.Write(buffer, binary.LittleEndian, uint32(len(meta.maxKey)))

		// Write keys
		buffer.Write(meta.minKey)
		buffer.Write(meta.maxKey)
	}

	// Write to file
	indexData := buffer.Bytes()
	if _, err := lsm.file.WriteAt(indexData, indexStart); err != nil {
		return err
	}

	return lsm.file.Sync()
}

// Exists checks if a key exists
func (lsm *SingleFileLSM) Exists(key []byte) bool {
	_, err := lsm.Get(key)
	return err == nil
}

// Size returns the approximate size of the store
func (lsm *SingleFileLSM) Size() int64 {
	lsm.mutex.RLock()
	defer lsm.mutex.RUnlock()

	info, err := lsm.file.Stat()
	if err != nil {
		return 0
	}

	return info.Size()
}

// Flush ensures all data is written to disk
func (lsm *SingleFileLSM) Flush() error {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()

	if lsm.closed {
		return fmt.Errorf("store is closed")
	}

	if err := lsm.flushMemTable(); err != nil {
		return err
	}

	return lsm.file.Sync()
}

// Close closes the store
func (lsm *SingleFileLSM) Close() error {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()

	if lsm.closed {
		return nil
	}

	// Flush memtable
	if err := lsm.flushMemTable(); err != nil {
		return err
	}

	lsm.closed = true
	return lsm.file.Close()
}

// MapFunc iterates over all key-value pairs
func (lsm *SingleFileLSM) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	lsm.mutex.RLock()
	defer lsm.mutex.RUnlock()

	if lsm.closed {
		return nil, fmt.Errorf("store is closed")
	}

	keys := make(map[string]bool)

	// Iterate over SSTables
	for i := range lsm.sstables {
		meta := &lsm.sstables[i]
		if err := lsm.iterateSSTable(meta, f, keys); err != nil {
			return keys, err
		}
	}

	// Iterate over memtable (overrides SSTable values)
	for k, v := range lsm.memTable {
		keyBytes := []byte(k)
		if v == nil {
			keys[k] = false
		} else {
			if err := f(keyBytes, v); err != nil {
				return keys, err
			}
			keys[k] = true
		}
	}

	return keys, nil
}

// iterateSSTable iterates over all entries in an SSTable
func (lsm *SingleFileLSM) iterateSSTable(meta *sstableMetadata, f func([]byte, []byte) error, keys map[string]bool) error {
	offset := meta.offset
	endOffset := meta.offset + meta.size

	for offset < endOffset {
		// Read extent header (checksum(8) + keyLen(8) + valLen(8))
		headerBuf := make([]byte, 24)
		if _, err := lsm.file.ReadAt(headerBuf, offset); err != nil {
			return err
		}

		checksum := binary.LittleEndian.Uint64(headerBuf[0:8])
		keyLen := int64(binary.LittleEndian.Uint64(headerBuf[8:16]))
		valLen := int64(binary.LittleEndian.Uint64(headerBuf[16:24]))

		if keyLen <= 0 || keyLen > maxKeySize {
			break
		}

		// Read key
		entryKey := make([]byte, keyLen)
		if _, err := lsm.file.ReadAt(entryKey, offset+24); err != nil {
			return err
		}

		keyStr := string(entryKey)

		// Skip if we've already seen this key (newer version in later SSTable or memtable)
		if _, seen := keys[keyStr]; seen {
			extentSize := int64(24) + keyLen
			if valLen > 0 {
				extentSize += valLen
			}
			offset += extentSize
			continue
		}

		if valLen == tombstoneMarker {
			keys[keyStr] = false
			offset += int64(24) + keyLen
			continue
		}

		// Read value
		entryValue := make([]byte, valLen)
		if _, err := lsm.file.ReadAt(entryValue, offset+24+keyLen); err != nil {
			return err
		}

		// Verify checksum
		checksumData := append(entryKey, entryValue...)
		computedChecksum := uint64(crc32.ChecksumIEEE(checksumData))
		if computedChecksum != checksum {
			// Skip corrupt entries
			extentSize := int64(24) + keyLen + valLen
			offset += extentSize
			continue
		}

		// Call function
		if err := f(entryKey, entryValue); err != nil {
			return err
		}
		keys[keyStr] = true

		// Move to next extent
		offset += int64(24) + keyLen + valLen
	}

	return nil
}

// MapPrefixFunc iterates over all key-value pairs with the given prefix
func (lsm *SingleFileLSM) MapPrefixFunc(prefix []byte, f func([]byte, []byte) error) (map[string]bool, error) {
	keys := make(map[string]bool)

	_, err := lsm.MapFunc(func(k, v []byte) error {
		if bytes.HasPrefix(k, prefix) {
			return f(k, v)
		}
		return nil
	})

	return keys, err
}

// DumpIndex prints the index for debugging
func (lsm *SingleFileLSM) DumpIndex() error {
	lsm.mutex.RLock()
	defer lsm.mutex.RUnlock()

	fmt.Printf("SingleFileLSM Index:\n")
	fmt.Printf("  SSTables: %d\n", len(lsm.sstables))
	fmt.Printf("  MemTable entries: %d\n", len(lsm.memTable))
	fmt.Printf("  MemTable size: %d bytes\n", lsm.memTableSize)

	for i, meta := range lsm.sstables {
		fmt.Printf("  SSTable %d:\n", i)
		fmt.Printf("    Offset: %d\n", meta.offset)
		fmt.Printf("    Size: %d bytes\n", meta.size)
		fmt.Printf("    Entries: %d\n", meta.numEntries)
		fmt.Printf("    Min key: %s\n", string(meta.minKey))
		fmt.Printf("    Max key: %s\n", string(meta.maxKey))
	}

	return nil
}

// KeyHistory returns the history of a key (for LSM stores)
func (lsm *SingleFileLSM) KeyHistory(key []byte) ([][]byte, error) {
	lsm.mutex.RLock()
	defer lsm.mutex.RUnlock()

	history := [][]byte{}

	// Check memtable
	if val, exists := lsm.memTable[string(key)]; exists {
		if val != nil {
			history = append(history, append([]byte(nil), val...))
		}
	}

	// Search SSTables from newest to oldest
	for i := len(lsm.sstables) - 1; i >= 0; i-- {
		meta := &lsm.sstables[i]

		val, found, err := lsm.searchSSTable(meta, key)
		if err != nil {
			continue
		}
		if found && val != nil {
			history = append(history, val)
		}
	}

	return history, nil
}

func (s *SingleFileLSM) Keys() [][]byte {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.closed {
		return nil
	}

	keysState := make(map[string]bool)

	// Iterate SSTables from oldest to newest so newer entries win.
	for i := 0; i < len(s.sstables); i++ {
		meta := &s.sstables[i]
		if err := s.iterateSSTable(meta, func(k, v []byte) error {
			keysState[string(k)] = true
			return nil
		}, keysState); err != nil {
			continue
		}
	}

	// Memtable is newest; it overrides SSTables.
	for k, v := range s.memTable {
		if v == nil {
			keysState[k] = false
		} else {
			keysState[k] = true
		}
	}

	keys := make([][]byte, 0, len(keysState))
	for k, exists := range keysState {
		if exists {
			keys = append(keys, []byte(k))
		}
	}
	return keys
}
