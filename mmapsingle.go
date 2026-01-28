package ensemblekv

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// MmapSingleKV is a crash-resistant key-value store using a single mmap'd append-only log file.
// It minimizes seeks for spinning disk friendliness and can recover from crashes.
type MmapSingleKV struct {
	DefaultOps
	directory string
	file      *os.File
	data      []byte
	index     map[string]int64
	mutex     sync.RWMutex
	offset    int64
	fileSize  int64
}

const (
	initialFileSize = 64 * 1024 * 1024 // 64MB initial size
	growthFactor    = 2
	headerSize      = 16 // 8 (timestamp) + 4 (key_len) + 4 (value_len)
	deletedMarker   = uint32(0xFFFFFFFF)
)

// Entry format:
// [8 bytes: timestamp (int64)]
// [4 bytes: key_length (uint32)]
// [4 bytes: value_length (uint32)] - 0xFFFFFFFF for deletes
// [key_bytes]
// [value_bytes]

func NewMmapSingleKV(directory string, blockSize, fileSize int64) (*MmapSingleKV, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	kv := &MmapSingleKV{
		directory: directory,
		index:     make(map[string]int64),
		fileSize:  initialFileSize,
	}

	if fileSize > 0 {
		kv.fileSize = fileSize
	}

	dataPath := filepath.Join(directory, "data.log")

	// Open or create the data file
	file, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}
	kv.file = file

	// Get current file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	existingSize := stat.Size()
	if existingSize == 0 {
		// New file, initialize with initial size
		if err := file.Truncate(kv.fileSize); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to initialize file size: %w", err)
		}
		existingSize = kv.fileSize
	}

	// mmap the file
	data, err := syscall.Mmap(int(file.Fd()), 0, int(existingSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to mmap file: %w", err)
	}
	kv.data = data

	// Recover from existing data
	if err := kv.recover(); err != nil {
		syscall.Munmap(kv.data)
		file.Close()
		return nil, fmt.Errorf("failed to recover: %w", err)
	}

	return kv, nil
}

func (kv *MmapSingleKV) recover() error {
	offset := int64(0)
	maxValidOffset := int64(0)

	for offset < int64(len(kv.data)) {
		// Check if we have enough bytes for a header
		if offset+headerSize > int64(len(kv.data)) {
			break
		}

		// Read header using unsafe for speed
		timestamp := *(*int64)(unsafe.Pointer(&kv.data[offset]))
		keyLen := *(*uint32)(unsafe.Pointer(&kv.data[offset+8]))
		valueLen := *(*uint32)(unsafe.Pointer(&kv.data[offset+12]))

		// Sanity checks
		if timestamp == 0 && keyLen == 0 && valueLen == 0 {
			// Empty region, we've reached the end of valid data
			break
		}

		// Check for reasonable sizes
		if keyLen > 32768 || (valueLen != deletedMarker && valueLen > 1073741822) {
			// Invalid entry, stop here
			break
		}

		entrySize := int64(headerSize + keyLen)
		if valueLen != deletedMarker {
			entrySize += int64(valueLen)
		}

		// Check if we have enough data
		if offset+entrySize > int64(len(kv.data)) {
			break
		}

		// Read key
		keyStart := offset + headerSize
		key := kv.data[keyStart : keyStart+int64(keyLen)]

		// Valid entry, update index
		if valueLen == deletedMarker {
			delete(kv.index, string(key))
		} else {
			kv.index[string(key)] = offset
		}

		maxValidOffset = offset + entrySize
		offset = maxValidOffset
	}

	kv.offset = maxValidOffset

	return nil
}

func (kv *MmapSingleKV) ensureSpace(needed int64) error {
	if kv.offset+needed <= int64(len(kv.data)) {
		return nil
	}

	// Need to grow the file
	newSize := int64(len(kv.data)) * growthFactor
	for newSize < kv.offset+needed {
		newSize *= growthFactor
	}

	// Unmap current mapping
	if err := syscall.Munmap(kv.data); err != nil {
		return fmt.Errorf("failed to munmap: %w", err)
	}

	// Grow the file
	if err := kv.file.Truncate(newSize); err != nil {
		return fmt.Errorf("failed to truncate file: %w", err)
	}

	// Remap
	data, err := syscall.Mmap(int(kv.file.Fd()), 0, int(newSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to remap file: %w", err)
	}

	kv.data = data
	return nil
}

func (kv *MmapSingleKV) writeEntry(key, value []byte, isDelete bool) error {
	timestamp := nowNanos()
	keyLen := uint32(len(key))
	valueLen := uint32(len(value))
	if isDelete {
		valueLen = deletedMarker
	}

	entrySize := int64(headerSize + keyLen)
	if !isDelete {
		entrySize += int64(valueLen)
	}

	if err := kv.ensureSpace(entrySize); err != nil {
		return err
	}

	offset := kv.offset

	// Write header using unsafe for speed
	*(*int64)(unsafe.Pointer(&kv.data[offset])) = timestamp
	*(*uint32)(unsafe.Pointer(&kv.data[offset+8])) = keyLen
	*(*uint32)(unsafe.Pointer(&kv.data[offset+12])) = valueLen

	// Write key
	copy(kv.data[offset+headerSize:offset+headerSize+int64(keyLen)], key)

	// Write value if not delete
	if !isDelete {
		valueOffset := offset + headerSize + int64(keyLen)
		copy(kv.data[valueOffset:valueOffset+int64(valueLen)], value)
	}

	// Update offset
	kv.offset += entrySize

	// Update index
	if isDelete {
		delete(kv.index, string(key))
	} else {
		kv.index[string(key)] = offset
	}

	return nil
}

func (kv *MmapSingleKV) Get(key []byte) ([]byte, error) {
	kv.mutex.RLock()
	defer kv.mutex.RUnlock()

	offset, exists := kv.index[string(key)]
	if !exists {
		return nil, fmt.Errorf("key not found")
	}

	// Read entry at offset using unsafe for speed
	keyLen := *(*uint32)(unsafe.Pointer(&kv.data[offset+8]))
	valueLen := *(*uint32)(unsafe.Pointer(&kv.data[offset+12]))

	if valueLen == deletedMarker {
		return nil, fmt.Errorf("key not found")
	}

	valueOffset := offset + headerSize + int64(keyLen)
	value := make([]byte, valueLen)
	copy(value, kv.data[valueOffset:valueOffset+int64(valueLen)])

	return value, nil
}

func (kv *MmapSingleKV) Keys() [][]byte {
	kv.mutex.RLock()
	defer kv.mutex.RUnlock()

	keys := make([][]byte, 0, len(kv.index))
	for k := range kv.index {
		keys = append(keys, []byte(k))
	}
	return keys
}

func (kv *MmapSingleKV) Put(key []byte, val []byte) error {
	if len(val) > 1073741822 {
		return fmt.Errorf("value size too large: %v", len(val))
	}
	if len(key) > 32768 {
		return fmt.Errorf("key size too large: %v", len(key))
	}

	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	return kv.writeEntry(key, val, false)
}

func (kv *MmapSingleKV) Exists(key []byte) bool {
	kv.mutex.RLock()
	defer kv.mutex.RUnlock()

	_, exists := kv.index[string(key)]
	return exists
}

func (kv *MmapSingleKV) Delete(key []byte) error {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	// Check if key exists
	if _, exists := kv.index[string(key)]; !exists {
		return nil // Already deleted
	}

	return kv.writeEntry(key, nil, true)
}

func (kv *MmapSingleKV) Size() int64 {
	kv.mutex.RLock()
	defer kv.mutex.RUnlock()

	return int64(len(kv.index))
}

func (kv *MmapSingleKV) Flush() error {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	// Only sync a small region around recent writes for performance
	// This is safe because we use MAP_SHARED
	if kv.offset > 0 {
		// Sync only the last 1MB or the used region, whichever is smaller
		syncStart := int64(0)
		if kv.offset > 1024*1024 {
			syncStart = kv.offset - 1024*1024
		}
		syncRegion := kv.data[syncStart:kv.offset]
		if err := unix.Msync(syncRegion, unix.MS_ASYNC); err != nil {
			return fmt.Errorf("failed to msync: %w", err)
		}
	}

	return nil
}

func (kv *MmapSingleKV) Close() error {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	// Sync all data before closing
	if len(kv.data) > 0 {
		unix.Msync(kv.data, unix.MS_SYNC)
	}

	// Unmap
	if err := syscall.Munmap(kv.data); err != nil {
		return fmt.Errorf("failed to munmap: %w", err)
	}

	// Close file
	if err := kv.file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	return nil
}

func (kv *MmapSingleKV) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	// Snapshot keys first
	keys := kv.Keys() // Keys() handles its own locking

	visited := make(map[string]bool)

	for _, key := range keys {
		// Fetch value for each key (thread-safe)
		val, err := kv.Get(key)
		if err != nil {
			continue // Deleted concurrently
		}

		visited[string(key)] = true

		// Callback without lock
		if err := f(key, val); err != nil {
			return visited, err
		}
	}

	return visited, nil
}

func (kv *MmapSingleKV) KeyHistory(key []byte) ([][]byte, error) {
	kv.mutex.RLock()
	defer kv.mutex.RUnlock()

	offset, exists := kv.index[string(key)]
	if !exists {
		return [][]byte{}, nil
	}

	// Read entry at offset using unsafe for speed
	keyLen := *(*uint32)(unsafe.Pointer(&kv.data[offset+8]))
	valueLen := *(*uint32)(unsafe.Pointer(&kv.data[offset+12]))

	if valueLen == deletedMarker {
		return [][]byte{}, nil
	}

	valueOffset := offset + headerSize + int64(keyLen)
	value := make([]byte, valueLen)
	copy(value, kv.data[valueOffset:valueOffset+int64(valueLen)])

	return [][]byte{value}, nil
}

func (kv *MmapSingleKV) MapPrefixFunc(prefix []byte, f func([]byte, []byte) error) (map[string]bool, error) {
	// Snapshot keys first
	keys := kv.Keys() // Keys() handles its own locking

	visited := make(map[string]bool)

	for _, key := range keys {
		if !bytes.HasPrefix(key, prefix) {
			continue
		}

		val, err := kv.Get(key)
		if err != nil {
			continue
		}

		visited[string(key)] = true

		if err := f(key, val); err != nil {
			return visited, err
		}
	}
	return visited, nil
}

// Helper function to get timestamp for entries
func nowNanos() int64 {
	return time.Now().UnixNano()
}
