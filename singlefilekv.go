package ensemblekv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

const HeaderSize = 1024 * 1024

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func mustMessage(err error, msg string) {
	if err != nil {
		log.Printf("Error: %v, %s", err, msg)
		panic(err)
	}
}

type FileHeader struct {
	Magic       [4]byte
	Version     uint32
	FileSize    int64
	KeysStart   int64
	KeysEnd     int64
	ValuesStart int64
	ValuesEnd   int64
	IndexStart  int64
	IndexEnd    int64
	Reserved    [1048496]byte
}

type IndexEntry struct {
	KeyEnd   int64
	ValueEnd int64
	Flags    int64
}

type KVStore struct {
	path   string
	file   *os.File
	header FileHeader
	lock   sync.Mutex
	closed bool
}

func NewSingleFileKVStore(filename string, blocksize, filesize int64) *KVStore {
	// Check if the file already exists
	if _, err := os.Stat(filename); err != nil {
		// File does not exist, create it
		if os.IsNotExist(err) {
			fmt.Printf("Creating new file: %s of size %d\n", filename, filesize)
			// Create the file with the specified size
			f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				//Find the parent directory
				parentDir := filepath.Dir(filename)
				// Create the parent directory if it doesn't exist
				if err := os.MkdirAll(parentDir, 0755); err != nil {
					panic(fmt.Sprintf("Failed to create parXXRent directory: %v", err))
				}
				// Try to create the file again
				f, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)

				mustMessage(err, "Failed to create file:"+filename)
			}
			must(f.Truncate(filesize))
		remaining := filesize - HeaderSize
		keySize := remaining / 100
		indexSize := remaining / 100
		valueSize := remaining - keySize - indexSize
		valueStart := HeaderSize + keySize
		indexStart := valueStart + valueSize
		hdr := FileHeader{
			Magic:       [4]byte{'E', 'K', 'V', 'S'},
			Version:     1,
			FileSize:    filesize,
			KeysStart:   HeaderSize,
			KeysEnd:     HeaderSize,
			ValuesStart: valueStart,
			ValuesEnd:   valueStart,
			IndexStart:  indexStart,
			IndexEnd:    indexStart,
		}
		must(writeHeader(f, &hdr))
		must(f.Close())
		} else {
			// Some other error occurred
			panic(fmt.Sprintf("Error checking file: %v", err))
		}
	}
	//fmt.Printf("Opening existing file: %s of size %d\n", filename, filesize)
	// Open the file for reading and writing
	f, err := os.OpenFile(filename, os.O_RDWR, 0644)
	must(err)
	// Read the file header
	hdr := FileHeader{}
	must(binary.Read(f, binary.BigEndian, &hdr))
	if hdr.Magic != [4]byte{'E', 'K', 'V', 'S'} {
		panic(fmt.Sprintf("Invalid file format: %s", filename))
	}
	if hdr.Version != 1 {
		panic(fmt.Sprintf("Unsupported version: %d", hdr.Version))
	}
	// Get real file size
	fi, err := f.Stat()
	must(err)
	filesize = fi.Size()
	if hdr.FileSize != filesize {
		panic(fmt.Sprintf("File size mismatch: expected %d, got %d", filesize, hdr.FileSize))
	}
	if hdr.KeysStart != HeaderSize {
		panic(fmt.Sprintf("Invalid KeysStart: %d", hdr.KeysStart))
	}

	//fmt.Printf("File %s opened successfully with size %d\n", filename, filesize)

	return &KVStore{filename, f, hdr, sync.Mutex{}, false}
}

func (kv *KVStore) assertOpen() {
	if kv.closed {
		panic("KVStore " + kv.path + " is closed")
	}
}

func writeHeader(f *os.File, hdr *FileHeader) error {
	_, err := f.Seek(0, 0)
	must(err)
	return binary.Write(f, binary.BigEndian, hdr)
}

func (kv *KVStore) updateHeader() {
	must(writeHeader(kv.file, &kv.header))
}

func (kv *KVStore) Put(key, value []byte) error {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	kv.assertOpen()
	kv.assertOpen()
	if kv.header.KeysEnd+int64(len(key)) > kv.header.ValuesStart {
		panic(fmt.Sprintf("KeysEnd %d + key size %d exceeds ValuesStart %d", kv.header.KeysEnd, len(key), kv.header.ValuesStart))
	}
	if kv.header.ValuesEnd+int64(len(value)) > kv.header.IndexStart {
		panic(fmt.Sprintf("ValuesEnd %d + value size %d exceeds IndexStart %d", kv.header.ValuesEnd, len(value), kv.header.IndexStart))
	}
	if kv.header.IndexEnd+int64(binary.Size(IndexEntry{})) > kv.header.FileSize {
		panic(fmt.Sprintf("IndexEnd %d + entry size %d exceeds FileSize %d", kv.header.IndexEnd, binary.Size(IndexEntry{}), kv.header.FileSize))
	}
	must(writeAt(kv.file, kv.header.ValuesEnd, value))
	kv.header.ValuesEnd += int64(len(value))
	must(writeAt(kv.file, kv.header.KeysEnd, key))
	kv.header.KeysEnd += int64(len(key))
	entry := IndexEntry{kv.header.KeysEnd, kv.header.ValuesEnd, 0}
	must(writeIndexEntry(kv.file, kv.header.IndexEnd, entry))
	kv.header.IndexEnd += int64(binary.Size(entry))
	kv.updateHeader()
	return nil
}

func (kv *KVStore) Get(key []byte) ([]byte, error) {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	kv.assertOpen()
	return kv.LockFreeGet(key)
}

func (kv *KVStore) Exists(key []byte) bool {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	kv.assertOpen()
	_, err := kv.LockFreeGet(key)
	if err != nil {
		return false
	}
	return true
}

func (kv *KVStore) Delete(key []byte) error {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	kv.assertOpen()
	var lastKeyEnd int64 = kv.header.KeysStart
	for offset := kv.header.IndexStart; offset < kv.header.IndexEnd; offset += int64(binary.Size(IndexEntry{})) {
		entry := readIndexEntry(kv.file, offset)
		keyData := mustReadAt(kv.file, lastKeyEnd, entry.KeyEnd-lastKeyEnd)
		if bytes.Equal(keyData, key) && entry.Flags != 1 {
			entry.Flags = 1
			must(writeIndexEntry(kv.file, offset, entry))
			return nil
		}
		lastKeyEnd = entry.KeyEnd
	}
	return nil
}

func (kv *KVStore) Flush() error {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	kv.assertOpen()
	kv.LockFreeFlush()
	return nil
}

func (kv *KVStore) LockFreeFlush() error {
	must(kv.file.Sync())
	return nil
}

func (kv *KVStore) Close() error {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	if kv.closed {
		log.Printf("close called when KVStore is already closed:" + kv.path)
		return nil
	}
	kv.LockFreeFlush()
	must(kv.file.Close())
	kv.closed = true
	//fmt.Printf("File %s closed successfully\n", kv.path)
	return nil
}

func (kv *KVStore) ClearCache() {}

func (kv *KVStore) DumpIndex() error {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	kv.assertOpen()
	return kv.DumpIndexLockFree()
}

func (kv *KVStore) DumpIndexLockFree() error {
	lastKeyEnd := kv.header.KeysStart
	lastValueEnd := kv.header.ValuesStart
	for offset := kv.header.IndexStart; offset < kv.header.IndexEnd; offset += int64(binary.Size(IndexEntry{})) {
		entry := readIndexEntry(kv.file, offset)
		keySize := entry.KeyEnd - lastKeyEnd
		valueSize := entry.ValueEnd - lastValueEnd
		fmt.Printf("KeyEnd: %d, ValueEnd: %d, Flags: %d, KeySize: %d, ValueSize: %d\n", entry.KeyEnd, entry.ValueEnd, entry.Flags, keySize, valueSize)
		lastKeyEnd = entry.KeyEnd
		lastValueEnd = entry.ValueEnd
	}
	return nil
}

func (kv *KVStore) LockFreeGet(key []byte) ([]byte, error) {
	var lastKeyEnd int64 = kv.header.KeysStart
	var lastValueEnd int64 = kv.header.ValuesStart

	retVal := make([]byte, 0)
	found := false

	for offset := kv.header.IndexStart; offset < kv.header.IndexEnd; offset += int64(binary.Size(IndexEntry{})) {
		entry := readIndexEntry(kv.file, offset)

		if lastKeyEnd > kv.header.FileSize {
			panic(fmt.Sprintf("lastKeyEnd %d > FileSize %d", lastKeyEnd, kv.header.FileSize))
		}
		if lastValueEnd > kv.header.FileSize {
			panic(fmt.Sprintf("lastValueEnd %d > FileSize %d", lastValueEnd, kv.header.FileSize))
		}

		keyData := mustReadAt(kv.file, lastKeyEnd, entry.KeyEnd-lastKeyEnd)
		if bytes.Equal(keyData, key) && entry.Flags != 1 {
			retVal =  mustReadAt(kv.file, lastValueEnd, entry.ValueEnd-lastValueEnd)
			found = true
		}


		lastKeyEnd = entry.KeyEnd
		lastValueEnd = entry.ValueEnd
	}
	if found {
		return retVal, nil
	}
	return nil, fmt.Errorf("Key not found")
}

func (kv *KVStore) KeyHistory(key []byte) ([][]byte, error) {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	kv.assertOpen()
	var lastKeyEnd int64 = kv.header.KeysStart
	var lastValueEnd int64 = kv.header.ValuesStart
	var history [][]byte
	for offset := kv.header.IndexStart; offset < kv.header.IndexEnd; offset += int64(binary.Size(IndexEntry{})) {
		entry := readIndexEntry(kv.file, offset)
		keyData := mustReadAt(kv.file, lastKeyEnd, entry.KeyEnd-lastKeyEnd)
		if bytes.Equal(keyData, key) && entry.Flags != 1 {
			valueData := mustReadAt(kv.file, lastValueEnd, entry.ValueEnd-lastValueEnd)
			history = append(history, valueData)
		}
		lastKeyEnd = entry.KeyEnd
		lastValueEnd = entry.ValueEnd
	}
	return history, fmt.Errorf("Key not found")
}

func (kv *KVStore) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	kv.assertOpen()
	return kv.LockFreeMapFunc(f)
}

func (kv *KVStore) LockFreeMapFunc(f func([]byte, []byte) error) (map[string]bool, error) {

	var lastKeyEnd int64 = kv.header.KeysStart
	var lastValueEnd int64 = kv.header.ValuesStart
	var validKeys = make(map[string]bool)
	for offset := kv.header.IndexStart; offset < kv.header.IndexEnd; offset += int64(binary.Size(IndexEntry{})) {
		entry := readIndexEntry(kv.file, offset)
		keyData := mustReadAt(kv.file, lastKeyEnd, entry.KeyEnd-lastKeyEnd)
		// Have we seen this key before?
		_, seen := validKeys[string(keyData)]
		if seen {
			continue
		}
		// check if this is a tombstone.  A position of -1 indicates a deleted key
		if entry.Flags == 1 {
			validKeys[string(keyData)] = false
			continue
		} else {
			validKeys[string(keyData)] = true
		}

		valueData := mustReadAt(kv.file, lastValueEnd, entry.ValueEnd-lastValueEnd)

		err := f(keyData, valueData)
		if err != nil {
			return nil, fmt.Errorf("LockFreeMapFunc: failed to process key-value pair: %w", err)
		}
		lastKeyEnd = entry.KeyEnd
		lastValueEnd = entry.ValueEnd
	}
	return validKeys, nil
}

func (kv *KVStore) Size() int64 {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	kv.assertOpen()
	// Count up all used spsce in file
	var size int64 =  kv.header.KeysEnd + (kv.header.ValuesEnd - kv.header.ValuesStart) + (kv.header.IndexEnd - kv.header.IndexStart)
	return size
}

func writeAt(f *os.File, pos int64, data []byte) error {
	_, err := f.Seek(pos, 0)
	must(err)
	_, err = f.Write(data)
	return err
}

func readAt(f *os.File, pos int64, size int64) ([]byte, error) {
	buf := make([]byte, size)
	_, err := f.ReadAt(buf, pos)
	return buf, err
}

func mustReadAt(f *os.File, pos, size int64) []byte {
	data, err := readAt(f, pos, size)
	must(err)
	return data
}

func writeIndexEntry(f *os.File, pos int64, entry IndexEntry) error {
	_, err := f.Seek(pos, 0)
	must(err)
	return binary.Write(f, binary.BigEndian, entry)
}

func readIndexEntry(f *os.File, pos int64) IndexEntry {
	_, err := f.Seek(pos, 0)
	must(err)
	var entry IndexEntry
	must(binary.Read(f, binary.BigEndian, &entry))
	return entry
}
