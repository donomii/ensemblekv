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
	"golang.org/x/sys/unix"
)

// mmapFile wraps an os.File with a shared mmap-backed byte slice so that read
// and write operations operate on memory rather than issuing syscalls on every
// access. It implements the minimal set of methods the extent stores rely on.
type mmapFile struct {
	file   *os.File
	data   []byte
	offset int64
	size   int64
	length int
	path   string
}

func openMmapFile(path string) (*mmapFile, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	size := info.Size()
	length := int(size)
	var data []byte
	if length > 0 {
		data, err = unix.Mmap(int(f.Fd()), 0, length, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
		if err != nil {
			f.Close()
			return nil, err
		}
	}

	return &mmapFile{
		file:   f,
		data:   data,
		offset: 0,
		size:   size,
		length: length,
		path:   path,
	}, nil
}

func (m *mmapFile) ensureCapacity(required int64) error {
	if required <= int64(m.length) {
		if required > m.size {
			m.size = required
		}
		return nil
	}

	if err := m.resize(required); err != nil {
		return err
	}
	m.size = required
	if m.offset > m.size {
		m.offset = m.size
	}
	return nil
}

func (m *mmapFile) resize(newSize int64) error {
	if newSize < 1 {
		return fmt.Errorf("invalid size %d", newSize)
	}

	if err := m.file.Truncate(newSize); err != nil {
		return err
	}

	if err := m.remap(newSize); err != nil {
		return err
	}
	return nil
}

func (m *mmapFile) remap(size int64) error {
	if m.data != nil {
		_ = unix.Msync(m.data, unix.MS_SYNC)
		if err := unix.Munmap(m.data); err != nil {
			return err
		}
	}

	if size == 0 {
		m.data = nil
		m.length = 0
		m.size = 0
		m.offset = 0
		return nil
	}

	mapLen := int(size)
	data, err := unix.Mmap(int(m.file.Fd()), 0, mapLen, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return err
	}

	m.data = data
	m.length = mapLen
	if size < 0 {
		size = 0
	}
	m.size = size
	if m.offset > m.size {
		m.offset = m.size
	}
	return nil
}

func (m *mmapFile) Read(p []byte) (int, error) {
	if m.offset >= m.size {
		return 0, io.EOF
	}

	n := copy(p, m.data[m.offset:m.size])
	m.offset += int64(n)
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *mmapFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= m.size {
		return 0, io.EOF
	}
	n := copy(p, m.data[off:m.size])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *mmapFile) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	required := m.offset + int64(len(p))
	if err := m.ensureCapacity(required); err != nil {
		return 0, err
	}

	copy(m.data[m.offset:m.offset+int64(len(p))], p)
	m.offset += int64(len(p))
	if m.offset > m.size {
		m.size = m.offset
	}
	return len(p), nil
}

func (m *mmapFile) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = m.offset + offset
	case io.SeekEnd:
		newOffset = m.size + offset
	default:
		return 0, fmt.Errorf("invalid whence %d", whence)
	}

	if newOffset < 0 {
		return 0, fmt.Errorf("negative seek target %d", newOffset)
	}

	m.offset = newOffset
	return newOffset, nil
}

func (m *mmapFile) Truncate(size int64) error {
	if size < 0 {
		return fmt.Errorf("invalid truncate size %d", size)
	}

	if err := m.file.Truncate(size); err != nil {
		return err
	}

	if err := m.remap(size); err != nil {
		return err
	}
	m.size = size
	return nil
}

func (m *mmapFile) Sync() error {
	if m.data != nil {
		if err := unix.Msync(m.data, unix.MS_SYNC); err != nil {
			return err
		}
	}
	return m.file.Sync()
}

func (m *mmapFile) Close() error {
	if m.data != nil {
		if err := unix.Msync(m.data, unix.MS_SYNC); err != nil {
			return err
		}
		if err := unix.Munmap(m.data); err != nil {
			return err
		}
		m.data = nil
	}
	return m.file.Close()
}

func (m *mmapFile) Name() string {
	return m.path
}

func (m *mmapFile) Stat() (os.FileInfo, error) {
	return m.file.Stat()
}

func (m *mmapFile) Size() int64 {
	return m.size
}

func readInt64At(file *mmapFile, offset int64) (int64, error) {
	if offset < 0 || offset+8 > file.Size() {
		return 0, fmt.Errorf("invalid offset %d for file of size %d", offset, file.Size())
	}
	var value int64
	buf := bytes.NewReader(file.data[offset : offset+8])
	if err := binary.Read(buf, binary.BigEndian, &value); err != nil {
		return 0, err
	}
	return value, nil
}

func writeInt64At(file *mmapFile, offset int64, value int64) error {
	if offset < 0 {
		return fmt.Errorf("invalid offset %d", offset)
	}
	required := offset + 8
	if err := file.ensureCapacity(required); err != nil {
		return err
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value))
	copy(file.data[offset:offset+8], buf)
	if file.offset < required {
		file.offset = required
	}
	return nil
}

func appendInt64(file *mmapFile, value int64) error {
	offset := file.Size()
	return writeInt64At(file, offset, value)
}

// ExtentMmapKeyValStore mirrors ExtentKeyValStore but keeps its backing files
// mmapped to minimise syscall overhead.
type ExtentMmapKeyValStore struct {
	DefaultOps
	keysFile    *mmapFile
	valuesFile  *mmapFile
	keysIndex   *mmapFile
	valuesIndex *mmapFile
	blockSize   int64
	globalLock  deadlock.Mutex
	cache       *syncmap.SyncMap[string, bool]

	keysIndexCache   []byte
	valuesIndexCache []byte

	cacheHits    int
	cacheMisses  int
	cacheWrites  int
	cacheLoads   int
	requestCount int
}

func (s *ExtentMmapKeyValStore) maybePrintCacheStats() {
	s.requestCount++
	if s.requestCount%100 == 0 {
		debugf("Cache stats: hits=%d, misses=%d, writes=%d, loads=%d\n",
			s.cacheHits, s.cacheMisses, s.cacheWrites, s.cacheLoads)
	}
}

func NewExtentMmapKeyValueStore(directory string, blockSize, filesize int64) (*ExtentMmapKeyValStore, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	keysFilePath := directory + "/keys.dat"
	valuesFilePath := directory + "/values.dat"
	keysIndexFilePath := directory + "/keys.index"
	valuesIndexFilePath := directory + "/values.index"

	if !goof.Exists(keysIndexFilePath) {
		index, err := os.OpenFile(keysIndexFilePath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		if err := binary.Write(index, binary.BigEndian, int64(0)); err != nil {
			index.Close()
			return nil, err
		}
		index.Close()
	}

	if !goof.Exists(valuesIndexFilePath) {
		index, err := os.OpenFile(valuesIndexFilePath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		if err := binary.Write(index, binary.BigEndian, int64(0)); err != nil {
			index.Close()
			return nil, err
		}
		index.Close()
	}

	keysFile, err := openMmapFile(keysFilePath)
	if err != nil {
		return nil, fmt.Errorf("open keys file: %w", err)
	}

	keysIndex, err := openMmapFile(keysIndexFilePath)
	if err != nil {
		return nil, fmt.Errorf("open keys index: %w", err)
	}

	if keysIndex.Size()%8 != 0 {
		return nil, fmt.Errorf("keys index length must be multiple of 8, got %d", keysIndex.Size())
	}

	if keysIndex.Size() < 8 {
		return nil, errors.New("keys index file is empty")
	}

	lastKeysIndex, err := readInt64At(keysIndex, keysIndex.Size()-8)
	if err != nil {
		return nil, fmt.Errorf("read last keys index entry: %w", err)
	}

	if lastKeysIndex < 0 {
		lastKeysIndex = -lastKeysIndex
	}

	if keysFile.Size() != lastKeysIndex {
		diff := lastKeysIndex - keysFile.Size()
		if diff < 0 {
			debugf("Index position is larger than actual keys file size: %d, cannot recover automatically\n", diff)
		}
		if diff > 1024 {
			return nil, fmt.Errorf("keys file difference too large: %d bytes", diff)
		}
		debugf("Keys file is larger than recorded in keys index. Truncating keys file %s to %d\n", keysFilePath, lastKeysIndex)
		if err := keysFile.Truncate(lastKeysIndex); err != nil {
			return nil, err
		}
	}

	valuesIndex, err := openMmapFile(valuesIndexFilePath)
	if err != nil {
		return nil, fmt.Errorf("open values index: %w", err)
	}

	valuesFile, err := openMmapFile(valuesFilePath)
	if err != nil {
		return nil, fmt.Errorf("open values file: %w", err)
	}

	if valuesIndex.Size()%8 != 0 {
		return nil, fmt.Errorf("values index length must be multiple of 8, got %d", valuesIndex.Size())
	}

	if valuesIndex.Size() < 8 {
		return nil, errors.New("values index file is empty")
	}

	lastValuesIndex, err := readInt64At(valuesIndex, valuesIndex.Size()-8)
	if err != nil {
		return nil, fmt.Errorf("read last values index entry: %w", err)
	}

	if lastValuesIndex < 0 {
		lastValuesIndex = -lastValuesIndex
	}

	if valuesFile.Size() != lastValuesIndex {
		diff := lastValuesIndex - valuesFile.Size()
		if diff < 0 {
			debugf("Index position is larger than actual values file size: %d, cannot recover automatically\n", diff)
		}
		if diff > 1024 {
			return nil, fmt.Errorf("values file difference too large: %d bytes", diff)
		}
		debugf("Values file is larger than recorded in values index. Truncating values file %s to %d\n", valuesFilePath, lastValuesIndex)
		if err := valuesFile.Truncate(lastValuesIndex); err != nil {
			return nil, err
		}
	}

	store := &ExtentMmapKeyValStore{
		keysFile:    keysFile,
		valuesFile:  valuesFile,
		keysIndex:   keysIndex,
		valuesIndex: valuesIndex,
		blockSize:   blockSize,
	}

	if EnableIndexCaching {
		store.ClearCache()
	}

	return store, nil
}

func checkLastIndexEntryMmap(indexFile *mmapFile, dataFile *mmapFile) {
	if !ExtraChecks {
		return
	}
	if indexFile.Size() < 8 {
		panic("index file too small")
	}
	lastEntry, err := readInt64At(indexFile, indexFile.Size()-8)
	panicOnError("Read last entry in index file", err)
	if lastEntry < 0 {
		panic("Last entry in index file is negative")
	}
	if lastEntry != dataFile.Size() {
		panic(fmt.Sprintf("Last entry in index file does not match data file length: %d != %d", lastEntry, dataFile.Size()))
	}
}

func checkIndexSignsMmap(keyIndex *mmapFile, valueIndex *mmapFile) {
	if !ExtraChecks {
		return
	}
	if keyIndex.Size() != valueIndex.Size() {
		panic("key and value index sizes must match")
	}
	for offset := int64(0); offset+8 <= keyIndex.Size(); offset += 8 {
		keyOffset, err := readInt64At(keyIndex, offset)
		panicOnError("Read entry in key index", err)
		valueOffset, err := readInt64At(valueIndex, offset)
		panicOnError("Read entry in value index", err)

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
			panic("Key offset is non-zero but value offset is 0")
		}
	}
}

func (s *ExtentMmapKeyValStore) loadKeysIndexCache() error {
	if !EnableIndexCaching {
		return nil
	}

	if s.keysIndexCache != nil {
		return nil
	}

	stat, err := s.keysIndex.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat keys index: %w", err)
	}

	if stat.Size() == 0 {
		s.keysIndexCache = []byte{}
		return nil
	}

	s.keysIndexCache = make([]byte, stat.Size())
	_, err = s.keysIndex.ReadAt(s.keysIndexCache, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		s.keysIndexCache = nil
		return fmt.Errorf("failed to read keys index: %w", err)
	}

	return nil
}

func (s *ExtentMmapKeyValStore) loadValuesIndexCache() error {
	if !EnableIndexCaching {
		return nil
	}

	if s.valuesIndexCache != nil {
		return nil
	}

	stat, err := s.valuesIndex.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat values index: %w", err)
	}

	if stat.Size() == 0 {
		s.valuesIndexCache = []byte{}
		return nil
	}

	s.valuesIndexCache = make([]byte, stat.Size())
	_, err = s.valuesIndex.ReadAt(s.valuesIndexCache, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		s.valuesIndexCache = nil
		return fmt.Errorf("failed to read values index: %w", err)
	}

	return nil
}

func (s *ExtentMmapKeyValStore) loadKeyCache() error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

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
		if entry*8+8 >= keyIndexFileLength {
			break
		}
		_, err := s.keysIndex.Seek(int64(entry)*8, 0)
		panicOnError("Seek to current entry in keys index file", err)
		var keyPos int64
		err = binary.Read(s.keysIndex, binary.BigEndian, &keyPos)
		if err != nil {
			break
		}

		keyData, deleted, err := s.readDataAtIndexPos(int64(entry*8), s.keysIndex, s.keysFile, s.keysIndexCache)
		panicOnError("Read key data", err)

		_, err = s.valuesIndex.Seek(int64(entry)*8, 0)
		panicOnError("Seek to current entry in values index file", err)
		var valuePos int64
		err = binary.Read(s.valuesIndex, binary.BigEndian, &valuePos)
		if err != nil {
			break
		}

		_ = keyPos
		_ = valuePos

		s.cache.Store(string(keyData), !deleted)

		entry++
	}

	s.cacheLoads++
	return nil
}

func (s *ExtentMmapKeyValStore) forwardScanForKey(key []byte) ([]byte, error) {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

	checkLastIndexEntryMmap(s.keysIndex, s.keysFile)
	checkLastIndexEntryMmap(s.valuesIndex, s.valuesFile)
	checkIndexSignsMmap(s.keysIndex, s.valuesIndex)

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

	checkIndexSignsMmap(s.keysIndex, s.valuesIndex)
	value, _, err := s.readDataAtIndexPos(offset, s.valuesIndex, s.valuesFile, s.valuesIndexCache)
	if err != nil {
		return nil, fmt.Errorf("forwardScanForKey: failed to read value: %v", err)
	}

	if EnableIndexCaching {
		s.cache.Store(string(key), true)
	}
	return value, nil
}

func (s *ExtentMmapKeyValStore) KeyHistory(key []byte) ([][]byte, error) {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

	history := make([][]byte, 0)

	s.keysFile.Seek(0, 0)
	s.keysIndex.Seek(0, 0)
	s.valuesFile.Seek(0, 0)
	s.valuesIndex.Seek(0, 0)

	keyIndexFileLength, _ := s.keysIndex.Seek(0, 2)
	s.keysIndex.Seek(0, 0)

	if EnableIndexCaching {
		s.loadKeysIndexCache()
		s.loadValuesIndexCache()
	}

	entry := int64(0)
	for {
		if entry*8+16 > keyIndexFileLength {
			break
		}

		keyData, deleted, err := s.readDataAtIndexPos(int64(entry*8), s.keysIndex, s.keysFile, s.keysIndexCache)
		if err != nil {
			entry++
			continue
		}

		if bytes.Equal(keyData, key) && !deleted {
			valueData, _, err := s.readDataAtIndexPos(int64(entry*8), s.valuesIndex, s.valuesFile, s.valuesIndexCache)
			if err == nil {
				copyValue := make([]byte, len(valueData))
				copy(copyValue, valueData)
				history = append(history, copyValue)
			}
		}

		entry++
	}

	return history, nil
}

func (s *ExtentMmapKeyValStore) readIndexAt(indexFile *mmapFile, cache []byte, offset int64, data interface{}) error {
	if EnableIndexCaching && cache != nil {
		if offset < 0 || offset+8 > int64(len(cache)) {
			return fmt.Errorf("index cache read out of bounds: offset=%d, len=%d", offset, len(cache))
		}
		return binary.Read(bytes.NewReader(cache[offset:offset+8]), binary.BigEndian, data)
	}

	_, err := indexFile.Seek(offset, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to offset %d: %w", offset, err)
	}
	return binary.Read(indexFile, binary.BigEndian, data)
}

func (s *ExtentMmapKeyValStore) ClearCache() {
	s.keysIndexCache = nil
	s.valuesIndexCache = nil
	s.cache = (*syncmap.SyncMap[string, bool])(syncmap.NewSyncMap[string, bool]())
}

func (s *ExtentMmapKeyValStore) Put(key, value []byte) error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

	valuePos, err := s.valuesFile.Seek(0, io.SeekEnd)
	panicOnError("Seek to end of values file", err)
	valueSize, err := s.valuesFile.Write(value)
	panicOnError("Write value to values file", err)

	if valueSize != len(value) {
		panic("Wrote wrong number of bytes")
	}

	endOfValuesFile, err := s.valuesFile.Seek(0, io.SeekEnd)
	panicOnError("Seek to end of values file", err)

	if valuePos+int64(valueSize) != endOfValuesFile {
		panic(fmt.Errorf("valuePos+valueSize (%d) != endOfValuesFile (%d)", valuePos+int64(valueSize), endOfValuesFile))
	}

	_, err = s.valuesIndex.Seek(0, io.SeekEnd)
	panicOnError("Seek to end of values index file", err)

	err = binary.Write(s.valuesIndex, binary.BigEndian, endOfValuesFile)
	panicOnError("Write end of values file to values index file", err)

	keyDataPos, err := s.keysFile.Seek(0, io.SeekEnd)
	panicOnError("Seek to end of keys file", err)

	keySize, err := s.keysFile.Write(key)
	panicOnError("Write key to keys file", err)

	if keySize != len(key) {
		panic("Key size mismatch")
	}

	endOfKeysFile, err := s.keysFile.Seek(0, io.SeekEnd)
	panicOnError("Seek to end of keys file", err)

	if keyDataPos+int64(keySize) != endOfKeysFile {
		panic(fmt.Errorf("keyDataPos+keySize (%d) != endOfKeysFile (%d)", keyDataPos+int64(keySize), endOfKeysFile))
	}

	_, err = s.keysIndex.Seek(0, io.SeekEnd)
	panicOnError("Seek to end of keys index file", err)

	err = binary.Write(s.keysIndex, binary.BigEndian, endOfKeysFile)
	panicOnError("Write to keys index file", err)

	checkLastIndexEntryMmap(s.keysIndex, s.keysFile)
	checkLastIndexEntryMmap(s.valuesIndex, s.valuesFile)
	checkIndexSignsMmap(s.keysIndex, s.valuesIndex)
	if EnableIndexCaching {
		s.cache.Store(string(key), true)
	}
	return nil
}

func (s *ExtentMmapKeyValStore) Get(key []byte) ([]byte, error) {
	s.maybePrintCacheStats()

	if EnableIndexCaching {
		if state, exists := s.cache.Load(string(key)); exists {
			s.cacheHits++
			if !state {
				return nil, errors.New("key marked as not present in cache")
			}
		} else {
			s.cacheMisses++
		}
	}

	val, err := s.forwardScanForKey(key)
	if err != nil {
		return nil, fmt.Errorf("Get: %w", err)
	}

	return val, nil
}

func (s *ExtentMmapKeyValStore) readDataAtIndexPos(indexPosition int64, indexFile *mmapFile, dataFile *mmapFile, cache []byte) ([]byte, bool, error) {
	dataFileLength, err := dataFile.Seek(0, io.SeekEnd)
	indexFileLength, err := indexFile.Seek(0, io.SeekEnd)
	deleted := false
	var dataPos int64 = 0

	if indexFileLength == indexPosition+8 {
		panic(fmt.Errorf("readDataAtIndexPos: invalid index position, data file length is %d, and position is %d (this is the end of file marker, you read one too far)", indexFileLength, indexPosition))
	}

	if indexPosition+16 > indexFileLength {
		panic(fmt.Sprintf("attempt to read past end of file: offset=%d, index file length len=%d.  Last index is the seocnd last pointer(len-16), not the last pointer(len-8)", indexPosition, indexFileLength))
		return nil, false, fmt.Errorf("readDataAtIndexPos: invalid index position %v greater than indexfile of length %v", indexPosition, indexFileLength)
	}

	if EnableIndexCaching && cache != nil {
		err = binary.Read(bytes.NewReader(cache[indexPosition:indexPosition+8]), binary.BigEndian, &dataPos)
	} else {
		_, err = indexFile.Seek(indexPosition, io.SeekStart)
		if err != nil {
			return nil, false, fmt.Errorf("failed to seek to index position: %w", err)
		}
		err = binary.Read(indexFile, binary.BigEndian, &dataPos)
	}
	if err != nil {
		return nil, false, fmt.Errorf("failed to read data offset at index position: %v, length %v, file length %v, %v", indexPosition, 8, indexFileLength, err)
	}

	if dataPos < 0 {
		deleted = true
		dataPos = -dataPos
	}

	var nextDataPos int64
	if EnableIndexCaching && cache != nil {
		if indexPosition+16 > int64(len(cache)) {
			return nil, false, fmt.Errorf("invalid index position %v for cache of length %v.  Probable attempted read on the last entry, but the last entry is always the closing entry", indexPosition, len(cache))
		}
		err = binary.Read(bytes.NewReader(cache[indexPosition+8:indexPosition+16]), binary.BigEndian, &nextDataPos)
	} else {
		_, err = indexFile.Seek(indexPosition+8, io.SeekStart)
		if err != nil {
			return nil, false, fmt.Errorf("failed to seek to data position: %w", err)
		}
		err = binary.Read(indexFile, binary.BigEndian, &nextDataPos)
	}
	if err != nil {
		return nil, false, fmt.Errorf("readDataAtIndexPos: failed to read next index position: %w", err)
	}

	// Flip the sign if nextDataPos is negative (deleted key marker)
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

	if size == 0 {
		return nil, deleted, nil
	}

	buffer := make([]byte, size)
	_, err = dataFile.Seek(dataPos, io.SeekStart)
	if err != nil {
		return nil, false, fmt.Errorf("failed to seek to data position: %w", err)
	}
	_, err = dataFile.Read(buffer)
	if err != nil {
		return nil, false, fmt.Errorf("failed to read data: %w", err)
	}

	return buffer, deleted, nil
}

func (s *ExtentMmapKeyValStore) Close() error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

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

func (s *ExtentMmapKeyValStore) List() ([]string, error) {
	s.loadKeyCache()
	keys := s.cache.Keys()
	return keys, nil
}

func (s *ExtentMmapKeyValStore) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
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

func (s *ExtentMmapKeyValStore) Delete(key []byte) error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

	checkIndexSignsMmap(s.keysIndex, s.valuesIndex)

	if EnableIndexCaching {
		s.cacheWrites++
	}
	s.ClearCache()

	keyPos, err := s.keysFile.Seek(0, io.SeekEnd)
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

	keyTombstone := -keyPos

	eofKeyIndex, err := s.keysIndex.Seek(0, io.SeekEnd)
	panicOnError("Seek to end of keys index file", err)

	keyIndexStart := eofKeyIndex - 8

	_, err = s.keysIndex.Seek(keyIndexStart, io.SeekStart)
	panicOnError("Seek to last key index", err)

	err = binary.Write(s.keysIndex, binary.BigEndian, keyTombstone)
	panicOnError("Write key tombstone", err)

	nextKeyPos := keyPos + int64(keySize)

	keyFileLength, err := s.keysFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	if nextKeyPos != keyFileLength {
		panic("Key file is corrupt")
	}

	s.keysIndex.Seek(0, io.SeekEnd)
	err = binary.Write(s.keysIndex, binary.BigEndian, nextKeyPos)
	if err != nil {
		return err
	}

	valuePos, err := s.valuesFile.Seek(0, io.SeekEnd)
	panicOnError("Seek to end of values file", err)

	tombStoneValue := []byte("DELETED")
	valueSize, err := s.valuesFile.Write(tombStoneValue)
	panicOnError("Write tombstone value", err)

	valueTombstone := -valuePos
	eofValueIndex, err := s.valuesIndex.Seek(0, io.SeekEnd)
	panicOnError("Seek to end of values index file", err)

	valueIndexStart := eofValueIndex - 8
	pos, err := s.valuesIndex.Seek(valueIndexStart, io.SeekStart)
	panicOnError("Seek to last value index", err)

	if pos != valueIndexStart {
		panic("Seek to last value index failed")
	}

	err = binary.Write(s.valuesIndex, binary.BigEndian, valueTombstone)
	panicOnError("Write value tombstone", err)

	nextValuePos := valuePos + int64(valueSize)

	valueFileLength, err := s.valuesFile.Seek(0, io.SeekEnd)
	panicOnError("Seek to end of values file", err)

	if nextValuePos != valueFileLength {
		panic("Value file is corrupt")
	}

	_, err = s.valuesIndex.Seek(0, io.SeekEnd)
	panicOnError("Seek to end of values index file", err)

	err = binary.Write(s.valuesIndex, binary.BigEndian, valueFileLength)
	panicOnError("Write to values index file", err)

	if EnableIndexCaching {
		s.cache.Store(string(key), false)
	}

	return nil
}

func (s *ExtentMmapKeyValStore) doFlush() error {
	if err := s.keysFile.Sync(); err != nil {
		return err
	}
	if err := s.valuesFile.Sync(); err != nil {
		return err
	}
	if err := s.keysIndex.Sync(); err != nil {
		return err
	}
	if err := s.valuesIndex.Sync(); err != nil {
		return err
	}
	s.keysIndexCache = nil
	s.valuesIndexCache = nil
	return nil
}

func (s *ExtentMmapKeyValStore) Flush() error {
	return s.doFlush()
}

func (s *ExtentMmapKeyValStore) DumpIndex() error {
	return s.DumpIndexLockFree()
}

func (s *ExtentMmapKeyValStore) DumpIndexLockFree() error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

	s.doFlush()
	debugln("==========DUMPING INDEX==========")
	keyIndexFileLength, err := s.keysIndex.Seek(0, io.SeekEnd)
	panicOnError("Get key index file length", err)

	_, err = s.keysIndex.Seek(0, io.SeekStart)
	panicOnError("Seek to start of keys index file", err)
	_, err = s.valuesIndex.Seek(0, io.SeekStart)
	panicOnError("Seek to start of values index file", err)
	entry := int64(0)
	for {
		if entry*8+16 > keyIndexFileLength {
			break
		}
		var keyPos int64
		_, err := s.keysIndex.Seek(int64(entry)*8, io.SeekStart)
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
		_, err = s.valuesIndex.Seek(int64(entry)*8, io.SeekStart)
		panicOnError("Seek to current entry in values index file", err)
		err = binary.Read(s.valuesIndex, binary.BigEndian, &valuePos)
		if err != nil {
			break
		}

		debugf("VALUE Entry: %d, BytePosition: %d\n", entry, valuePos)
		entry++

	}
	debugln("==========END DUMPING INDEX==========")
	checkIndexSignsMmap(s.keysIndex, s.valuesIndex)
	return nil
}

func (s *ExtentMmapKeyValStore) Size() int64 {
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

func (s *ExtentMmapKeyValStore) searchDbForKeyExists(searchKey []byte, keysIndex *mmapFile, keysFile *mmapFile, keysIndexCache []byte) (bool, int64, error) {

	checkLastIndexEntryMmap(s.keysIndex, s.keysFile)
	checkLastIndexEntryMmap(s.valuesIndex, s.valuesFile)
	checkIndexSignsMmap(s.keysIndex, s.valuesIndex)

	keyIndexFileLength, err := s.keysIndex.Seek(0, io.SeekEnd)
	panicOnError("Get key index file length", err)

	_, err = s.keysIndex.Seek(0, io.SeekStart)
	panicOnError("Seek to start of keys index file", err)
	_, err = s.valuesIndex.Seek(0, io.SeekStart)
	panicOnError("Seek to start of values index file", err)
	entry := int64(0)
	outFound := false
	outPos := int64(0)

	for {
		if entry*8+8 >= keyIndexFileLength {
			break
		}
		_, err := s.keysIndex.Seek(int64(entry)*8, io.SeekStart)
		panicOnError("Seek to current entry in keys index file", err)
		var keyPos int64
		err = binary.Read(s.keysIndex, binary.BigEndian, &keyPos)
		if err != nil {
			break
		}

		keyData, deleted, err := s.readDataAtIndexPos(int64(entry*8), s.keysIndex, s.keysFile, s.keysIndexCache)
		panicOnError("Read key data", err)

		_, err = s.valuesIndex.Seek(int64(entry)*8, io.SeekStart)
		panicOnError("Seek to current entry in values index file", err)
		var valuePos int64
		err = binary.Read(s.valuesIndex, binary.BigEndian, &valuePos)
		if err != nil {
			break
		}

		_ = keyPos
		_ = valuePos

		if bytes.Equal(keyData, searchKey) {
			if !deleted {
				outFound = true
				outPos = entry * 8
			} else {
				outFound = false
				outPos = -1
			}
		}

		entry++

	}
	return outFound, outPos, nil
}

func (s *ExtentMmapKeyValStore) Exists(key []byte) bool {

	s.maybePrintCacheStats()

	if EnableIndexCaching {
		if s.cache.Len() == 0 {
			if err := s.loadKeyCache(); err != nil {
				s.globalLock.Lock()
				defer s.globalLock.Unlock()
				found, _, err := s.searchDbForKeyExists(key, s.keysIndex, s.keysFile, s.keysIndexCache)
				return err == nil && found
			}
		}

		state, exists := s.cache.Load(string(key))
		if exists {
			s.cacheHits++
			return state
		}
		s.cacheMisses++
	} else {
		s.globalLock.Lock()
		defer s.globalLock.Unlock()
		found, _, err := s.searchDbForKeyExists(key, s.keysIndex, s.keysFile, s.keysIndexCache)
		debugf("err: %v and found: %v\n", err, found)
		return err == nil && found

	}
	return false
}

func (s *ExtentMmapKeyValStore) LockFreeGet(key []byte) ([]byte, error) {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

	checkLastIndexEntryMmap(s.keysIndex, s.keysFile)
	checkLastIndexEntryMmap(s.valuesIndex, s.valuesFile)
	checkIndexSignsMmap(s.keysIndex, s.valuesIndex)

	_, err := s.keysFile.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek keys file: %w", err)
	}
	_, err = s.keysIndex.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek keys index: %w", err)
	}
	_, err = s.valuesFile.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek values file: %w", err)
	}
	_, err = s.valuesIndex.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek values index: %w", err)
	}

	if EnableIndexCaching {
		if err := s.loadKeysIndexCache(); err != nil {
			return nil, fmt.Errorf("failed to load keys index cache: %w", err)
		}
		if err := s.loadValuesIndexCache(); err != nil {
			return nil, fmt.Errorf("failed to load values index cache: %w", err)
		}
	}

	keyIndexPosEndFile, err := s.keysIndex.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to end of keys index: %w", err)
	}

	keyIndexPosStart := keyIndexPosEndFile - 8
	if keyIndexPosStart < 0 {
		return nil, fmt.Errorf("corrupt index file: %s", s.keysIndex.Name())
	}

	found := false
	for {
		keyIndexPosStart = keyIndexPosStart - 8
		if keyIndexPosStart < 0 {
			return nil, fmt.Errorf("key not found after searching to start of file")
		}

		data, deleted, err := s.readDataAtIndexPos(
			keyIndexPosStart,
			s.keysIndex,
			s.keysFile,
			s.keysIndexCache,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to read key data: %w", err)
		}

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

func (s *ExtentMmapKeyValStore) MapPrefixFunc(prefix []byte, f func([]byte, []byte) error) (map[string]bool, error) {
	// ExtentMmapKeyValStore doesn't have native prefix support, so filter through MapFunc
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
