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

type ExtentKeyValStore struct {
	DefaultOps
	keysFile    *os.File
	valuesFile  *os.File
	keysIndex   *os.File
	valuesIndex *os.File
	blockSize   int
	globalLock  sync.Mutex
	cache 	 map[string]bool
}

func NewExtentKeyValueStore(directory string, blockSize int) (*ExtentKeyValStore, error) {
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


	return &ExtentKeyValStore{
		keysFile:    keysFile,
		valuesFile:  valuesFile,
		keysIndex:   keysIndex,
		valuesIndex: valuesIndex,
		blockSize:   blockSize,
		cache: make(map[string]bool),
	}, nil
}

func (s *ExtentKeyValStore) Put(key, value []byte) error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

	//Move to end of data file
	keyPos, err := s.keysFile.Seek(0, 2)
	if err != nil {
		return err
	}

	// Write the key to the keys data file
	keySize, err := s.keysFile.Write(key)
	if err != nil {
		return err
	}

	// Check the written key size is the same as the key size
	if keySize != len(key) {
		panic("Key size mismatch")
	}

	// Write the key position to the keys index file
	err = binary.Write(s.keysIndex, binary.BigEndian, keyPos+int64(keySize))
	if err != nil {
		return err
	}

	// Move to end of data file
	valuePos, err := s.valuesFile.Seek(0, 2)
	if err != nil {
		return err
	}

	// Write the value to the values data file
	valueSize, err := s.valuesFile.Write(value)
	if err != nil {
		return err
	}

	// Check the written value size is the same as the value size
	if valueSize != len(value) {
		panic("Value size mismatch")
	}

	// Write the value position to the values index file
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

func (s *ExtentKeyValStore) Get(key []byte) ([]byte, error) {

	s.globalLock.Lock()
	defer s.globalLock.Unlock()
	state, exists := s.cache[string(key)]
	if exists && !state {
		return nil, errors.New("key marked as not present in cache")
	}
	val, err := s.LockFreeGet(key)
	if err != nil {
		return nil, err
	}
	s.cache[string(key)] = true
	return val, nil
}

func (s *ExtentKeyValStore) LockFreeGet(key []byte) ([]byte, error) {

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

	found := false
	//start at the end of the keysIndex file
	keyIndexPosEndFile, err := s.keysIndex.Seek(0, 2)
	if err != nil {
		panic(err)
	}
	keyIndexPosStart := keyIndexPosEndFile - 8
	if keyIndexPosStart < 0 {
		panic("Corrupt index file: " + s.keysIndex.Name())
	}

	for {
		keyIndexPosStart = keyIndexPosStart - 8
		if keyIndexPosStart < 0 {

			return nil, errors.New("key not found after searching to start of file")
		}

		data , deleted, err := readDataAtIndexPos(keyIndexPosStart, s.keysIndex, s.keysFile)
		if err != nil {
			return nil, err
		}


		if bytes.Equal(key, data) {
			if deleted {
				return nil, errors.New(fmt.Sprintf("key has been deleted at entry %d", keyIndexPosStart/8))
			}
			found = true
			break
		}

	}

	if !found {
		return nil, errors.New("key not found after searching to start of file, after loop")
	}

	data, _, err := readDataAtIndexPos(keyIndexPosStart, s.valuesIndex, s.valuesFile)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// 1. Read the index file at position indexPosition to get dataPos
// 2. Read the next index file at position (indexPosition + 8) to get the end of the block in the data file
// 3. Read the data file from dataPos to end of block

func readDataAtIndexPos(indexPosition int64, indexFile *os.File, dataFile *os.File) ([]byte, bool,error) {
	deleted := false
	var dataPos int64 = 0
	_, err := indexFile.Seek(indexPosition, 0)
	if err != nil {
		panic(err)
	}

	err = binary.Read(indexFile, binary.BigEndian, &dataPos)
	if err != nil {
		panic(err)
	}

	// check if this is a tombstone.  A position of -1 indicates a deleted key
	if dataPos < 0 {
		deleted = true
		dataPos = -dataPos
		//fmt.Printf("Tombstone found at index %d, dataPos is now %d\n", indexPosition/8, dataPos)
	}

	_, err = dataFile.Seek(dataPos, 0)
	if err != nil {

		panic(err)
	}

	start := dataPos
	end := dataPos

	//read next value in valueIndex file to get the size of the block
	stat, _ := indexFile.Stat()
	nextValueIndexPos := indexPosition + 8
	//We don't store the end of the final block in the index file, so we need to check the data file size.  We should store this, to detect partial writes and recover from them.  Otherwise we could return garbage at the end of a block.
	if nextValueIndexPos >= stat.Size() {
		panic("Corrupt index file or read past end of index: "+indexFile.Name())
	} else {
		var nextValuePos int64

		err = binary.Read(indexFile, binary.BigEndian, &nextValuePos)
		if err != nil {
			panic(err)
		}
		//The next value is a tombstone
		if nextValuePos < 0 {
			nextValuePos = -nextValuePos
		}
		end = nextValuePos
	}

	_, err = dataFile.Seek(start, 0)
	if err != nil {
		panic(err)
	}

	//fmt.Printf("Block starts at %d and ends at %d\n", start, end)
	size := end - start
	//fmt.Printf("Loading %d bytes at %d\n", size, start)

	valueBuffer, err := readNbytes(dataFile, int(size))
	if err != nil {
		return nil, deleted, err
	}

	// Check size of valueBuffer is the same as the size
	if int(size) != len(valueBuffer) {
		panic("Size mismatch")
	}

	//fmt.Printf("Tombstone found at index %d, dataPos is now %d for key %s\n", indexPosition/8, dataPos, trimTo40(valueBuffer))

	return valueBuffer, deleted,  nil

}

func searchDbForKeyExists(key []byte, keysIndex *os.File, keysFile *os.File) (bool, error) {
	//start at the end of the keysIndex file
	keyIndexPosStart, err := keysIndex.Seek(-8, 2)
	if err != nil {
		panic(err)
	}

	//Step back 8 bytes to read the last key
	for {
		keyIndexPosStart = keyIndexPosStart - 8
		if keyIndexPosStart < 0 {
			return false, errors.New("key not found")
		}

		currentKey, deleted, err := readDataAtIndexPos(keyIndexPosStart, keysIndex, keysFile)
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

func (s *ExtentKeyValStore) Close() error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()
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

	var keyIndexPosStart int64
	//start at the end of the keysIndex file
	keyIndexPosStart, err = s.keysIndex.Seek(-8, 2)
	if err != nil {
		panic(err)
	}

	var validKeys = make(map[string]bool)
	for {
		keyIndexPosStart = keyIndexPosStart - 8
		if keyIndexPosStart < 0 {

			return validKeys, nil
		}


		keyData , deleted, err := readDataAtIndexPos(keyIndexPosStart, s.keysIndex, s.keysFile)



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

		valueBuffer, _, err := readDataAtIndexPos(keyIndexPosStart, s.valuesIndex, s.valuesFile)
		err = f(keyData, valueBuffer)
		if err != nil {
			return nil, err
		}
	}

}

func (s *ExtentKeyValStore) Delete(key []byte) error {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()

	state, exists := s.cache[string(key)]
	if exists && !state {
		return errors.New("key not found")
	}

	//Write the key to the keys data file
	keyPos, err := s.keysFile.Seek(0, 2)
	if err != nil {
		return err
	}
	keySize, err := s.keysFile.Write(key)
	if err != nil {
		return err
	}

	//Check the written key size is the same as the key size
	if keySize != len(key) {
		panic("Key size mismatch")
	}

	//Overwrite the current key index with a tombstone
	keyTombstone := -keyPos
	eofKeyIndex, err := s.keysIndex.Seek(0, 2)
	if err != nil {
		return err
	}
	keyIndexStart := eofKeyIndex - 8
	_, err = s.keysIndex.Seek(keyIndexStart, 0)
	if err != nil {
		return err
	}
	err = binary.Write(s.keysIndex, binary.BigEndian, keyTombstone)
	//fmt.Printf("Wrote tombstone at index %d, position is now %d\n", keyIndexStart/8, keyTombstone)

	nextKeyPos :=keyPos+int64(keySize)
	keyFileLength, err := s.keysFile.Seek(0, 2)
	if err != nil {
		return err
	}
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
	if err != nil {
		return err
	}
	valueSize, err := s.valuesFile.Write(key)
	if err != nil {
		return err
	}

	//Overwrite the current key with a tombstone
	valueTombstone := -valuePos
	eofValueIndex, err := s.valuesIndex.Seek(0, 2)
	if err != nil {
		return err
	}
	valueIndexStart := eofValueIndex - 8
	_, err = s.valuesIndex.Seek(valueIndexStart, 0)
	if err != nil {
		return err
	}
	err = binary.Write(s.valuesIndex, binary.BigEndian, valueTombstone)

	//Write the next value index (the end of the value data file)
	nextValuePos :=valuePos+int64(valueSize)
	valueFileLength, err := s.valuesFile.Seek(0, 2)
	if err != nil {
		return err
	}
	if nextValuePos != valueFileLength {
		panic("Value file is corrupt")
	}

	s.valuesIndex.Seek(0, 2)
	err = binary.Write(s.valuesIndex, binary.BigEndian, nextValuePos)
	if err != nil {
		return err
	}


	/*
	fmt.Println("Dumping keys")
	s.LockFreeMapFunc(func(k []byte, v []byte) error {
		fmt.Printf("Key: %s\n", k)
		return nil
	})
	*/

	s.cache[string(key)] = false

	return nil
}

func (s *ExtentKeyValStore) Exists(key []byte) bool {
	s.globalLock.Lock()
	defer s.globalLock.Unlock()
	state, exists := s.cache[string(key)]
	if exists {
		return state
	}
	found, err := searchDbForKeyExists(key, s.keysIndex, s.keysFile)
	if err == nil && found {
		return true
	}
	return false
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