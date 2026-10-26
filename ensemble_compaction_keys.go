package ensemblekv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strings"

	bolt "go.etcd.io/bbolt"
)

func compactionKeys(store KvLike) ([][]byte, error) {
	switch source := store.(type) {
	case *SingleFileLSM:
		return source.keysForCompaction()
	case *ExtentKeyValStore:
		return extentCompactionKeys(source)
	case *BoltDbShim:
		var keys [][]byte
		err := source.boltHandle.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("Blocks"))
			if bucket == nil {
				return fmt.Errorf("Bolt compaction source has no Blocks bucket")
			}
			return bucket.ForEach(func(key, value []byte) error {
				if value == nil {
					return fmt.Errorf("Bolt compaction source contains nested bucket %q", key)
				}
				keys = append(keys, bytes.Clone(key))
				return nil
			})
		})
		return keys, err
	case *SQLiteKV:
		names, err := source.List()
		return compactionByteKeys(names), err
	case *JsonKV:
		return source.Keys(), nil
	case *PudgeShim:
		if _, err := source.db.FileSize(); err != nil {
			return nil, fmt.Errorf("Pudge compaction source %s: %w", source.filename, err)
		}
		return source.db.Keys(nil, 0, 0, true)
	case *MmapSingleKV:
		return mmapCompactionKeys(source)
	case *MegaPool:
		return megaCompactionKeys(source)
	case *EnsembleKv:
		return ensembleCompactionKeys(source)
	case *Linelsm:
		return lineCompactionKeys(source)
	case *TreeLSM:
		source.mutex.RLock()
		defer source.mutex.RUnlock()
		if !source.isReadOnly {
			return compactionKeys(source.currentStore)
		}
		if len(source.subStores) != 16 {
			return nil, fmt.Errorf("TreeLSM compaction source %s has %d children; a completed split requires 16", source.directory, len(source.subStores))
		}
		for partition := 0; partition < 16; partition++ {
			if source.subStores[fmt.Sprintf("%s%x", source.prefix, partition)] == nil {
				return nil, fmt.Errorf("TreeLSM compaction source %s is missing child %s%x", source.directory, source.prefix, partition)
			}
		}
		var keys [][]byte
		for prefix, child := range source.subStores {
			childKeys, err := compactionKeys(child)
			if err != nil {
				return nil, fmt.Errorf("TreeLSM child %q: %w", prefix, err)
			}
			keys = append(keys, childKeys...)
		}
		return keys, nil
	case *StarLSM:
		source.mutex.RLock()
		defer source.mutex.RUnlock()
		if !source.isReadOnly {
			return compactionKeys(source.currentStore)
		}
		if len(source.subStores) != 16 {
			return nil, fmt.Errorf("StarLSM compaction source %s has %d children; a completed split requires 16", source.directory, len(source.subStores))
		}
		for partition := 0; partition < 16; partition++ {
			if source.subStores[fmt.Sprintf("%s%x", source.prefix, partition)] == nil {
				return nil, fmt.Errorf("StarLSM compaction source %s is missing child %s%x", source.directory, source.prefix, partition)
			}
		}
		var keys [][]byte
		for prefix, child := range source.subStores {
			childKeys, err := compactionKeys(child)
			if err != nil {
				return nil, fmt.Errorf("StarLSM child %q: %w", prefix, err)
			}
			keys = append(keys, childKeys...)
		}
		return keys, nil
	case interface{ KeysWithError() ([][]byte, error) }:
		return source.KeysWithError()
	default:
		return nil, fmt.Errorf("compaction requires reliable key enumeration; backend %T must implement KeysWithError", store)
	}
}

func compactionByteKeys(names []string) [][]byte {
	keys := make([][]byte, len(names))
	for index, name := range names {
		keys[index] = []byte(name)
	}
	return keys
}

func ensembleCompactionKeys(source *EnsembleKv) ([][]byte, error) {
	if !source.running.Load() {
		return nil, fmt.Errorf("Ensemble compaction source %s is closed", source.directory)
	}
	var keys [][]byte
	for index := int64(0); index < source.N; index++ {
		var childKeys [][]byte
		err := source.runInSubstoreWithErr(int(index), func(child KvLike) error {
			var err error
			childKeys, err = compactionKeys(child)
			return err
		})
		if err != nil {
			return nil, fmt.Errorf("Ensemble substore %d: %w", index, err)
		}
		keys = append(keys, childKeys...)
	}
	return keys, nil
}

func lineCompactionKeys(source *Linelsm) ([][]byte, error) {
	source.mutex.RLock()
	defer source.mutex.RUnlock()
	states := make(map[string]bool)
	for level, tier := range source.tiers {
		for index, child := range tier {
			keys, err := compactionKeys(child)
			if err != nil {
				return nil, fmt.Errorf("LineLSM tier %d store %d: %w", level, index, err)
			}
			for _, key := range keys {
				if strings.HasPrefix(string(key), tombstonePrefix) {
					name := string(key[len(tombstonePrefix):])
					if _, seen := states[name]; !seen {
						states[name] = false
					}
				}
			}
			for _, key := range keys {
				if strings.HasPrefix(string(key), dataPrefix) {
					name := string(key[len(dataPrefix):])
					if _, seen := states[name]; !seen {
						states[name] = true
					}
				}
			}
		}
	}
	var keys [][]byte
	for key, exists := range states {
		if exists {
			keys = append(keys, []byte(key))
		}
	}
	return keys, nil
}

func extentCompactionKeys(source *ExtentKeyValStore) ([][]byte, error) {
	source.globalLock.Lock()
	defer source.globalLock.Unlock()
	files := [4]*os.File{source.keysIndex, source.valuesIndex, source.keysFile, source.valuesFile}
	var sizes [4]int64
	for index, file := range files {
		stat, err := file.Stat()
		if err != nil {
			return nil, fmt.Errorf("Extent compaction source %s: %w", file.Name(), err)
		}
		sizes[index] = stat.Size()
	}
	if sizes[0] != sizes[1] || sizes[0] < 8 || sizes[0]%8 != 0 {
		return nil, fmt.Errorf("Extent compaction requires matching complete indexes; key bytes=%d value bytes=%d", sizes[0], sizes[1])
	}
	states := make(map[string]bool)
	var previous [2]int64
	var deleted bool
	for position := int64(0); position < sizes[0]; position += 8 {
		var offsets [2]int64
		var keyDeleted bool
		for index, file := range files[:2] {
			var encoded [8]byte
			if _, err := file.ReadAt(encoded[:], position); err != nil {
				return nil, fmt.Errorf("read Extent compaction index %s at %d: %w", file.Name(), position, err)
			}
			offset := int64(binary.BigEndian.Uint64(encoded[:]))
			if index == 0 {
				keyDeleted = offset < 0
			}
			if offset < 0 {
				offset = -offset
			}
			if offset < previous[index] || offset > sizes[index+2] || (position == 0 && offset != 0) {
				return nil, fmt.Errorf("invalid Extent compaction offset %d in %s at %d after %d; data size=%d", offset, file.Name(), position, previous[index], sizes[index+2])
			}
			offsets[index] = offset
		}
		if position > 0 {
			key := make([]byte, offsets[0]-previous[0])
			if _, err := source.keysFile.ReadAt(key, previous[0]); err != nil {
				return nil, err
			}
			states[string(key)] = !deleted
		}
		previous = offsets
		deleted = keyDeleted
	}
	if previous != [2]int64{sizes[2], sizes[3]} {
		return nil, fmt.Errorf("Extent compaction EOF offsets %v differ from data sizes %v", previous, sizes[2:])
	}
	var keys [][]byte
	for key, exists := range states {
		if exists {
			keys = append(keys, []byte(key))
		}
	}
	return keys, nil
}

func mmapCompactionKeys(source *MmapSingleKV) ([][]byte, error) {
	source.mutex.RLock()
	defer source.mutex.RUnlock()
	stat, err := source.file.Stat()
	if err != nil {
		return nil, err
	}
	if source.offset < 0 || stat.Size() < int64(len(source.data)) || source.offset > int64(len(source.data)) {
		return nil, fmt.Errorf("MmapSingle compaction end %d exceeds file size %d or mapped size %d", source.offset, stat.Size(), len(source.data))
	}
	keys := make([][]byte, 0, len(source.index))
	for key, offset := range source.index {
		if offset < 0 || offset > source.offset-headerSize {
			return nil, fmt.Errorf("MmapSingle key %q has invalid record offset %d", key, offset)
		}
		keyLength := int64(binary.NativeEndian.Uint32(source.data[offset+8 : offset+12]))
		valueLength := int64(binary.NativeEndian.Uint32(source.data[offset+12 : offset+16]))
		if keyLength != int64(len(key)) || valueLength == int64(deletedMarker) || keyLength+valueLength > source.offset-offset-headerSize {
			return nil, fmt.Errorf("MmapSingle key %q has invalid lengths key=%d value=%d at %d", key, keyLength, valueLength, offset)
		}
		if !bytes.Equal([]byte(key), source.data[offset+headerSize:offset+headerSize+keyLength]) {
			return nil, fmt.Errorf("MmapSingle index key %q differs from its record at %d", key, offset)
		}
		keys = append(keys, []byte(key))
	}
	if source.offset+headerSize <= int64(len(source.data)) {
		if !bytes.Equal(source.data[source.offset:source.offset+headerSize], make([]byte, headerSize)) {
			return nil, fmt.Errorf("MmapSingle compaction found an unread record at %d", source.offset)
		}
	}
	return keys, nil
}

func megaCompactionKeys(source *MegaPool) ([][]byte, error) {
	source.mu.RLock()
	defer source.mu.RUnlock()
	if source.data == nil {
		return nil, fmt.Errorf("MegaPool compaction source is closed")
	}
	stat, err := source.file.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() < int64(len(source.data)) {
		return nil, fmt.Errorf("MegaPool compaction file size %d is shorter than mapped size %d", stat.Size(), len(source.data))
	}
	pending := []int64{source.header.BtreeRoot}
	visited := make(map[int64]bool)
	var keys [][]byte
	for len(pending) > 0 {
		offset := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if offset == 0 {
			continue
		}
		if visited[offset] {
			return nil, fmt.Errorf("MegaPool compaction found repeated node offset %d", offset)
		}
		visited[offset] = true
		node := source.nodeAt(offset)
		if node == nil {
			return nil, fmt.Errorf("MegaPool compaction found invalid node offset %d", offset)
		}
		mappedSize := int64(len(source.data))
		if !compactionMappedRange(mappedSize, node.KeyOffset, node.KeyLen) ||
			!compactionMappedRange(mappedSize, node.DataOffset, node.DataLen) {
			return nil, fmt.Errorf("MegaPool compaction node %d has invalid key or value bounds", offset)
		}
		key := []byte{}
		if node.KeyLen > 0 {
			key = bytes.Clone(source.data[node.KeyOffset : node.KeyOffset+node.KeyLen])
		}
		keys = append(keys, key)
		pending = append(pending, node.Left, node.Right)
	}
	return keys, nil
}

func compactionMappedRange(size, offset, length int64) bool {
	if length == 0 {
		return offset >= -1 && offset <= size
	}
	return length > 0 && offset >= 0 && offset <= size && length <= size-offset
}
