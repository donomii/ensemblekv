package ensemblekv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	bolt "go.etcd.io/bbolt"
)

func compactionKeys(store KvLike) ([][]byte, error) {
	var keys [][]byte
	err := forEachLiveKey(store, func(key []byte) error {
		keys = append(keys, bytes.Clone(key))
		return nil
	})
	return keys, err
}

func streamKeyRecords(store KvLike, visit func([]byte, bool) error) error {
	return streamKeyRecordsAt(store, "", visit)
}

func streamKeyRecordsAt(store KvLike, scratchParent string, visit func([]byte, bool) error) error {
	switch source := store.(type) {
	case *SingleFileLSM:
		return source.streamKeyRecords(visit)
	case *ExtentKeyValStore:
		return streamExtentKeys(source, visit)
	case *BoltDbShim:
		return source.boltHandle.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("Blocks"))
			if bucket == nil {
				return fmt.Errorf("Bolt compaction source has no Blocks bucket")
			}
			return bucket.ForEach(func(key, value []byte) error {
				if value == nil {
					return fmt.Errorf("Bolt compaction source contains nested bucket %q", key)
				}
				return visit(key, true)
			})
		})
	case *SQLiteKV:
		source.mu.RLock()
		defer source.mu.RUnlock()
		rows, err := source.db.Query("SELECT key FROM kv_store")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var key []byte
			if err := rows.Scan(&key); err != nil {
				return err
			}
			if err := visit(key, true); err != nil {
				return err
			}
		}
		return rows.Err()
	case *JsonKV:
		source.mutex.RLock()
		defer source.mutex.RUnlock()
		for key := range source.data {
			if err := visit([]byte(key), true); err != nil {
				return err
			}
		}
		return nil
	case *PudgeShim:
		if _, err := source.db.FileSize(); err != nil {
			return fmt.Errorf("Pudge compaction source %s: %w", source.filename, err)
		}
		for offset := 0; ; offset += 256 {
			keys, err := source.db.Keys(nil, 256, offset, true)
			if err != nil {
				return err
			}
			for _, key := range keys {
				if err := visit(key, true); err != nil {
					return err
				}
			}
			if len(keys) < 256 {
				return nil
			}
		}
	case *MmapSingleKV:
		return streamMmapKeys(source, visit)
	case *MegaPool:
		return streamMegaKeys(source, scratchParent, visit)
	case *EnsembleKv:
		return streamEnsembleKeys(source, scratchParent, visit)
	case *Linelsm:
		return streamLineKeys(source, scratchParent, visit)
	case *TreeLSM:
		return streamTreeKeys(source, scratchParent, visit)
	case *StarLSM:
		return streamStarKeys(source, scratchParent, visit)
	case KeyRecordStreamer:
		return source.StreamKeyRecords(visit)
	default:
		return fmt.Errorf("compaction requires bounded checked key enumeration; backend %T must implement StreamKeyRecords", store)
	}
}

func streamExtentKeys(source *ExtentKeyValStore, visit func([]byte, bool) error) error {
	source.globalLock.Lock()
	defer source.globalLock.Unlock()
	files := [4]*os.File{source.keysIndex, source.valuesIndex, source.keysFile, source.valuesFile}
	var sizes [4]int64
	for index, file := range files {
		stat, err := file.Stat()
		if err != nil {
			return fmt.Errorf("Extent compaction source %s: %w", file.Name(), err)
		}
		sizes[index] = stat.Size()
	}
	if sizes[0] != sizes[1] || sizes[0] < 8 || sizes[0]%8 != 0 {
		return fmt.Errorf("Extent compaction requires matching complete indexes; key bytes=%d value bytes=%d", sizes[0], sizes[1])
	}
	var previous [2]int64
	var deleted bool
	for position := int64(0); position < sizes[0]; position += 8 {
		var offsets [2]int64
		var keyDeleted bool
		for index, file := range files[:2] {
			var encoded [8]byte
			if _, err := file.ReadAt(encoded[:], position); err != nil {
				return fmt.Errorf("read Extent compaction index %s at %d: %w", file.Name(), position, err)
			}
			offset := int64(binary.BigEndian.Uint64(encoded[:]))
			if index == 0 {
				keyDeleted = offset < 0
			}
			if offset < 0 {
				offset = -offset
			}
			if offset < previous[index] || offset > sizes[index+2] || (position == 0 && offset != 0) {
				return fmt.Errorf("invalid Extent compaction offset %d in %s at %d after %d; data size=%d", offset, file.Name(), position, previous[index], sizes[index+2])
			}
			offsets[index] = offset
		}
		if position > 0 {
			key := make([]byte, offsets[0]-previous[0])
			if _, err := source.keysFile.ReadAt(key, previous[0]); err != nil {
				return err
			}
			if err := visit(key, !deleted); err != nil {
				return err
			}
		}
		previous = offsets
		deleted = keyDeleted
	}
	if previous != [2]int64{sizes[2], sizes[3]} {
		return fmt.Errorf("Extent compaction EOF offsets %v differ from data sizes %v", previous, sizes[2:])
	}
	return nil
}

func streamMmapKeys(source *MmapSingleKV, visit func([]byte, bool) error) error {
	source.mutex.RLock()
	defer source.mutex.RUnlock()
	stat, err := source.file.Stat()
	if err != nil {
		return err
	}
	if source.offset < 0 || stat.Size() < int64(len(source.data)) || source.offset > int64(len(source.data)) {
		return fmt.Errorf("MmapSingle compaction end %d exceeds file size %d or mapped size %d", source.offset, stat.Size(), len(source.data))
	}
	for key, offset := range source.index {
		if offset < 0 || offset > source.offset-headerSize {
			return fmt.Errorf("MmapSingle key %q has invalid record offset %d", key, offset)
		}
		keyLength := int64(binary.NativeEndian.Uint32(source.data[offset+8 : offset+12]))
		valueLength := int64(binary.NativeEndian.Uint32(source.data[offset+12 : offset+16]))
		if keyLength != int64(len(key)) || valueLength == int64(deletedMarker) || keyLength+valueLength > source.offset-offset-headerSize {
			return fmt.Errorf("MmapSingle key %q has invalid lengths key=%d value=%d at %d", key, keyLength, valueLength, offset)
		}
		if !bytes.Equal([]byte(key), source.data[offset+headerSize:offset+headerSize+keyLength]) {
			return fmt.Errorf("MmapSingle index key %q differs from its record at %d", key, offset)
		}
		if err := visit([]byte(key), true); err != nil {
			return err
		}
	}
	if source.offset+headerSize <= int64(len(source.data)) {
		if !bytes.Equal(source.data[source.offset:source.offset+headerSize], make([]byte, headerSize)) {
			return fmt.Errorf("MmapSingle compaction found an unread record at %d", source.offset)
		}
	}
	return nil
}

func streamMegaKeys(source *MegaPool, scratchParent string, visit func([]byte, bool) error) (result error) {
	source.mu.RLock()
	defer source.mu.RUnlock()
	if source.data == nil {
		return fmt.Errorf("MegaPool compaction source is closed")
	}
	stat, err := source.file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() < int64(len(source.data)) {
		return fmt.Errorf("MegaPool compaction file size %d is shorter than mapped size %d", stat.Size(), len(source.data))
	}
	pending, err := newDiskKeyIndex(scratchParent)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, pending.close()) }()
	// The tree can be unbalanced or corrupt; keep both the traversal queue and visited nodes on disk.
	err = pending.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("queue"))
		return err
	})
	if err != nil {
		return err
	}
	var sequence uint64
	enqueue := func(tx *bolt.Tx, offset int64) error {
		if offset == 0 {
			return nil
		}
		var encoded, order [8]byte
		binary.BigEndian.PutUint64(encoded[:], uint64(offset))
		visited := tx.Bucket(diskKeyBucket)
		if visited.Get(encoded[:]) != nil {
			return fmt.Errorf("MegaPool compaction found repeated node offset %d", offset)
		}
		if err := visited.Put(encoded[:], []byte{1}); err != nil {
			return err
		}
		binary.BigEndian.PutUint64(order[:], sequence)
		sequence++
		return tx.Bucket([]byte("queue")).Put(order[:], encoded[:])
	}
	if err := pending.db.Update(func(tx *bolt.Tx) error { return enqueue(tx, source.header.BtreeRoot) }); err != nil {
		return err
	}
	for {
		var key []byte
		var found bool
		err := pending.db.Update(func(tx *bolt.Tx) error {
			queue := tx.Bucket([]byte("queue"))
			order, encoded := queue.Cursor().First()
			if order == nil {
				return nil
			}
			offset := int64(binary.BigEndian.Uint64(encoded))
			node := source.nodeAt(offset)
			if node == nil {
				return fmt.Errorf("MegaPool compaction found invalid node offset %d", offset)
			}
			mappedSize := int64(len(source.data))
			if !compactionMappedRange(mappedSize, node.KeyOffset, node.KeyLen) ||
				!compactionMappedRange(mappedSize, node.DataOffset, node.DataLen) {
				return fmt.Errorf("MegaPool compaction node %d has invalid key or value bounds", offset)
			}
			if node.KeyLen > 0 {
				key = source.data[node.KeyOffset : node.KeyOffset+node.KeyLen]
			}
			found = true
			if err := queue.Delete(order); err != nil {
				return err
			}
			if err := enqueue(tx, node.Left); err != nil {
				return err
			}
			return enqueue(tx, node.Right)
		})
		if err != nil || !found {
			return err
		}
		if err := visit(key, true); err != nil {
			return err
		}
	}
}

func compactionMappedRange(size, offset, length int64) bool {
	if length == 0 {
		return offset >= -1 && offset <= size
	}
	return length > 0 && offset >= 0 && offset <= size && length <= size-offset
}
