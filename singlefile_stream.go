package ensemblekv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

func (lsm *SingleFileLSM) streamKeyRecords(visit func([]byte, bool) error) error {
	lsm.mutex.RLock()
	defer lsm.mutex.RUnlock()
	if lsm.closed {
		return fmt.Errorf("SingleFileLSM compaction source %s is closed", lsm.filePath)
	}
	header, _, _, err := lsm.readPublishedHeader()
	if err != nil {
		return err
	}
	if int(header.numSSTables) != len(lsm.sstables) || header.indexOffset != lsm.indexOffset {
		return fmt.Errorf("SingleFileLSM compaction source loaded %d tables at index %d; header requires %d tables at index %d",
			len(lsm.sstables), lsm.indexOffset, header.numSSTables, header.indexOffset)
	}
	for index := range lsm.sstables {
		meta := &lsm.sstables[index]
		if err := lsm.validateSSTable(meta); err != nil {
			return fmt.Errorf("validate compaction SSTable %d: %w", index, err)
		}
		if err := lsm.streamSSTableKeys(meta, visit); err != nil {
			return fmt.Errorf("enumerate compaction SSTable %d: %w", index, err)
		}
	}
	for key, value := range lsm.memTable {
		if err := visit([]byte(key), value != nil); err != nil {
			return err
		}
	}
	return nil
}

func (lsm *SingleFileLSM) streamSSTableKeys(meta *sstableMetadata, visit func([]byte, bool) error) error {
	end := meta.offset + meta.size
	entries := 0
	var previousKey []byte
	var buffer [32 * 1024]byte
	for offset := meta.offset; offset < end; {
		if end-offset < 24 {
			return fmt.Errorf("SSTable at %d has %d trailing bytes; expected an extent header", meta.offset, end-offset)
		}
		var encoded [24]byte
		if _, err := lsm.file.ReadAt(encoded[:], offset); err != nil {
			return err
		}
		storedChecksum := binary.LittleEndian.Uint64(encoded[:8])
		keyLength := int64(binary.LittleEndian.Uint64(encoded[8:16]))
		valueLength := int64(binary.LittleEndian.Uint64(encoded[16:24]))
		if keyLength <= 0 || keyLength > maxKeySize || keyLength > end-offset-24 ||
			valueLength < tombstoneMarker || valueLength > maxValueSize || max(valueLength, 0) > end-offset-24-keyLength {
			return fmt.Errorf("invalid SSTable extent at %d: key length=%d value length=%d remaining=%d", offset, keyLength, valueLength, end-offset)
		}
		key := make([]byte, keyLength)
		if _, err := lsm.file.ReadAt(key, offset+24); err != nil {
			return err
		}
		if (entries == 0 && !bytes.Equal(key, meta.minKey)) || (entries > 0 && bytes.Compare(previousKey, key) >= 0) {
			return fmt.Errorf("SSTable at %d has invalid key order at %d: previous=%q current=%q", meta.offset, offset, previousKey, key)
		}
		checksum := crc32.NewIEEE()
		checksum.Write(key)
		read, err := io.CopyBuffer(checksum, io.NewSectionReader(lsm.file, offset+24+keyLength, max(valueLength, 0)), buffer[:])
		if err != nil {
			return fmt.Errorf("checksum SSTable value at %d: %w", offset, err)
		}
		if read != max(valueLength, 0) {
			return fmt.Errorf("SSTable value at %d requires %d bytes, read %d: %w", offset, max(valueLength, 0), read, io.ErrUnexpectedEOF)
		}
		if uint64(checksum.Sum32()) != storedChecksum {
			return fmt.Errorf("SSTable extent at %d checksum mismatch: stored=%d computed=%d", offset, storedChecksum, checksum.Sum32())
		}
		if err := visit(key, valueLength != tombstoneMarker); err != nil {
			return err
		}
		previousKey = key
		entries++
		offset += 24 + keyLength + max(valueLength, 0)
	}
	if entries != meta.numEntries || !bytes.Equal(previousKey, meta.maxKey) {
		return fmt.Errorf("SSTable at %d has entries=%d last key=%q; metadata requires entries=%d last key=%q", meta.offset, entries, previousKey, meta.numEntries, meta.maxKey)
	}
	return nil
}
