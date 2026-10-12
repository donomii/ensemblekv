package ensemblekv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
)

const singleFileHeaderSlotSize = singleFileHeaderSize / 2

func encodeSingleFileHeader(header fileHeader) []byte {
	encoded := make([]byte, singleFileHeaderSlotSize)
	binary.LittleEndian.PutUint32(encoded[0:4], singleFileMagic)
	binary.LittleEndian.PutUint32(encoded[4:8], singleFileVersion)
	binary.LittleEndian.PutUint64(encoded[8:16], uint64(header.walOffset))
	binary.LittleEndian.PutUint64(encoded[16:24], uint64(header.sstableOffset))
	binary.LittleEndian.PutUint64(encoded[24:32], uint64(header.indexOffset))
	binary.LittleEndian.PutUint32(encoded[32:36], uint32(header.numSSTables))
	binary.LittleEndian.PutUint32(encoded[36:40], crc32.ChecksumIEEE(encoded[:36]))
	binary.LittleEndian.PutUint64(encoded[40:48], header.generation)
	binary.LittleEndian.PutUint32(encoded[48:52], singleFileGenerationChecksum(encoded))
	return encoded
}

func singleFileGenerationChecksum(encoded []byte) uint32 {
	// Including the legacy CRC in this checksum would cancel the protected header bytes through CRC's residue.
	return crc32.Update(crc32.ChecksumIEEE(encoded[:36]), crc32.IEEETable, encoded[40:48])
}

func decodeSingleFileHeader(encoded []byte, slot int) (fileHeader, error) {
	header := fileHeader{
		magic: binary.LittleEndian.Uint32(encoded[0:4]), version: binary.LittleEndian.Uint32(encoded[4:8]),
		walOffset:     int64(binary.LittleEndian.Uint64(encoded[8:16])),
		sstableOffset: int64(binary.LittleEndian.Uint64(encoded[16:24])),
		indexOffset:   int64(binary.LittleEndian.Uint64(encoded[24:32])),
		numSSTables:   int32(binary.LittleEndian.Uint32(encoded[32:36])),
		checksum:      binary.LittleEndian.Uint32(encoded[36:40]),
	}
	if header.magic != singleFileMagic || header.checksum != crc32.ChecksumIEEE(encoded[:36]) {
		return fileHeader{}, fmt.Errorf("header slot %d has invalid magic or checksum", slot)
	}
	if header.version == singleFileVersion {
		header.generation = binary.LittleEndian.Uint64(encoded[40:48])
		if header.generation == 0 || binary.LittleEndian.Uint32(encoded[48:52]) != singleFileGenerationChecksum(encoded) {
			return fileHeader{}, fmt.Errorf("header slot %d has invalid generation %d or generation checksum", slot, header.generation)
		}
	} else if header.version != 1 || slot != 0 {
		return fileHeader{}, fmt.Errorf("header slot %d has unsupported version %d", slot, header.version)
	}
	if header.walOffset < singleFileHeaderSize || header.sstableOffset <= header.walOffset ||
		header.numSSTables < 0 || (header.numSSTables == 0 && header.indexOffset != 0) ||
		(header.numSSTables > 0 && header.indexOffset < singleFileHeaderSize) {
		return fileHeader{}, fmt.Errorf("header slot %d has invalid WAL [%d,%d), index %d, table count %d",
			slot, header.walOffset, header.sstableOffset, header.indexOffset, header.numSSTables)
	}
	return header, nil
}

func (lsm *SingleFileLSM) readPublishedHeader() (fileHeader, int, bool, error) {
	var headers [2]fileHeader
	var failures [2]error
	for slot := range headers {
		var encoded [52]byte
		if _, err := lsm.file.ReadAt(encoded[:], int64(slot*singleFileHeaderSlotSize)); err != nil {
			failures[slot] = fmt.Errorf("read header slot %d: %w", slot, err)
		} else {
			headers[slot], failures[slot] = decodeSingleFileHeader(encoded[:], slot)
		}
	}
	if failures[0] != nil && failures[1] != nil {
		return fileHeader{}, 0, false, fmt.Errorf("SingleFileLSM %s has no valid header: %w", lsm.filePath, errors.Join(failures[0], failures[1]))
	}
	selected := 0
	if failures[0] != nil || (failures[1] == nil && headers[1].generation > headers[0].generation) {
		selected = 1
	}
	if failures[0] == nil && failures[1] == nil && headers[0].generation == headers[1].generation && headers[0] != headers[1] {
		return fileHeader{}, 0, false, fmt.Errorf("SingleFileLSM %s header generation %d has conflicting slots", lsm.filePath, headers[0].generation)
	}
	repair := failures[0] != nil || failures[1] != nil || headers[0] != headers[1]
	return headers[selected], selected, repair, nil
}

func (lsm *SingleFileLSM) writeHeaderSlot(header fileHeader, slot int) error {
	if _, err := lsm.file.WriteAt(encodeSingleFileHeader(header), int64(slot*singleFileHeaderSlotSize)); err != nil {
		return fmt.Errorf("write header generation %d slot %d: %w", header.generation, slot, err)
	}
	if err := lsm.file.Sync(); err != nil {
		return fmt.Errorf("sync header generation %d slot %d: %w", header.generation, slot, err)
	}
	return nil
}

func (lsm *SingleFileLSM) publishHeader(header fileHeader) error {
	if lsm.writeFailure != nil {
		return lsm.writeFailure
	}
	if lsm.headerGeneration == math.MaxUint64 {
		return fmt.Errorf("SingleFileLSM %s exhausted header generations", lsm.filePath)
	}
	header.generation = lsm.headerGeneration + 1
	first := 1 - lsm.headerSlot
	// Both copies must describe the replacement index before the WAL they replace can be reused.
	for _, slot := range []int{first, lsm.headerSlot} {
		if err := lsm.writeHeaderSlot(header, slot); err != nil {
			lsm.writeFailure = fmt.Errorf("SingleFileLSM %s header publication failed; reopen before writing: %w", lsm.filePath, err)
			return lsm.writeFailure
		}
	}
	lsm.headerGeneration, lsm.headerSlot = header.generation, first
	return nil
}
