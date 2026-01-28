package ensemblekv

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	MegaMagic      = 0x4D504B56 // "MPKV"
	MegaHeaderSize = 256        // Fixed header size
)

// MegaHeader represents the fixed-size header at the start of the file.
type MegaHeader struct {
	Magic     int64
	Size      int64
	Reserved  int64
	BtreeRoot int64
	StartFree int64
	Padding   [216]byte // Padding to reach 256 bytes (256 - 5*8 = 216)
}

// MegaNode represents a node in the BST.
// Standardized to use explicit offsets (int64).
type MegaNode struct {
	Bumper     int64
	KeyOffset  int64
	KeyLen     int64
	DataOffset int64
	DataLen    int64
	Right      int64
	Left       int64
	BumperL    int32
	_          int32 // Padding to align to 64 bytes
}

// MegaPool is the main structure for the KV store.
type MegaPool struct {
	file   *os.File
	data   []byte // mmapped data
	header *MegaHeader
	path   string
}

// OpenMegaPool opens or creates a MegaPool file.
func OpenMegaPool(path string, size int64) (*MegaPool, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	fileSize := info.Size()

	// Check if existing file (size > 0)
	isNew := false
	if fileSize == 0 {
		isNew = true
	}

	if size > 0 && fileSize != size {
		if size > fileSize {
			// Resize (grow) if requested size is larger
			// Resize (grow) if requested size is larger.
			if err := f.Truncate(size); err != nil {
				f.Close()
				return nil, err
			}
			fileSize = size
		} else {
			// Requested size is smaller. Do not shrink.
			// Trust valid file on disk.
		}
	} else if size == 0 && fileSize == 0 {
		// Default size if not specified and new file
		size = 10 * 1024 * 1024 // 10MB default
		if err := f.Truncate(size); err != nil {
			f.Close()
			return nil, err
		}
		fileSize = size
	} else if size == 0 {
		size = fileSize
	}

	// Mmap the file
	data, err := unix.Mmap(int(f.Fd()), 0, int(fileSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("mmap failed: %v", err)
	}

	pool := &MegaPool{
		file: f,
		data: data,
		path: path,
	}

	pool.header = (*MegaHeader)(unsafe.Pointer(&pool.data[0]))

	if isNew {
		// Initialize header
		pool.header.Magic = MegaMagic
		pool.header.Size = fileSize
		pool.header.BtreeRoot = 0
		pool.header.StartFree = int64(unsafe.Sizeof(MegaHeader{}))
	} else {
		if pool.header.Magic != MegaMagic {
			pool.Close()
			return nil, fmt.Errorf("invalid magic number: expected %x, got %x", MegaMagic, pool.header.Magic)
		}
	}

	return pool, nil
}

func (p *MegaPool) Close() error {
	if err := p.Flush(); err != nil {
		return err
	}
	if p.data != nil {
		if err := unix.Munmap(p.data); err != nil {
			return err
		}
		p.data = nil
	}
	if p.file != nil {
		return p.file.Close()
	}
	return nil
}

// Alloc reserves space in the pool and returns the offset.
// It DOES NOT handle resizing yet.
func (p *MegaPool) Alloc(size int64) (int64, error) {
	if size <= 0 {
		msg := fmt.Sprintf("invalid allocation size: %d", size)
		panic(msg)
	}

	start := p.header.StartFree
	newFree := start + size

	// Check for overflow and resize if needed
	if newFree > p.header.Size {
		// Calculate new size (at least double, or fit the new allocation)
		newSize := p.header.Size * 2
		if newFree > newSize {
			newSize = newFree + (1024 * 1024) // Add 1MB padding
		}

		if err := p.resize(newSize); err != nil {
			return 0, fmt.Errorf("failed to resize pool: %v", err)
		}
	}

	p.header.StartFree = newFree
	return start, nil
}

// resize expands the pool to the new size.
func (p *MegaPool) resize(newSize int64) error {
	// Sync current data before unmapping
	// (Optional but good for safety)
	// p.Flush()

	// 1. Unmap current data
	if err := unix.Munmap(p.data); err != nil {
		return err
	}
	p.data = nil
	p.header = nil // Invalidated

	// 2. Truncate file to new size
	if err := p.file.Truncate(newSize); err != nil {
		return err
	}

	// 3. Remap
	data, err := unix.Mmap(int(p.file.Fd()), 0, int(newSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return err
	}

	p.data = data
	p.header = (*MegaHeader)(unsafe.Pointer(&p.data[0]))
	p.header.Size = newSize // Update size in header

	return nil
}

// InsertData allocates space and copies data into it.
func (p *MegaPool) InsertData(data []byte) (int64, error) {
	if len(data) == 0 {
		panic("zero length data not allowed")
	}
	l := int64(len(data))
	offset, err := p.Alloc(l)
	if err != nil {
		return 0, err
	}

	copy(p.data[offset:], data)
	return offset, nil
}

// Helper to get a pointer to a node at a given offset
func (p *MegaPool) nodeAt(offset int64) *MegaNode {
	if offset <= 0 || offset >= int64(len(p.data)) {
		return nil
	}
	// Check if node fits
	if offset+int64(unsafe.Sizeof(MegaNode{})) > int64(len(p.data)) {
		return nil
	}
	// Unsafe casting to access struct at offset
	return (*MegaNode)(unsafe.Pointer(&p.data[offset]))
}

// Helper to read byte slice from offset
func (p *MegaPool) readBytes(offset, length int64) []byte {
	if offset <= 0 || length <= 0 || offset+length > int64(len(p.data)) {
		return nil
	}
	return p.data[offset : offset+length]
}

// Put adds or updates a key-value pair.
func (p *MegaPool) Put(key, value []byte) error {
	// Implement "write data first" policy
	keyOffset, err := p.InsertData(key)
	if err != nil {
		return err
	}
	valOffset := int64(-1)
	if len(value) > 0 {
		valOffset, err = p.InsertData(value)
		if err != nil {
			return err
		}
	}

	// Insert into tree
	root, err := p.insert(p.header.BtreeRoot, key, keyOffset, valOffset, int64(len(key)), int64(len(value)))
	if err != nil {
		return err
	}
	p.header.BtreeRoot = root
	return nil
}

// insert recursive function
func (p *MegaPool) insert(nodeOffset int64, key []byte, keyOff, valOff, keyLen, valLen int64) (int64, error) {
	if nodeOffset == 0 {
		// New Node
		nodeSize := int64(unsafe.Sizeof(MegaNode{}))
		newOffset, err := p.Alloc(nodeSize)
		if err != nil {
			return 0, err
		}

		node := p.nodeAt(newOffset)
		node.KeyOffset = keyOff
		node.KeyLen = keyLen
		node.DataOffset = valOff
		node.DataLen = valLen
		node.Left = 0
		node.Right = 0
		node.Bumper = 0
		node.BumperL = 0

		return newOffset, nil
	}

	node := p.nodeAt(nodeOffset)
	if node == nil {
		return 0, errors.New("corrupted tree: invalid node offset")
	}

	nodeKey := p.readBytes(node.KeyOffset, node.KeyLen)
	cmp := bytes.Compare(key, nodeKey)

	if cmp < 0 {
		left, err := p.insert(node.Left, key, keyOff, valOff, keyLen, valLen)
		if err != nil {
			return 0, err
		}
		node.Left = left
	} else if cmp > 0 {
		right, err := p.insert(node.Right, key, keyOff, valOff, keyLen, valLen)
		if err != nil {
			return 0, err
		}
		node.Right = right
	} else {
		// Update existing node
		// "Update pointers last" - we update the DataOffset/Len
		node.DataOffset = valOff
		node.DataLen = valLen
		// Note: We are NOT freeing the old data. This is append-only.
	}

	return nodeOffset, nil
}

// Get retrieves a value by key.
func (p *MegaPool) Get(key []byte) ([]byte, error) {
	nodeOffset := p.search(p.header.BtreeRoot, key)
	if nodeOffset == 0 {
		return nil, os.ErrNotExist // Not found
	}
	node := p.nodeAt(nodeOffset)
	// Return a copy to be safe, or direct slice if read-only is fine.
	// Returning copy is safer for general usage.
	data := p.readBytes(node.DataOffset, node.DataLen)
	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}

func (p *MegaPool) search(nodeOffset int64, key []byte) int64 {
	if nodeOffset == 0 {
		return 0
	}
	node := p.nodeAt(nodeOffset)
	if node == nil {
		return 0
	}

	nodeKey := p.readBytes(node.KeyOffset, node.KeyLen)
	cmp := bytes.Compare(key, nodeKey)

	if cmp < 0 {
		return p.search(node.Left, key)
	} else if cmp > 0 {
		return p.search(node.Right, key)
	} else {
		return nodeOffset
	}
}

// Exists checks if a key exists in the store.
func (p *MegaPool) Exists(key []byte) bool {
	return p.search(p.header.BtreeRoot, key) != 0
}

// Size returns the total size of the pool file.
func (p *MegaPool) Size() int64 {
	return p.header.Size
}

// Flush syncs the mmapped data to disk.
func (p *MegaPool) Flush() error {
	if p.data == nil {
		return errors.New("pool is closed")
	}
	// syscall.Msync is not always available or consistent effectively across platforms in pure Go without cgo or unsafe mostly.
	// However, since we have the file descriptor, we can confirm the file is synced.
	// Mapped memory sync is ... tricky.
	// Use the specific MS_SYNC flag constants if needed, but for now, rely on OS or implementing a specific helper.
	// Actually, simple way:
	// Use unix.Msync
	return unix.Msync(p.data, unix.MS_SYNC)
}

// MapFunc applies a function to all key-value pairs.
func (p *MegaPool) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	processed := make(map[string]bool)
	err := p.iterate(p.header.BtreeRoot, func(key, val []byte) error {
		if err := f(key, val); err != nil {
			return err
		}
		processed[string(key)] = true
		return nil
	})
	return processed, err
}

// MapPrefixFunc applies a function to key-value pairs with a prefix.
func (p *MegaPool) MapPrefixFunc(prefix []byte, f func([]byte, []byte) error) (map[string]bool, error) {
	processed := make(map[string]bool)
	err := p.iterate(p.header.BtreeRoot, func(key, val []byte) error {
		if bytes.HasPrefix(key, prefix) {
			if err := f(key, val); err != nil {
				return err
			}
			processed[string(key)] = true
		}
		return nil
	})
	return processed, err
}

// Keys returns all keys in the store.
func (p *MegaPool) Keys() [][]byte {
	var keys [][]byte
	p.iterate(p.header.BtreeRoot, func(key, val []byte) error {
		// Need to copy key because the slice points to mmap
		k := make([]byte, len(key))
		copy(k, key)
		keys = append(keys, k)
		return nil
	})
	return keys
}

// KeyHistory returns the history of a key. MegaPool only stores current value.
func (p *MegaPool) KeyHistory(key []byte) ([][]byte, error) {
	val, err := p.Get(key)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil // Not found
	}
	return [][]byte{val}, nil
}

// DumpIndex prints the tree structure to stdout (for debugging).
func (p *MegaPool) DumpIndex() error {
	fmt.Println("MegaPool Index Dump:")
	return p.iterate(p.header.BtreeRoot, func(key, val []byte) error {
		fmt.Printf("Key: %s, ValueLen: %d\n", string(key), len(val))
		return nil
	})
}

// iterate performs an in-order traversal of the tree.
func (p *MegaPool) iterate(nodeOffset int64, f func([]byte, []byte) error) error {
	if nodeOffset == 0 {
		return nil
	}
	node := p.nodeAt(nodeOffset)
	if node == nil {
		return errors.New("invalid node offset in iteration")
	}

	// Left
	if err := p.iterate(node.Left, f); err != nil {
		return err
	}

	// Current
	key := p.readBytes(node.KeyOffset, node.KeyLen)
	val := p.readBytes(node.DataOffset, node.DataLen)
	if err := f(key, val); err != nil {
		return err
	}

	// Right
	if err := p.iterate(node.Right, f); err != nil {
		return err
	}

	return nil
}

// Delete removes a key and its value from the store.
func (p *MegaPool) Delete(key []byte) error {
	root, err := p.deleteNode(p.header.BtreeRoot, key)
	if err != nil {
		return err
	}
	p.header.BtreeRoot = root
	return nil
}

func (p *MegaPool) deleteNode(nodeOffset int64, key []byte) (int64, error) {
	if nodeOffset == 0 {
		return 0, nil // Not found, nothing to delete
	}
	node := p.nodeAt(nodeOffset)
	if node == nil {
		return 0, errors.New("corrupt tree: invalid node offset in delete")
	}

	nodeKey := p.readBytes(node.KeyOffset, node.KeyLen)
	cmp := bytes.Compare(key, nodeKey)

	if cmp < 0 {
		left, err := p.deleteNode(node.Left, key)
		if err != nil {
			return 0, err
		}
		node.Left = left
		return nodeOffset, nil
	} else if cmp > 0 {
		right, err := p.deleteNode(node.Right, key)
		if err != nil {
			return 0, err
		}
		node.Right = right
		return nodeOffset, nil
	} else {
		// Found node to delete
		// Case 1: No children
		if node.Left == 0 && node.Right == 0 {
			return 0, nil
		}
		// Case 2: One child
		if node.Left == 0 {
			return node.Right, nil
		}
		if node.Right == 0 {
			return node.Left, nil
		}

		// Case 3: Two children
		// Find successor (min in right subtree)
		successor, _ := p.findMin(node.Right)
		if successor == nil {
			return 0, errors.New("corrupt tree: could not find successor")
		}

		// Read successor key/data
		succKey := p.readBytes(successor.KeyOffset, successor.KeyLen)

		// Copy key/data offsets from successor to current node
		node.KeyOffset = successor.KeyOffset
		node.KeyLen = successor.KeyLen
		node.DataOffset = successor.DataOffset
		node.DataLen = successor.DataLen

		// We need a copy of succKey because we are about to traverse the tree where it lives
		keyCopy := make([]byte, len(succKey))
		copy(keyCopy, succKey)

		// Delete successor from right subtree
		newRight, err := p.deleteNode(node.Right, keyCopy)
		if err != nil {
			return 0, err
		}
		node.Right = newRight
		return nodeOffset, nil
	}
}

func (p *MegaPool) findMin(nodeOffset int64) (*MegaNode, int64) {
	if nodeOffset == 0 {
		return nil, 0
	}
	node := p.nodeAt(nodeOffset)
	if node == nil {
		return nil, 0
	}
	if node.Left == 0 {
		return node, nodeOffset
	}
	return p.findMin(node.Left)
}
