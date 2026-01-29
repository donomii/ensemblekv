package ensemblekv

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	MegaMagic      = 0x4D504B56 // "MPKV"
	MegaHeaderSize = 256        // Fixed header size
	MaxDepth       = 10000
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
	BumperL    int64
}

// MegaPool is the main structure for the KV store.
type MegaPool struct {
	mu     sync.RWMutex
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
		size = 1 * 1024 * 1024 // 1MB default
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
	fmt.Printf("MegaPool(%p).Close: locking\n", p)
	p.mu.Lock()
	defer func() { fmt.Printf("MegaPool(%p).Close: unlocking\n", p); p.mu.Unlock() }()

	if err := p.flush(); err != nil {
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
func (p *MegaPool) Alloc(size int64) (int64, error) {
	fmt.Printf("MegaPool(%p).Alloc: locking\n", p)
	p.mu.Lock()
	defer func() { fmt.Printf("MegaPool(%p).Alloc: unlocking\n", p); p.mu.Unlock() }()
	return p.alloc(size)
}

func (p *MegaPool) alloc(size int64) (int64, error) {
	if size <= 0 {
		msg := fmt.Sprintf("invalid allocation size: %d", size)
		panic(msg)
	}

	start := p.header.StartFree
	if start < int64(unsafe.Sizeof(MegaPool{})) {
		panic("free space start corrupted, freespace starts at " + strconv.FormatInt(start, 10))
	}
	if start > p.header.Size {
		panic("free space start corrupted, freespace starts at " + strconv.FormatInt(start, 10))
	}
	neededFileSize := start + size

	// Check for overflow and resize if needed
	if neededFileSize >= p.header.Size {
		// Calculate new size (at least double, or fit the new allocation)
		extendSize := size * 2
		if extendSize < 1024*1024 {
			extendSize = 1024 * 1024
		}
		newFileSize := p.header.Size + extendSize
		if neededFileSize > newFileSize {
			panic("Could not extend file")
		}

		if err := p.resize(newFileSize); err != nil {
			return 0, fmt.Errorf("failed to resize pool: %v", err)
		}
	}

	p.header.StartFree = neededFileSize
	return start, nil
}

// resize expands the pool to the new size.
func (p *MegaPool) resize(newSize int64) error {
	// Sync current data before unmapping
	// (Optional but good for safety)
	p.flush()

	// Check that the new size is greater than the current file size
	if newSize <= p.header.Size {
		panic("New size(" + strconv.FormatInt(newSize, 10) + ") is less than or equal to current size(" + strconv.FormatInt(p.header.Size, 10) + ")")
	}

	// 1. Unmap current data
	if err := unix.Munmap(p.data); err != nil {
		panic("Failed to unmap data file:" + p.path + ": " + err.Error())
	}
	p.data = nil
	p.header = nil // Invalidated

	// 2. Truncate file to new size

	if err := p.file.Truncate(newSize); err != nil {
		panic("Failed to truncate file:" + p.path + " to size:" + strconv.FormatInt(newSize, 10) + ": " + err.Error())
	}

	// 3. Remap
	data, err := unix.Mmap(int(p.file.Fd()), 0, int(newSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		panic("Failed to remap file:" + p.path + " to size:" + strconv.FormatInt(newSize, 10) + ": " + err.Error())
	}

	p.data = data
	p.header = (*MegaHeader)(unsafe.Pointer(&p.data[0]))
	p.header.Size = newSize // Update size in header
	p.flush()               //Make sure all these changes are safely written to disk

	return nil
}

// InsertData allocates space and copies data into it.
func (p *MegaPool) InsertData(data []byte) (int64, error) {
	fmt.Printf("MegaPool(%p).InsertData: locking\n", p)
	p.mu.Lock()
	defer func() { fmt.Printf("MegaPool(%p).InsertData: unlocking\n", p); p.mu.Unlock() }()
	return p.insertData(data)
}

func (p *MegaPool) insertData(data []byte) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}
	l := int64(len(data))
	offset, err := p.alloc(l)
	if err != nil {
		return 0, err
	}

	copy(p.data[offset:], data)
	return offset, nil
}

// Helper to get a pointer to a node at a given offset
func (p *MegaPool) nodeAt(offset int64) *MegaNode {
	// fmt.Printf("nodeAt %d\n", offset)
	if offset <= 0 {
		msg := fmt.Sprintf("corrupted tree: invalid node offset %d less than 0\n", offset)
		fmt.Printf(msg)
		panic(msg)
	}
	if offset >= int64(len(p.data)) {
		msg := fmt.Sprintf("corrupted tree: invalid node offset %d greater than file size %d\n", offset, p.header.Size)
		fmt.Printf(msg)
		panic(msg)
	}
	// Check if node fits
	if offset+int64(unsafe.Sizeof(MegaNode{})) > int64(len(p.data)) {
		msg := fmt.Sprintf("corrupted tree: invalid node offset %d greater than file size %d\n", offset, p.header.Size)
		fmt.Printf(msg)
		panic(msg)
	}
	// Unsafe casting to access struct at offset
	node := (*MegaNode)(unsafe.Pointer(&p.data[offset]))
	// Now access the members to make sure they are valid
	test := node.Left + node.Right + node.KeyOffset + node.DataOffset + node.DataLen + node.Bumper + node.BumperL + node.Bumper
	if test < 0 { // Golang :(
		msg := fmt.Sprintf("corrupted tree: invalid node offset %d less than 0\n", offset)
		fmt.Printf(msg)
		panic(msg)
	}
	if node.Bumper != ^node.BumperL {
		msg := fmt.Sprintf("corrupted tree: invalid node offset %d less than 0\n", offset)
		fmt.Printf(msg)
		panic(msg)
	}
	return node
}

// Helper to read byte slice from offset
func (p *MegaPool) readBytes(offset, length int64) []byte {
	// fmt.Printf("readBytes %d %d\n", offset, length)
	if offset <= 0 || length <= 0 || offset+length > int64(len(p.data)) {
		msg := fmt.Sprintf("corrupted tree: invalid node offset %d less than 0\n", offset)
		fmt.Printf(msg)
		panic(msg)
	}
	if length < 1 {
		msg := fmt.Sprintf("corrupted tree: invalid node offset %d less than 0\n", offset)
		fmt.Printf(msg)
		panic(msg)
	}
	return p.data[offset : offset+length]
}

// Put adds or updates a key-value pair.
func (p *MegaPool) Put(key, value []byte) error {
	fmt.Printf("MegaPool(%p).Put: locking\n", p)
	p.mu.Lock()
	defer func() { fmt.Printf("MegaPool(%p).Put: unlocking\n", p); p.mu.Unlock() }()
	var err error

	// Implement "write data first" policy
	keyOffset := int64(-1)
	if len(key) > 0 {
		keyOffset, err = p.insertData(key)
		if err != nil {
			return err
		}
	}
	valOffset := int64(-1)
	if len(value) > 0 {
		valOffset, err = p.insertData(value)
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

func (p *MegaPool) enforceBounds(nodeOffset int64) error {
	fmt.Printf("enforceBounds %d\n", nodeOffset)
	defer fmt.Printf("enforceBounds %d DONE\n", nodeOffset)
	node := p.nodeAt(nodeOffset)
	if node == nil {
		return nil
	}
	if node.Left < 0 {
		fmt.Printf("corrupted tree: left child of node at %d is invalid(%v)\n", nodeOffset, node.Left)
		node.Left = 0
	}
	if node.Right < 0 {
		fmt.Printf("corrupted tree: right child of node at %d is invalid(%v)\n", nodeOffset, node.Right)
		node.Right = 0
	}

	if node.Left >= p.header.Size-int64(unsafe.Sizeof(MegaNode{})) {
		fmt.Printf("corrupted tree: left child of node at %d is invalid(%v greater than file size %v)\n", nodeOffset, node.Left, p.header.Size)
		node.Left = 0
	}
	if node.Right >= p.header.Size-int64(unsafe.Sizeof(MegaNode{})) {
		fmt.Printf("corrupted tree: right child of node at %d is invalid(%v greater than file size %v)\n", nodeOffset, node.Right, p.header.Size)
		node.Right = 0
	}
	if node.KeyOffset+node.KeyLen >= p.header.Size {
		fmt.Printf("corrupted tree: key of node at %d is invalid(%v greater than file size %v)\n", nodeOffset, node.KeyOffset+node.KeyLen, p.header.Size)
		node.KeyOffset = 0
		node.KeyLen = 0
	}
	if node.DataOffset+node.DataLen >= p.header.Size {
		fmt.Printf("corrupted tree: data of node at %d is invalid(%v greater than file size %v)\n", nodeOffset, node.DataOffset+node.DataLen, p.header.Size)
		node.DataOffset = 0
		node.DataLen = 0
	}

	if node.KeyOffset < 0 {
		fmt.Printf("corrupted tree: key of node at %d is invalid(%v less than 0)\n", nodeOffset, node.KeyOffset)
		node.KeyOffset = 0
		node.KeyLen = 0
	}
	if node.DataOffset < 0 {
		fmt.Printf("corrupted tree: data of node at %d is invalid(%v less than 0)\n", nodeOffset, node.DataOffset)
		node.DataOffset = 0
		node.DataLen = 0
	}

	if node.Bumper != ^node.BumperL {
		fmt.Printf("corrupted tree: bumper mismatch at %d\n", nodeOffset)
		panic("corrupted tree: bumper mismatch")
	}
	return nil
}

// insert recursive function
func (p *MegaPool) insert(nodeOffset int64, key []byte, keyOff, valOff, keyLen, valLen int64) (int64, error) {
	if nodeOffset < 1 {
		// New Node
		nodeSize := int64(unsafe.Sizeof(MegaNode{}))
		newOffset, err := p.alloc(nodeSize)
		if err != nil {
			return 0, err
		}

		node := (*MegaNode)(unsafe.Pointer(&p.data[newOffset]))
		node.KeyOffset = keyOff
		node.KeyLen = keyLen
		node.DataOffset = valOff
		node.DataLen = valLen
		node.Left = 0
		node.Right = 0
		// Random bumper
		node.Bumper = rand.Int64()
		// Invert Bumper
		node.BumperL = ^node.Bumper
		return newOffset, nil
	}

	p.enforceBounds(nodeOffset)
	node := p.nodeAt(nodeOffset)
	if node == nil {
		return 0, errors.New("corrupted tree: invalid node offset " + strconv.FormatInt(nodeOffset, 10))
	}

	nodeKey := p.readBytes(node.KeyOffset, node.KeyLen)
	cmp := bytes.Compare(key, nodeKey)

	if cmp < 0 {
		left, err := p.insert(node.Left, key, keyOff, valOff, keyLen, valLen)
		if err != nil {
			return 0, err
		}
		p.nodeAt(nodeOffset).Left = left
		// Balance
		if p.nodeAt(left).Bumper > p.nodeAt(nodeOffset).Bumper {
			nodeOffset = p.rotateRight(nodeOffset)
		}
	} else if cmp > 0 {
		right, err := p.insert(node.Right, key, keyOff, valOff, keyLen, valLen)
		if err != nil {
			return 0, err
		}
		p.nodeAt(nodeOffset).Right = right
		// Balance
		if p.nodeAt(right).Bumper > p.nodeAt(nodeOffset).Bumper {
			nodeOffset = p.rotateLeft(nodeOffset)
		}
	} else {
		// Update existing node
		// "Update pointers last" - we update the DataOffset/Len
		node.DataOffset = valOff
		node.DataLen = valLen
		// Note: We are NOT freeing the old data. This is append-only.
	}

	return nodeOffset, nil
}

func (p *MegaPool) rotateRight(rootOffset int64) int64 {
	root := p.nodeAt(rootOffset)
	newRootOffset := root.Left
	newRoot := p.nodeAt(newRootOffset)

	root.Left = newRoot.Right
	newRoot.Right = rootOffset
	return newRootOffset
}

func (p *MegaPool) rotateLeft(rootOffset int64) int64 {
	root := p.nodeAt(rootOffset)
	newRootOffset := root.Right
	newRoot := p.nodeAt(newRootOffset)

	root.Right = newRoot.Left
	newRoot.Left = rootOffset
	return newRootOffset
}

// Get retrieves a value by key.
func (p *MegaPool) Get(key []byte) ([]byte, error) {
	fmt.Printf("MegaPool(%p).Get: rlocking\n", p)
	p.mu.RLock()
	defer func() { fmt.Printf("MegaPool(%p).Get: runlocking\n", p); p.mu.RUnlock() }()
	return p.get(key)
}

func (p *MegaPool) get(key []byte) ([]byte, error) {
	nodeOffset := p.search(p.header.BtreeRoot, key, 0)
	if nodeOffset < 1 {
		return nil, os.ErrNotExist // Not found
	}
	p.enforceBounds(nodeOffset)
	node := p.nodeAt(nodeOffset)
	// Return a copy to be safe, or direct slice if read-only is fine.
	// Returning copy is safer for general usage.
	if node.DataLen < 1 {
		return nil, nil
	}
	data := p.readBytes(node.DataOffset, node.DataLen)
	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}

func (p *MegaPool) search(nodeOffset int64, key []byte, depth int) int64 {
	fmt.Printf("search %d depth %d\n", nodeOffset, depth)
	defer fmt.Printf("search %d depth %d DONE\n", nodeOffset, depth)
	if depth > MaxDepth {
		panic("MegaPool cycle detected or tree too deep")
	}
	if nodeOffset < 1 {
		return 0
	}
	node := p.nodeAt(nodeOffset)
	if node == nil {
		return 0
	}

	if node.KeyLen < 1 {
		if len(key) < 1 {
			return nodeOffset
		}
		return 0
	}

	nodeKey := p.readBytes(node.KeyOffset, node.KeyLen)
	cmp := bytes.Compare(key, nodeKey)

	if cmp < 0 {
		return p.search(node.Left, key, depth+1)
	} else if cmp > 0 {
		return p.search(node.Right, key, depth+1)
	} else {
		return nodeOffset
	}
}

// Exists checks if a key exists in the store.
func (p *MegaPool) Exists(key []byte) bool {
	fmt.Printf("MegaPool(%p).Exists: rlocking\n", p)
	p.mu.RLock()
	defer func() { fmt.Printf("MegaPool(%p).Exists: runlocking\n", p); p.mu.RUnlock() }()
	return p.search(p.header.BtreeRoot, key, 0) != 0
}

// Size returns the total size of the pool file.
func (p *MegaPool) Size() int64 {
	fmt.Printf("MegaPool(%p).Size: rlocking\n", p)
	p.mu.RLock()
	defer func() { fmt.Printf("MegaPool(%p).Size: runlocking\n", p); p.mu.RUnlock() }()
	return p.header.Size
}

// Flush syncs the mmapped data to disk.
func (p *MegaPool) Flush() error {
	fmt.Printf("MegaPool(%p).Flush: locking\n", p)
	p.mu.Lock()
	defer func() { fmt.Printf("MegaPool(%p).Flush: unlocking\n", p); p.mu.Unlock() }()
	return p.flush()
}

func (p *MegaPool) flush() error {
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

	keys := p.Keys()

	processed := make(map[string]bool)
	for _, key := range keys {
		val, err := p.Get(key)
		if err != nil {
			return nil, err
		}
		if err := f(key, val); err != nil {
			return nil, err
		}
		processed[string(key)] = true
	}
	return processed, nil
}

// MapPrefixFunc applies a function to key-value pairs with a prefix.
func (p *MegaPool) MapPrefixFunc(prefix []byte, f func([]byte, []byte) error) (map[string]bool, error) {
	processed := make(map[string]bool)
	keys := p.Keys()
	for _, key := range keys {
		if bytes.HasPrefix(key, prefix) {
			val, err := p.Get(key)
			if err != nil {
				return nil, err
			}
			if err := f(key, val); err != nil {
				return nil, err
			}
			processed[string(key)] = true
		}
	}
	return processed, nil
}

// Keys returns all keys in the store.
func (p *MegaPool) Keys() [][]byte {
	fmt.Printf("MegaPool(%p).Keys: rlocking\n", p)
	p.mu.RLock()
	defer func() { fmt.Printf("MegaPool(%p).Keys: runlocking\n", p); p.mu.RUnlock() }()
	return p.keys()
}

func (p *MegaPool) keys() [][]byte {
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
	fmt.Printf("MegaPool(%p).KeyHistory: rlocking\n", p)
	p.mu.RLock()
	defer func() { fmt.Printf("MegaPool(%p).KeyHistory: runlocking\n", p); p.mu.RUnlock() }()
	val, err := p.get(key)
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
	fmt.Printf("MegaPool(%p).DumpIndex: rlocking\n", p)
	p.mu.RLock()
	defer func() { fmt.Printf("MegaPool(%p).DumpIndex: runlocking\n", p); p.mu.RUnlock() }()
	fmt.Println("MegaPool Index Dump:")
	return p.iterate(p.header.BtreeRoot, func(key, val []byte) error {
		fmt.Printf("Key: %s, ValueLen: %d\n", string(key), len(val))
		return nil
	})
}

// iterate performs an in-order traversal of the tree.
func (p *MegaPool) iterate(nodeOffset int64, f func([]byte, []byte) error) error {
	if nodeOffset < 1 {
		return nil
	}
	p.enforceBounds(nodeOffset)
	node := p.nodeAt(nodeOffset)
	if node == nil {
		return errors.New("invalid node offset in iteration")
	}

	// Left
	if err := p.iterate(node.Left, f); err != nil {
		return err
	}

	// Current
	var key, val []byte
	if node.KeyLen > 0 {
		key = p.readBytes(node.KeyOffset, node.KeyLen)
	}
	if node.DataLen > 0 {
		val = p.readBytes(node.DataOffset, node.DataLen)
	}
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
	fmt.Printf("MegaPool(%p).Delete: locking\n", p)
	p.mu.Lock()
	defer func() { fmt.Printf("MegaPool(%p).Delete: unlocking\n", p); p.mu.Unlock() }()
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
