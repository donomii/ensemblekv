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

	"runtime/debug"

	"golang.org/x/sys/unix"
)

func panicWithStack(err interface{}) {
	panic(fmt.Sprintf("MegaPool Panic: %v\nStack Trace:\n%s", err, debug.Stack()))
}

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
	Debug  bool
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
				panicWithStack(err)
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
			panicWithStack(err)
		}
		fileSize = size
	} else if size == 0 {
		size = fileSize
	}

	// Mmap the file
	data, err := unix.Mmap(int(f.Fd()), 0, int(fileSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		panicWithStack(fmt.Errorf("mmap failed: %v", err))
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
			panicWithStack(fmt.Errorf("invalid magic number: expected %x, got %x", MegaMagic, pool.header.Magic))
		}
	}

	return pool, nil
}

func (p *MegaPool) lock(name string) {
	if p.Debug {
		fmt.Printf("MegaPool(%p).%s: locking\n", p, name)
	}
	p.mu.Lock()
}

func (p *MegaPool) unlock(name string) {
	if p.Debug {
		fmt.Printf("MegaPool(%p).%s: unlocking\n", p, name)
	}
	p.mu.Unlock()
}

func (p *MegaPool) rlock(name string) {
	if p.Debug {
		fmt.Printf("MegaPool(%p).%s: rlocking\n", p, name)
	}
	p.mu.RLock()
}

func (p *MegaPool) runlock(name string) {
	if p.Debug {
		fmt.Printf("MegaPool(%p).%s: runlocking\n", p, name)
	}
	p.mu.RUnlock()
}

func (p *MegaPool) Close() error {
	p.lock("Close")
	defer p.unlock("Close")

	if p.data == nil {
		return nil
	}

	if err := p.flush(); err != nil {
		panicWithStack(err)
	}
	if p.data != nil {
		if err := unix.Munmap(p.data); err != nil {
			panicWithStack(err)
		}
		p.data = nil
	}
	if p.file != nil {
		if err := p.file.Close(); err != nil {
			panicWithStack(err)
		}
	}
	return nil
}

// Alloc reserves space in the pool and returns the offset.
func (p *MegaPool) Alloc(size int64) (int64, error) {
	p.lock("Alloc")
	defer p.unlock("Alloc")
	return p.alloc(size)
}

func (p *MegaPool) alloc(size int64) (int64, error) {
	if size <= 0 {
		msg := fmt.Sprintf("invalid allocation size: %d", size)
		panicWithStack(msg)
	}

	// Align to 8 bytes
	if p.header.StartFree%8 != 0 {
		p.header.StartFree += (8 - (p.header.StartFree % 8))
	}

	start := p.header.StartFree
	if start < MegaHeaderSize {
		// If StartFree is corrupted/zero/too small, reset to after header
		fmt.Printf("MegaPool: StartFree corrupted (%d), resetting to %d\n", start, MegaHeaderSize)
		start = MegaHeaderSize
	}
	if start > p.header.Size {
		panicWithStack("free space start corrupted, freespace starts at " + strconv.FormatInt(start, 10))
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
			panicWithStack("Could not extend file")
		}

		if err := p.resize(newFileSize); err != nil {
			panicWithStack(fmt.Errorf("failed to resize pool: %v", err))
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
		panicWithStack("New size(" + strconv.FormatInt(newSize, 10) + ") is less than or equal to current size(" + strconv.FormatInt(p.header.Size, 10) + ")")
	}

	// 1. Unmap current data
	if err := unix.Munmap(p.data); err != nil {
		panicWithStack("Failed to unmap data file:" + p.path + ": " + err.Error())
	}
	p.data = nil
	p.header = nil // Invalidated

	// 2. Truncate file to new size

	if err := p.file.Truncate(newSize); err != nil {
		panicWithStack("Failed to truncate file:" + p.path + " to size:" + strconv.FormatInt(newSize, 10) + ": " + err.Error())
	}

	// 3. Remap
	data, err := unix.Mmap(int(p.file.Fd()), 0, int(newSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		panicWithStack("Failed to remap file:" + p.path + " to size:" + strconv.FormatInt(newSize, 10) + ": " + err.Error())
	}

	p.data = data
	p.header = (*MegaHeader)(unsafe.Pointer(&p.data[0]))
	p.header.Size = newSize // Update size in header
	p.flush()               //Make sure all these changes are safely written to disk

	return nil
}

// InsertData allocates space and copies data into it.
func (p *MegaPool) InsertData(data []byte) (int64, error) {
	p.lock("InsertData")
	defer p.unlock("InsertData")
	return p.insertData(data)
}

func (p *MegaPool) insertData(data []byte) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}
	l := int64(len(data))
	offset, err := p.alloc(l)
	if err != nil {
		panicWithStack(err)
	}

	copy(p.data[offset:], data)
	return offset, nil
}

// Helper to get a pointer to a node at a given offset
func (p *MegaPool) nodeAt(offset int64) *MegaNode {
	//fmt.Printf("nodeAt %d\n", offset)
	if offset <= 0 {
		msg := fmt.Sprintf("corrupted tree: invalid node offset %d less than 0\n", offset)
		fmt.Print(msg)
		//panicWithStack(msg)
		return nil
	}
	if offset >= int64(len(p.data)) {
		msg := fmt.Sprintf("corrupted tree: invalid node offset %d greater than file size %d\n", offset, p.header.Size)
		fmt.Print(msg)
		//panicWithStack(msg)
		return nil
	}
	// Check if node fits
	if offset+int64(unsafe.Sizeof(MegaNode{})) > int64(len(p.data)) {
		msg := fmt.Sprintf("corrupted tree: invalid node offset %d greater than file size %d\n", offset, p.header.Size)
		fmt.Print(msg)
		//panicWithStack(msg)
		return nil
	}
	// Unsafe casting to access struct at offset
	node := (*MegaNode)(unsafe.Pointer(&p.data[offset]))
	// Now access the members to make sure they are valid
	test := node.Left + node.Right + node.KeyOffset + node.DataOffset + node.DataLen + node.Bumper + node.BumperL + node.Bumper
	if test < 0 { // Golang :(
		msg := fmt.Sprintf("corrupted tree: invalid node offset %d less than 0\n", offset)
		fmt.Print(msg)
		//panicWithStack(msg)
		return nil
	}
	if node.Bumper != ^node.BumperL {
		msg := fmt.Sprintf("corrupted tree: bumper mismatch at %d\n", offset)
		fmt.Print(msg)
		//panicWithStack(msg)
		return nil
	}
	return node
}

// Helper to read byte slice from offset
func (p *MegaPool) readBytes(offset, length int64) []byte {
	//fmt.Printf("readBytes %d %d\n", offset, length)
	if length == 0 {
		return nil
	}
	if offset <= 0 {
		msg := fmt.Sprintf("corrupted tree: invalid node offset %d less than 0\n", offset)
		fmt.Print(msg)
		panicWithStack(msg)
	}
	if length <= 0 {
		msg := fmt.Sprintf("corrupted tree: invalid length %d less than or equal to 0\n", length)
		fmt.Print(msg)
		panicWithStack(msg)
	}
	if offset+length > int64(len(p.data)) {
		msg := fmt.Sprintf("corrupted tree: invalid node range %d+%d greater than file size %d\n", offset, length, len(p.data))
		fmt.Print(msg)
		panicWithStack(msg)
	}
	return p.data[offset : offset+length]
}

func (p *MegaPool) flushRange(start, length int64) error {
	if start < 0 || length <= 0 || start+length > int64(len(p.data)) {
		return nil
	}
	pageSize := int64(os.Getpagesize())
	alignedStart := (start / pageSize) * pageSize
	end := start + length
	err := unix.Msync(p.data[alignedStart:end], unix.MS_SYNC)
	if err != nil {
		panicWithStack(err)
	}
	return nil
}

func (p *MegaPool) copyNode(offset int64) (int64, error) {
	if offset <= 0 {
		return 0, nil
	}
	nodeSize := int64(unsafe.Sizeof(MegaNode{}))
	newOffset, err := p.alloc(nodeSize)
	if err != nil {
		panicWithStack(err)
	}

	src := p.nodeAt(offset)
	if src == nil {
		return 0, fmt.Errorf("copyNode: invalid offset %d", offset)
	}
	// dst is uninitialized, so we cannot use nodeAt (which checks consistency)
	dst := (*MegaNode)(unsafe.Pointer(&p.data[newOffset]))

	// Refetch src in case alloc resized
	src = p.nodeAt(offset)

	*dst = *src
	return newOffset, nil
}

// Put adds or updates a key-value pair.
// Put adds or updates a key-value pair.
func (p *MegaPool) Put(key, value []byte) error {
	p.lock("Put")
	defer p.unlock("Put")
	var err error

	startSync := p.header.StartFree

	// Implement "write data first" policy
	keyOffset := int64(-1)
	if len(key) > 0 {
		keyOffset, err = p.insertData(key)
		if err != nil {
			panicWithStack(err)
		}
	}
	valOffset := int64(-1)
	if len(value) > 0 {
		valOffset, err = p.insertData(value)
		if err != nil {
			panicWithStack(err)
		}
	}

	// Insert into tree using COW
	root, err := p.insert(p.header.BtreeRoot, key, keyOffset, valOffset, int64(len(key)), int64(len(value)))
	if err != nil {
		panicWithStack(err)
	}

	// Flush new data
	endSync := p.header.StartFree
	if endSync > startSync {
		if err := p.flushRange(startSync, endSync-startSync); err != nil {
			panicWithStack(err)
		}
	}

	p.header.BtreeRoot = root

	// Flush header
	if err := p.flushRange(0, MegaHeaderSize); err != nil {
		panicWithStack(err)
	}
	return nil
}

func (p *MegaPool) enforceBounds(nodeOffset int64) error {
	node := p.nodeAt(nodeOffset)
	if node == nil {
		return nil
	}
	if node.Left < 0 {
		fmt.Printf("corrupted tree: left child of node at %d is invalid(%v)\n", nodeOffset, node.Left)

		return fmt.Errorf("corrupted tree: left child of node at %d is invalid(%v)", nodeOffset, node.Left)
	}
	if node.Right < 0 {
		fmt.Printf("corrupted tree: right child of node at %d is invalid(%v)\n", nodeOffset, node.Right)

		return fmt.Errorf("corrupted tree: right child of node at %d is invalid(%v)", nodeOffset, node.Right)
	}

	if node.Left >= p.header.Size-int64(unsafe.Sizeof(MegaNode{})) {
		fmt.Printf("corrupted tree: left child of node at %d is invalid(%v greater than file size %v)\n", nodeOffset, node.Left, p.header.Size)

		return fmt.Errorf("corrupted tree: left child of node at %d is invalid(%v greater than file size %v)", nodeOffset, node.Left, p.header.Size)
	}
	if node.Right >= p.header.Size-int64(unsafe.Sizeof(MegaNode{})) {
		fmt.Printf("corrupted tree: right child of node at %d is invalid(%v greater than file size %v)\n", nodeOffset, node.Right, p.header.Size)

		return fmt.Errorf("corrupted tree: right child of node at %d is invalid(%v greater than file size %v)", nodeOffset, node.Right, p.header.Size)
	}
	if node.KeyOffset+node.KeyLen >= p.header.Size {
		fmt.Printf("corrupted tree: key of node at %d is invalid(%v greater than file size %v)\n", nodeOffset, node.KeyOffset+node.KeyLen, p.header.Size)

		return fmt.Errorf("corrupted tree: key of node at %d is invalid(%v greater than file size %v)", nodeOffset, node.KeyOffset+node.KeyLen, p.header.Size)
	}
	if node.DataOffset+node.DataLen >= p.header.Size {
		fmt.Printf("corrupted tree: data of node at %d is invalid(%v greater than file size %v)\n", nodeOffset, node.DataOffset+node.DataLen, p.header.Size)

		return fmt.Errorf("corrupted tree: data of node at %d is invalid(%v greater than file size %v)", nodeOffset, node.DataOffset+node.DataLen, p.header.Size)
	}

	if node.KeyOffset < 0 && node.KeyLen > 0 {
		fmt.Printf("corrupted tree: key of node at %d is invalid(%v less than 0)\n", nodeOffset, node.KeyOffset)

		return fmt.Errorf("corrupted tree: key of node at %d is invalid(%v less than 0)", nodeOffset, node.KeyOffset)
	}
	if node.DataOffset < 0 && node.DataLen > 0 {
		fmt.Printf("corrupted tree: data of node at %d is invalid(%v less than 0)\n", nodeOffset, node.DataOffset)

		return fmt.Errorf("corrupted tree: data of node at %d is invalid(%v less than 0)", nodeOffset, node.DataOffset)
	}

	if node.Bumper != ^node.BumperL {
		fmt.Printf("corrupted tree: bumper mismatch at %d\n", nodeOffset)
		return fmt.Errorf("corrupted tree: bumper mismatch at %d", nodeOffset)
	}
	return nil
}

// insert recursive function
// insert recursive function with COW
func (p *MegaPool) insert(nodeOffset int64, key []byte, keyOff, valOff, keyLen, valLen int64) (int64, error) {
	if nodeOffset < 1 {
		// New Node
		nodeSize := int64(unsafe.Sizeof(MegaNode{}))
		newOffset, err := p.alloc(nodeSize)
		if err != nil {
			panicWithStack(err)
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

	if err := p.enforceBounds(nodeOffset); err != nil {
		fmt.Printf("Pruning corrupted node in insert (enforceBounds): %v\n", err)
		return 0, nil
	}

	// Copy node (COW)
	myOffset, err := p.copyNode(nodeOffset)
	if err != nil {
		fmt.Printf("Pruning corrupted node in insert: %v\n", err)
		return 0, nil
	}

	node := p.nodeAt(myOffset)
	if node == nil {
		fmt.Printf("Pruning corrupted node in insert (nil node): %d\n", myOffset)
		return 0, nil
	}

	nodeKey := p.readBytes(node.KeyOffset, node.KeyLen)
	cmp := bytes.Compare(key, nodeKey)

	if cmp < 0 {
		left, err := p.insert(node.Left, key, keyOff, valOff, keyLen, valLen)
		if err != nil {
			panicWithStack(err)
		}
		// Refetch myOffset
		node := p.nodeAt(myOffset)
		if node != nil {
			node.Left = left
			// Balance
			if left != 0 {
				leftNode := p.nodeAt(left)
				if leftNode != nil && leftNode.Bumper > node.Bumper {
					myOffset = p.rotateRight(myOffset)
				}
			}
		}
	} else if cmp > 0 {
		right, err := p.insert(node.Right, key, keyOff, valOff, keyLen, valLen)
		if err != nil {
			panicWithStack(err)
		}
		// Refetch myOffset
		node := p.nodeAt(myOffset)
		if node != nil {
			node.Right = right
			// Balance
			if right != 0 {
				rightNode := p.nodeAt(right)
				if rightNode != nil && rightNode.Bumper > node.Bumper {
					myOffset = p.rotateLeft(myOffset)
				}
			}
		}
	} else {
		// Update existing node (already copied)
		n := p.nodeAt(myOffset)
		n.DataOffset = valOff
		n.DataLen = valLen
		n.KeyOffset = keyOff
		n.KeyLen = keyLen
	}

	return myOffset, nil
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
	p.rlock("Get")
	defer p.runlock("Get")
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
	if depth > MaxDepth {
		panicWithStack("MegaPool cycle detected or tree too deep")
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
	p.rlock("Exists")
	defer p.runlock("Exists")
	return p.search(p.header.BtreeRoot, key, 0) != 0
}

// Size returns the total size of the pool file.
func (p *MegaPool) Size() int64 {
	p.rlock("Size")
	defer p.runlock("Size")
	return p.header.Size
}

// Flush syncs the mmapped data to disk.
func (p *MegaPool) Flush() error {
	p.lock("Flush")
	defer p.unlock("Flush")
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
	if err := unix.Msync(p.data, unix.MS_SYNC); err != nil {
		panicWithStack(err)
	}
	return nil
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
			panicWithStack(err)
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
				panicWithStack(err)
			}
			processed[string(key)] = true
		}
	}
	return processed, nil
}

// Keys returns all keys in the store.
func (p *MegaPool) Keys() [][]byte {
	p.rlock("Keys")
	defer p.runlock("Keys")
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
	p.rlock("KeyHistory")
	defer p.runlock("KeyHistory")
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
	p.rlock("DumpIndex")
	defer p.runlock("DumpIndex")
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
	if err := p.enforceBounds(nodeOffset); err != nil {
		fmt.Printf("Pruning corrupted node in iterate: %v\n", err)
		return nil
	}
	node := p.nodeAt(nodeOffset)
	if node == nil {
		fmt.Printf("Pruning corrupted node in iterate (nil node): %d\n", nodeOffset)
		return nil
	}

	// Left
	if err := p.iterate(node.Left, f); err != nil {
		panicWithStack(err)
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
		panicWithStack(err)
	}

	// Right
	if err := p.iterate(node.Right, f); err != nil {
		panicWithStack(err)
	}

	return nil
}

// Delete removes a key and its value from the store.
// Delete removes a key and its value from the store.
func (p *MegaPool) Delete(key []byte) error {
	fmt.Printf("MegaPool(%p).Delete: locking\n", p)
	p.mu.Lock()
	defer func() { fmt.Printf("MegaPool(%p).Delete: unlocking\n", p); p.mu.Unlock() }()

	startSync := p.header.StartFree
	root, err := p.deleteNode(p.header.BtreeRoot, key)
	if err != nil {
		panicWithStack(err)
	}

	// Flush new data
	endSync := p.header.StartFree
	if endSync > startSync {
		if err := p.flushRange(startSync, endSync-startSync); err != nil {
			panicWithStack(err)
		}
	}

	p.header.BtreeRoot = root
	// Flush header
	if err := p.flushRange(0, MegaHeaderSize); err != nil {
		panicWithStack(err)
	}
	return nil
}

func (p *MegaPool) deleteNode(nodeOffset int64, key []byte) (int64, error) {
	if nodeOffset == 0 {
		return 0, nil // Not found, nothing to delete
	}

	// Copy node (COW)
	myOffset, err := p.copyNode(nodeOffset)
	if err != nil {
		fmt.Printf("Pruning corrupted node in deleteNode: %v\n", err)
		return 0, nil
	}
	node := p.nodeAt(myOffset) // Safe

	nodeKey := p.readBytes(node.KeyOffset, node.KeyLen)
	cmp := bytes.Compare(key, nodeKey)

	if cmp < 0 {
		left, err := p.deleteNode(node.Left, key)
		if err != nil {
			panicWithStack(err)
		}
		p.nodeAt(myOffset).Left = left
	} else if cmp > 0 {
		right, err := p.deleteNode(node.Right, key)
		if err != nil {
			panicWithStack(err)
		}
		p.nodeAt(myOffset).Right = right
	} else {
		// Found the node to delete
		if node.Left == 0 {
			return node.Right, nil
		} else if node.Right == 0 {
			return node.Left, nil
		}

		// Node with two children: Get the inorder successor (smallest in the right subtree)
		minRight := p.minNode(node.Right)
		minRightKey := p.readBytes(minRight.KeyOffset, minRight.KeyLen)
		//minRightVal := p.readBytes(minRight.DataOffset, minRight.DataLen)

		// Copy data to current node
		// Actually, we should probably just update offsets if we wanted to be pure COW,
		// but since we already copied the node, we can modify it.
		// Wait, minRight pointers might be invalid if we just copy struct?
		// No, we just need key and value.
		// But in this implementation, key/value are just offsets.
		// So we can copy offsets.
		n := p.nodeAt(myOffset)
		n.KeyOffset = minRight.KeyOffset
		n.KeyLen = minRight.KeyLen
		n.DataOffset = minRight.DataOffset
		n.DataLen = minRight.DataLen

		// Delete the inorder successor
		right, err := p.deleteNode(node.Right, minRightKey)
		if err != nil {
			panicWithStack(err)
		}
		p.nodeAt(myOffset).Right = right
	}
	return myOffset, nil
}

func (p *MegaPool) minNode(nodeOffset int64) *MegaNode {
	current := p.nodeAt(nodeOffset)
	for current.Left != 0 {
		current = p.nodeAt(current.Left)
	}
	return current
}
