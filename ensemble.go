package ensemblekv

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	_ "log"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"time"

	nudb "github.com/iand/gonudb"
	barrel "github.com/mr-karan/barreldb"
	bolt "go.etcd.io/bbolt"
)

// This is ensemblekv, a key-value store that uses multiple sub-stores to store data. It is designed to be used with large data sets that do not fit into other kv stores, while still being able to use the features of those kv stores.

type KvLike interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, val []byte) error
	Exists(key []byte) bool
	Delete(key []byte) error
	Flush() error
	Close() error
	MapFunc(f func([]byte, []byte) error) (map[string]bool, error)
	DumpIndex() error
}

type DefaultOps struct {
}

func (d *DefaultOps) DumpIndex() error {
	return nil
}

// ExtentKeyValueStore is a key-value store that uses a single storage file and an index file to store data.  It can ingest a large amount of data at close to the maximum transfer speed of the drive, and still have reasonable performance.  Deletes do not recover disk space, compaction is required.
func ExtentCreator(directory string, blockSize int) (KvLike, error) {
	return NewExtentKeyValueStore(directory, blockSize)
}

func BarrelDbCreator(directory string, blockSize int) (KvLike, error) {
	return NewBarrelShim(directory, blockSize)
}

/*
func LotusCreator(directory string, blockSize int) (KvLike, error) {
	return NewLotusShim(directory, blockSize)
}
*/

func NuDbCreator(directory string, blockSize int) (KvLike, error) {
	return NewNuDbShim(directory, blockSize)
}

func BoltDbCreator(directory string, blockSize int) (KvLike, error) {
	return NewBoltDbShim(directory, blockSize)
}

func EnsembleCreator(directory string, blockSize int, creator func(directory string, blockSize int) (KvLike, error)) (KvLike, error) {
	N := 3          //Number of sub-stores
	maxKeys := 1000 //Max number of keys in each sub-store, before it is split

	return NewEnsembleKv(directory, N, blockSize, maxKeys, creator)
}

func SimpleEnsembleCreator(tipe, subtipe, location string, blockSize int, substores int) KvLike {
	// Initialise.

	switch tipe {
	case "nudb":
		h, err := NewNuDbShim(location, blockSize)
		if err != nil {
			panic(err)
		}
		return h
	case "barrel":
		h, err := NewBarrelShim(location, blockSize)
		if err != nil {
			panic(err)
		}
		return h
		/*
			case "lotus":
				h, err := NewLotusShim(location, blockSize)
				if err != nil {
					panic(err)
				}
				return h
		*/
	case "extent":
		h, err := NewExtentKeyValueStore(location, blockSize)
		if err != nil {
			panic(err)
		}

		return h
	case "bolt":
		h, err := NewBoltDbShim(location, blockSize)
		if err != nil {
			panic(err)
		}
		return h
	case "ensemble":
		var creator func(directory string, blockSize int) (KvLike, error)
		switch subtipe {
		case "nudb":
			creator = NuDbCreator
		case "barrel":
			creator = BarrelDbCreator
			/*
				case "lotus":
					creator = LotusCreator
			*/
		case "extent":
			creator = ExtentCreator
		case "bolt":
			creator = BoltDbCreator
		default:
			panic("Unknown substore type: " + subtipe)
		}

		h, err := NewEnsembleKv(location, 60, 10*1024*1024, 100000, func(path string, blockSize int) (KvLike, error) {
			kvstore, err := creator(path, blockSize)
			if err != nil {
				panic(err)
			}
			return kvstore, nil
		})
		if err != nil {
			panic(err)
		}
		return h
	default:
		panic("Unknown store type: " + tipe)
	}
}

type nuDbShim struct {
	DefaultOps
	filename string
	dataPath string
	keyPath  string
	logPath  string
	dbHandle *nudb.Store
}

func NewNuDbShim(filename string, blockSize int) (*nuDbShim, error) {
	s := nuDbShim{}
	s.filename = filename
	s.dataPath = filename + "/db.dat"
	s.keyPath = filename + "/db.key"
	s.logPath = filename + "/db.log"

	err := os.MkdirAll(s.filename, os.ModePerm)
	if err != nil {
		log.Fatalf("error creating data dir: %v", err)
	} // Creating data dir.
	err = nudb.CreateStore(
		s.dataPath,
		s.keyPath,
		s.logPath,
		1,
		nudb.NewSalt(),
		4096,
		0.5,
	)
	if err != nil {
		var pathErr *os.PathError
		if errors.As(err, &pathErr) && os.IsExist(pathErr) {
			fmt.Println("Store already exists")
		} else {
			log.Panic("Failed to create store: "+err.Error(), 1)
		}
	}

	dbHandle, err := nudb.OpenStore(s.dataPath, s.keyPath, s.logPath, nil)
	if err != nil {
		return nil, err
	}
	s.dbHandle = dbHandle

	return &s, nil
}

func (s *nuDbShim) Get(key []byte) ([]byte, error) {
	return s.dbHandle.Fetch(string(key))
}

func (s *nuDbShim) Put(key []byte, val []byte) error {
	err := s.dbHandle.Insert(string(key), val)
	if err != nil {
		if errors.Is(err, nudb.ErrKeyExists) {

		} else {
			return err
		}
	}
	return nil
}

func (s *nuDbShim) Exists(key []byte) bool {
	e, err := s.dbHandle.Exists(string(key))
	if err != nil {
		panic(err)
	}
	return e
}

func (s *nuDbShim) Delete(key []byte) error {
	fmt.Println("Cannot delete any keys from nudb")
	return errors.New("Cannot delete any keys from nudb")
}

func (s *nuDbShim) Flush() error {
	return s.dbHandle.Flush()
}

func (s *nuDbShim) Close() error {
	return s.dbHandle.Close()
}

func (s *nuDbShim) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	// this library doesn't work anyway so why bother?
	panic("lol")
}

type barrelShim struct {
	DefaultOps
	filename string
	dataPath string
	keyPath  string
	logPath  string
	dbHandle *barrel.Barrel
}

func NewBarrelShim(filename string, blockSize int) (*barrelShim, error) {

	s := barrelShim{}
	s.filename = filename
	err := os.MkdirAll(filename, os.ModePerm)
	if err != nil {
		log.Fatalf("error creating data dir: %v", err)
	} // Creating data dir.

	b, err := barrel.Init(barrel.WithDir(s.filename), barrel.WithBackgrondSync(30*time.Second), barrel.WithCheckFileSizeInterval(30*time.Second), barrel.WithCompactInterval(5*time.Minute))
	if err != nil {
		log.Fatalf("error initialising barrel: %v", err)
	}

	s.dbHandle = b

	return &s, nil
}

func (s *barrelShim) Get(key []byte) ([]byte, error) {
	return s.dbHandle.Get(string(key))
}

func (s *barrelShim) Put(key []byte, val []byte) error {
	return s.dbHandle.Put(string(key), val)
}

func (s *barrelShim) Exists(key []byte) bool {
	_, err := s.dbHandle.Get(string(key))
	if err != nil {
		return false
	}
	return true
}

func (s *barrelShim) Delete(key []byte) error {
	return s.dbHandle.Delete(string(key))
}

func (s *barrelShim) Flush() error {
	return s.dbHandle.Sync()
}

func (s *barrelShim) Close() error {
	return s.dbHandle.Shutdown()
}

func (s *barrelShim) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	keys := s.dbHandle.List()
	for _, key := range keys {
		value, err := s.dbHandle.Get(key)
		if err != nil {
			return nil, err
		}
		err = f([]byte(key), value)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

/*
type lotusShim struct {
	filename     string
	lotusHandle  *lotus.DB
	lotusOptions *lotus.Options
}

func NewLotusShim(filename string, blockSize int) (*lotusShim, error) {
	s := lotusShim{}
	s.filename = filename
	var err error
	options := lotus.DefaultOptions
	options.DirPath = s.filename
	options.ValueLogFileSize = 5 * 1000000000
	options.PartitionNum = 15
	s.lotusOptions = &options

	s.lotusHandle, err = lotus.Open(options)
	if err != nil {
		panic(err)
	}

	err = s.lotusHandle.Compact()
	if err != nil {
		panic(err)
	}

	return &s, nil
}

func (s *lotusShim) Get(key []byte) ([]byte, error) {
	return s.lotusHandle.Get(key)
}

func (s *lotusShim) Put(key []byte, val []byte) error {
	return s.lotusHandle.Put(key, val, nil)
}

func (s *lotusShim) Exists(key []byte) bool {
	_, err := s.lotusHandle.Get(key)
	if err != nil {
		return false
	}
	return true
}

func (s *lotusShim) Delete(key []byte) error {
	return s.lotusHandle.Delete(key, nil)
}

func (s *lotusShim) Flush() error {
	return s.lotusHandle.Sync()
}

func (s *lotusShim) Close() error {
	return s.lotusHandle.Close()
}

func (s *lotusShim) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	panic("lol")
}
*/

// HashFunc is the type of function used to hash keys for consistent hashing.
type HashFunc func(data []byte) uint64

// EnsembleKv represents the ensemble key-value store.
type EnsembleKv struct {
	DefaultOps
	directory   string
	substores   []KvLike
	hashFunc    HashFunc
	N           int
	maxKeys     int
	maxBlock    int
	totalKeys   int // Total number of keys across all substores
	mutex       sync.Mutex
	generation  int
	createStore func(path string, blockSize int) (KvLike, error)
}

func NewEnsembleKv(directory string, N int, maxBlock int, maxKeys int, createStore func(path string, blockSize int) (KvLike, error)) (*EnsembleKv, error) {
	store := &EnsembleKv{
		directory:   directory,
		N:           N,
		maxBlock:    maxBlock,
		maxKeys:     maxKeys,
		createStore: createStore,
		hashFunc:    defaultHashFunc,
	}

	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, err
	}

	if err := store.loadMetadata(); err != nil {
		fmt.Printf("Ensemble: Error loading shard metadata: %v\n", err)
		return nil, err
	}

	if err := store.setup(); err != nil {
		return nil, err
	}

	store.saveMetadata()

	return store, nil
}

// setup initializes the store by creating the directory structure and substores.
func (s *EnsembleKv) setup() error {
	genPath := filepath.Join(s.directory, fmt.Sprintf("%d", s.generation))
	if err := os.MkdirAll(genPath, os.ModePerm); err != nil {
		return err
	}

	for i := 0; i < s.N; i++ {
		subPath := filepath.Join(genPath, fmt.Sprintf("%d", i))
		substore, err := s.createStore(subPath, s.maxBlock)
		if err != nil {
			return err
		}
		s.substores = append(s.substores, substore)
	}

	return nil
}

func defaultHashFunc(data []byte) uint64 {
	bi := big.NewInt(0)
	h := md5.New()
	h.Write(data)
	hexstr := hex.EncodeToString(h.Sum(nil))
	bi.SetString(hexstr, 16)

	value := bi.Int64()
	if value < 0 {
		value = value * -1
	}
	return uint64(value)
}

// hashToIndex maps a hash value to an index of a substore.
func (s *EnsembleKv) hashToIndex(hash uint64) int {
	return int(hash) % len(s.substores)
}

// Get retrieves a value for a given key from the appropriate substore.
func (s *EnsembleKv) Get(key []byte) ([]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	hash := s.hashFunc(key)
	index := s.hashToIndex(hash)
	//fmt.Printf("Getting key %v from substore %d\n", clampString(string(key), 100), index) //FIXME make debug log
	return s.substores[index].Get(key)
}

func clampString(in string, length int) string {
	if len(in) > length {
		return in[:length]
	}
	return in
}

// Put stores a key-value pair in the appropriate substore.

// Adjusted Put method to track the total number of keys and trigger rebalance if needed.
func (s *EnsembleKv) Put(key []byte, val []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	hash := s.hashFunc(key)
	index := s.hashToIndex(hash)
	//fmt.Printf("Putting key %v in substore %d.  %v keys in total\n", clampString(string(key), 100), index, s.totalKeys) //FIXME make debug log
	if err := s.substores[index].Put(key, val); err != nil {
		return err
	}

	// Increment total key count
	s.totalKeys = s.totalKeys + 1

	// Check if rebalance is needed
	if s.totalKeys/s.N >= s.maxKeys {
		fmt.Printf("Substores hav grown past key limit of %d\n", s.maxKeys) //FIXME make debug log
		if err := s.rebalance(); err != nil {
			return err
		}
	}

	return nil
}

// Delete removes a key-value pair from the appropriate substore.
func (s *EnsembleKv) Delete(key []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	hash := s.hashFunc(key)
	index := s.hashToIndex(hash)

	// Decriment total key count
	s.totalKeys = s.totalKeys - 1
	return s.substores[index].Delete(key)
}

// Exists checks if a key exists in the appropriate substore.
func (s *EnsembleKv) Exists(key []byte) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	hash := s.hashFunc(key)
	index := s.hashToIndex(hash)
	return s.substores[index].Exists(key)
}

// Flush calls Flush on all substores.
func (s *EnsembleKv) Flush() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, substore := range s.substores {
		if err := substore.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// Close closes all substores.
func (s *EnsembleKv) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, substore := range s.substores {
		if err := substore.Close(); err != nil {
			return err
		}
	}
	return nil
}

// rebalance redistributes keys across a new generation of substores when the current generation reaches capacity.
func (s *EnsembleKv) rebalance() error {
	fmt.Printf("Rebalancing store with %d keys\n", s.totalKeys)
	go fmt.Printf("Rebalancing store complete with %v keys\n", s.totalKeys)

	// Increment generation and calculate new N
	s.generation++
	s.N *= 2

	// Create new generation directory
	newGenPath := filepath.Join(s.directory, fmt.Sprintf("%d", s.generation))
	if err := os.MkdirAll(newGenPath, os.ModePerm); err != nil {
		return err
	}

	fmt.Printf("Creating new generation %d with %d substores\n", s.generation, s.N)
	// Create new substores
	newSubstores := make([]KvLike, s.N)
	for i := 0; i < s.N; i++ {
		var err error
		subPath := filepath.Join(newGenPath, fmt.Sprintf("%d", i))
		newSubstores[i], err = s.createStore(subPath, s.maxBlock)
		if err != nil {
			return err
		}

	}

	fmt.Printf("Rebalancing %d keys\n", s.totalKeys)
	// Re-distribute keys
	for _, substore := range s.substores {
		_, err := substore.MapFunc(func(key, value []byte) error {
			hash := s.hashFunc(key)
			newIndex := int(hash) % s.N
			fmt.Printf("Rebalancing key %v to substore %d\n", string(key), newIndex)
			return newSubstores[newIndex].Put(key, value)
		})
		if err != nil {
			return err
		}
	}

	fmt.Printf("Rebalancing complete\n")

	fmt.Printf("Closing old substores\n")
	// Close and remove old substores
	oldGenPath := filepath.Join(s.directory, fmt.Sprintf("%d", s.generation-1))
	for _, substore := range s.substores {
		fmt.Printf("Closing substore\n")
		if err := substore.Close(); err != nil {
			return fmt.Errorf("Error closing substore: %v\n", err)
		}
	}

	fmt.Printf("Removing old generation %d\n", s.generation-1)
	fmt.Printf("Removing old generation %v\n", oldGenPath)
	if err := os.RemoveAll(oldGenPath); err != nil {
		fmt.Printf("Error removing old generation: %v\n", err)
		return err
	}

	// Update substores to new generation
	s.substores = newSubstores

	// Save metadata after rebalancing
	return s.saveMetadata()
}

// Metadata represents the persistent state of the Store.
type Metadata struct {
	Generation int `json:"generation"`
	N          int `json:"n"`
	TotalKeys  int `json:"totalKeys"`
}

// saveMetadata writes the current store metadata to a file.
func (s *EnsembleKv) saveMetadata() error {
	metadata := Metadata{
		Generation: s.generation,
		N:          s.N,
		TotalKeys:  s.totalKeys,
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	metadataPath := filepath.Join(s.directory, "metadata.json")
	return ioutil.WriteFile(metadataPath, data, 0644)
}

// loadMetadata reads the store metadata from a file.
func (s *EnsembleKv) loadMetadata() error {
	metadataPath := filepath.Join(s.directory, "metadata.json")
	data, err := ioutil.ReadFile(metadataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No metadata exists, likely new store
		}
		return err
	}
	var metadata Metadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return err
	}
	s.generation = metadata.Generation
	s.N = metadata.N
	s.totalKeys = metadata.TotalKeys
	return nil
}

// Implementation of the MapFunc method for the Store. This method applies a function to each key-value pair in all substores.

func (s *EnsembleKv) MapFunc(f func(key []byte, value []byte) error) (map[string]bool, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return lockFreeMapFunc(s.substores, f)
}

// Implementation of the MapFunc method for the Store. This method applies a function to each key-value pair in all substores.

func lockFreeMapFunc(substores []KvLike, f func(key []byte, value []byte) error) (map[string]bool, error) {

	// Iterate over each substore
	for _, substore := range substores {
		// Use the MapFunc of each substore, passing the function 'f'
		_, err := substore.MapFunc(func(key []byte, value []byte) error {
			// Call the function 'f' for each key-value pair
			return f(key, value)
		})
		if err != nil {
			return nil, err // Return error if any occurs
		}
	}

	return nil, nil // Return nil if the operation completes successfully on all substores
}

type BoltDbShim struct {
	DefaultOps
	filename   string
	boltHandle *bolt.DB
}

func NewBoltDbShim(filename string, blockSize int) (*BoltDbShim, error) {
	s := BoltDbShim{}
	s.filename = filename

	boltHandle, err := bolt.Open(s.filename, 0600, nil)
	if err != nil {
		return nil, err
	}
	boltHandle.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("Blocks"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	s.boltHandle = boltHandle

	return &s, nil
}

func (s *BoltDbShim) Get(key []byte) ([]byte, error) {
	var v []byte = nil
	s.boltHandle.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Blocks"))
		v = b.Get(key)
		return nil
	})
	return v, nil
}

func (s *BoltDbShim) Put(key []byte, val []byte) error {
	if len(val) > 1073741822 {
		return fmt.Errorf("Block size too large: %v", len(val))
	}
	if len(key) > 32768 {
		return fmt.Errorf("Key size too large: %v", len(key))
	}

	s.boltHandle.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Blocks"))
		err := b.Put(key, val)
		return err
	})
	return nil
}

func (s *BoltDbShim) Exists(key []byte) bool {
	v, err := s.Get(key)
	if err != nil {
		return false
	}
	if v == nil {
		return false
	}
	return true
}

func (s *BoltDbShim) Delete(key []byte) error {
	s.boltHandle.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Blocks"))
		err := b.Delete(key)
		return err
	})
	return nil
}

func (s *BoltDbShim) Flush() error {
	return s.boltHandle.Sync()
}

func (s *BoltDbShim) Close() error {
	return s.boltHandle.Close()
}

func (s *BoltDbShim) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	keys := make(map[string]bool)
	s.boltHandle.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Blocks"))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			keys[string(k)] = true
			err := f(k, v)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return keys, nil
}

func (s *BoltDbShim) List() ([]string, error) {
	keys := []string{}
	s.boltHandle.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Blocks"))
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			keys = append(keys, string(k))
		}
		return nil
	})
	return keys, nil
}
