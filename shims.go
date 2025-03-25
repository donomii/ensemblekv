package ensemblekv

import (
	"errors"
	"fmt"
	"log"
	"os"

	nudb "github.com/iand/gonudb"
	bolt "go.etcd.io/bbolt"
	"path/filepath"
	"github.com/recoilme/pudge"
)

// ExtentKeyValueStore is a key-value store that uses a single storage file and an index file to store data.  It can ingest a large amount of data at close to the maximum transfer speed of the drive, and still have reasonable performance.  Deletes do not recover disk space, compaction is required.
func ExtentCreator(directory string, blockSize int) (KvLike, error) {
	return NewExtentKeyValueStore(directory, blockSize)
}


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

func LineLSMCreator(directory string, blockSize int, creator func(directory string, blockSize int) (KvLike, error)) (KvLike, error) {
	return NewLinelsm(directory, blockSize, 1000, creator)
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
	case "pudge":
		h, err := NewPudgeShim(location, blockSize)
		if err != nil {
			panic(err)
		}
		return h
	
	case "ensemble":
		var creator func(directory string, blockSize int) (KvLike, error)
		switch subtipe {
		case "pudge":
			creator = PudgeCreator
		case "nudb":
			creator = NuDbCreator
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

// For NuDbShim:
func (s *nuDbShim) Size() int64 {
    var count int64
    // NuDB doesn't provide a direct way to count entries
    // We'll need to iterate through the store
    _, err := s.MapFunc(func(k, v []byte) error {
        count++
        return nil
    })
    if err != nil {
        return 0
    }
    return count
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



type BoltDbShim struct {
	DefaultOps
	directory  string
	boltHandle *bolt.DB
}

func NewBoltDbShim(directory string, blockSize int) (*BoltDbShim, error) {
	s := BoltDbShim{}
	s.directory = directory
	
	// Ensure directory exists
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Create bolt db file within the directory
	dbPath := filepath.Join(directory, "bolt.db")
	boltHandle, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open bolt database: %w", err)
	}

	// Initialize the default bucket
	err = boltHandle.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("Blocks"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	s.boltHandle = boltHandle
	return &s, nil
}

func (s *BoltDbShim) KeyHistory(key []byte) ([][]byte, error) {
	// BoltDB doesn't store history, only the latest value
	value, err := s.Get(key)
	if err != nil {
		return nil, err
	}
	
	return [][]byte{value}, nil
}


func (s *BoltDbShim) Get(key []byte) ([]byte, error) {
	var v []byte = nil
	s.boltHandle.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Blocks"))
		v = b.Get(key)
		return nil
	})
	var err error
	if v != nil && len(v) == 0 {
		// Key exists but value is empty
	} else if v == nil {
		err = fmt.Errorf("key not found")
	}
	return v, err
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

func (s *BoltDbShim) Size() int64 {
    var count int64
    s.boltHandle.View(func(tx *bolt.Tx) error {
        b := tx.Bucket([]byte("Blocks"))
        c := b.Cursor()
        for k, _ := c.First(); k != nil; k, _ = c.Next() {
            count++
        }
        return nil
    })
    return count
}



type PudgeShim struct {
	DefaultOps
	filename string
	db       *pudge.Db
}

func NewPudgeShim(directory string, blockSize int) (*PudgeShim, error) {
	s := &PudgeShim{
		filename: filepath.Join(directory, "pudge.db"),
	}

	// Configure Pudge
	cfg := pudge.DefaultConfig
	cfg.SyncInterval = 1000 // Sync every 1000ms for balance of performance/safety
	
	// Open the database
	db, err := pudge.Open(s.filename, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to open pudge database: %w", err)
	}
	s.db = db

	return s, nil
}

func (s *PudgeShim) KeyHistory(key []byte) ([][]byte, error) {
	// Pudge doesn't store history, only the latest value
	value, err := s.Get(key)
	if err != nil {
		return nil, err
	}
	
	return [][]byte{value}, nil
}

// Get implements KvLike interface
func (s *PudgeShim) Get(key []byte) ([]byte, error) {
	var value []byte
	err := s.db.Get(key, &value)
	if err != nil {
		if err == pudge.ErrKeyNotFound {
			return nil, fmt.Errorf("key not found")
		}
		return nil, err
	}
	return value, nil
}

// Put implements KvLike interface
func (s *PudgeShim) Put(key []byte, val []byte) error {
	return s.db.Set(key, val)
}

// Exists implements KvLike interface
func (s *PudgeShim) Exists(key []byte) bool {
	exists, err := s.db.Has(key)
	if err != nil {
		return false
	}
	return exists
}

// Delete implements KvLike interface
func (s *PudgeShim) Delete(key []byte) error {
	return s.db.Delete(key)
}

// Size implements KvLike interface
func (s *PudgeShim) Size() int64 {
	count, err := s.db.Count()
	if err != nil {
		return 0
	}
	return int64(count)
}

// Flush implements KvLike interface
func (s *PudgeShim) Flush() error {
	// Pudge doesn't have a direct flush method, but we can force a sync
	// by closing and reopening the database
	if err := s.db.Close(); err != nil {
		return err
	}

	cfg := pudge.DefaultConfig
	cfg.SyncInterval = 1000
	
	db, err := pudge.Open(s.filename, cfg)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

// Close implements KvLike interface
func (s *PudgeShim) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// MapFunc implements KvLike interface
func (s *PudgeShim) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	// Get all keys
	keys, err := s.db.Keys(nil, 0, 0, true) // Get all keys in ascending order
	if err != nil {
		return nil, err
	}

	visited := make(map[string]bool)
	
	// Iterate through keys
	for _, key := range keys {
		var value []byte
		err := s.db.Get(key, &value)
		if err != nil {
			continue // Skip errored entries
		}
		
		visited[string(key)] = true
		if err := f(key, value); err != nil {
			return visited, err
		}
	}

	return visited, nil
}

// PudgeCreator is the creator function for PudgeShim
func PudgeCreator(directory string, blockSize int) (KvLike, error) {
	return NewPudgeShim(directory, blockSize)
}