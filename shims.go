package ensemblekv

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	nudb "github.com/iand/gonudb"
	barrel "github.com/mr-karan/barreldb"
	bolt "go.etcd.io/bbolt"
)

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

func (s *barrelShim) Size() int64 {
    // BarrelDB provides a List() method that returns all keys
    keys := s.dbHandle.List()
    return int64(len(keys))
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
