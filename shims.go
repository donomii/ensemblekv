package ensemblekv

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"path/filepath"

	nudb "github.com/iand/gonudb"
	"github.com/recoilme/pudge"
	bolt "go.etcd.io/bbolt"
)

type CreatorFunc func(directory string, blockSize, fileSize int64) (KvLike, error)

// ExtentKeyValueStore is a key-value store that uses a single storage file and an index file to store data.  It can ingest a large amount of data at close to the maximum transfer speed of the drive, and still have reasonable performance.  Deletes do not recover disk space, compaction is required.
func ExtentCreator(directory string, blockSize, fileSize int64) (KvLike, error) {
	return NewExtentKeyValueStore(directory, blockSize, fileSize)
}

func SingleFileKVCreator(directory string, blockSize, fileSize int64) (KvLike, error) {
	// Ensure directory exists
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}
	return NewSingleFileLSM(directory+"/singlefilekv", blockSize, int64(fileSize))
}

func MmapSingleCreator(directory string, blockSize, fileSize int64) (KvLike, error) {
	return NewMmapSingleKV(directory, blockSize, fileSize)
}

func SQLiteCreator(directory string, blockSize, fileSize int64) (KvLike, error) {
	return NewSQLiteKVStore(directory, blockSize, fileSize)
}

func NuDbCreator(directory string, blockSize, fileSize int64) (KvLike, error) {
	return NewNuDbShim(directory, blockSize, fileSize)
}

func BoltDbCreator(directory string, blockSize, fileSize int64) (KvLike, error) {
	return NewBoltDbShim(directory, blockSize, fileSize)
}

func MegapoolCreator(directory string, blockSize, fileSize int64) (KvLike, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}
	return OpenMegaPool(filepath.Join(directory, "megapool.db"), fileSize)
}

func EnsembleCreator(directory string, blockSize, filesize int64, creator CreatorFunc) (KvLike, error) {
	N := int64(3)          //Number of sub-stores
	maxKeys := int64(1000) //Max number of keys in each sub-store, before it is split

	return NewEnsembleKv(directory, N, blockSize, maxKeys, filesize, creator)
}

func LineLSMCreator(directory string, blockSize int64, fileSize int64, creator CreatorFunc) (KvLike, error) {
	return NewLinelsm(directory, blockSize, 1000, fileSize, creator)
}

func SimpleEnsembleCreator(tipe, subtipe, location string, blockSize, substores, filesize int64) KvLike {
	// Initialise.

	switch tipe {
	case "nudb":
		h, err := NewNuDbShim(location, blockSize, filesize)
		if err != nil {
			panic(err)
		}
		return h
	case "extent":
		h, err := NewExtentKeyValueStore(location, blockSize, filesize)
		if err != nil {
			panic(err)
		}

		return h

	case "sqlite":
		h, err := NewSQLiteKVStore(location, blockSize, filesize)
		if err != nil {
			panic(err)
		}
		return h
	case "bolt":
		h, err := NewBoltDbShim(location, blockSize, filesize)
		if err != nil {
			panic(err)
		}
		return h
	case "pudge":
		h, err := NewPudgeShim(location, blockSize, filesize)
		if err != nil {
			panic(err)
		}
		return h
	case "mmapsingle":
		h, err := NewMmapSingleKV(location, blockSize, filesize)
		if err != nil {
			panic(err)
		}
		return h
	case "megapool":
		h, err := OpenMegaPool(filepath.Join(location, "megapool.db"), filesize)
		if err != nil {
			panic(err)
		}
		return h

	case "ensemble":
		var creator CreatorFunc
		switch subtipe {
		case "pudge":
			creator = PudgeCreator
		case "nudb":
			creator = NuDbCreator
		case "extent":
			creator = ExtentCreator

		case "sqlite":
			creator = SQLiteCreator
		case "bolt":
			creator = BoltDbCreator
		case "mmapsingle":
			creator = MmapSingleCreator
		case "megapool":
			creator = MegapoolCreator
		default:
			panic("Unknown substore type: " + subtipe)
		}

		h, err := NewEnsembleKv(location, 60, 10*1024*1024, 100000, filesize, func(path string, blockSize, filesize int64) (KvLike, error) {
			kvstore, err := creator(path, blockSize, filesize)
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

func NewNuDbShim(filename string, blockSize, fileSize int64) (*nuDbShim, error) {
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

func (s *nuDbShim) Keys() [][]byte {

	var keys [][]byte
	panic("not implemented")
	return keys
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
	// hmmm
	panic("lol")
}

func (s *nuDbShim) MapPrefixFunc(prefix []byte, f func([]byte, []byte) error) (map[string]bool, error) {
	// NuDb doesn't support native prefix search, so filter through MapFunc
	keys := make(map[string]bool)
	_, err := s.MapFunc(func(k, v []byte) error {
		if bytes.HasPrefix(k, prefix) {
			keys[string(k)] = true
			return f(k, v)
		}
		return nil
	})
	if err != nil {
		return keys, err
	}
	// Return only the keys that matched the prefix
	return keys, nil
}

type BoltDbShim struct {
	DefaultOps
	directory  string
	boltHandle *bolt.DB
}

func NewBoltDbShim(directory string, blockSize, filesize int64) (*BoltDbShim, error) {
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

func (s *BoltDbShim) Keys() [][]byte {
	var keys [][]byte
	s.boltHandle.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Blocks"))
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			// Copy the key since k is valid only for the life of the transaction
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			keys = append(keys, keyCopy)
		}
		return nil
	})
	return keys
}

func (s *BoltDbShim) Get(key []byte) ([]byte, error) {
	var out []byte = nil
	var exists bool = false
	s.boltHandle.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Blocks"))
		v := b.Get(key)
		if v == nil {
			return nil
		}
		exists = true
		// Copy the value to avoid referencing the underlying data
		out = make([]byte, len(v))
		copy(out, v)
		return nil
	})
	if !exists {
		return nil, fmt.Errorf("key not found")
	}

	if out == nil {
		return nil, fmt.Errorf("key not found")
	}

	return out, nil
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
	seenKeys := make(map[string]bool)
	keys := s.Keys()

	for _, k := range keys {
		var v []byte
		s.boltHandle.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("Blocks"))
			v = b.Get(k)
			return nil
		})
		seenKeys[string(k)] = true
		err := f(k, v)
		if err != nil {
			return seenKeys, err
		}
	}
	return seenKeys, nil
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

func (s *BoltDbShim) MapPrefixFunc(prefix []byte, f func([]byte, []byte) error) (map[string]bool, error) {
	seenKeys := make(map[string]bool)
	keys := s.Keys()

	for _, k := range keys {
		if !bytes.HasPrefix(k, prefix) {
			continue
		}
		var v []byte
		s.boltHandle.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("Blocks"))
			v = b.Get(k)
			return nil
		})
		seenKeys[string(k)] = true
		err := f(k, v)
		if err != nil {
			return seenKeys, err
		}
	}

	return seenKeys, nil
}

type PudgeShim struct {
	DefaultOps
	filename string
	db       *pudge.Db
}

func NewPudgeShim(directory string, blockSize, filesize int64) (*PudgeShim, error) {
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

func (s *PudgeShim) Keys() [][]byte {
	var keys [][]byte
	panic("not implemented")
	return keys
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

func (s *PudgeShim) MapPrefixFunc(prefix []byte, f func([]byte, []byte) error) (map[string]bool, error) {
	// Pudge doesn't support native prefix search, so filter through MapFunc
	keys := make(map[string]bool)
	_, err := s.MapFunc(func(k, v []byte) error {
		if bytes.HasPrefix(k, prefix) {
			keys[string(k)] = true
			return f(k, v)
		}
		return nil
	})
	return keys, err
}

// PudgeCreator is the creator function for PudgeShim
func PudgeCreator(directory string, blockSize, filesize int64) (KvLike, error) {
	return NewPudgeShim(directory, blockSize, filesize)
}

// S3Shim implements KvLike for S3-compatible object storage.
type S3Shim struct {
	DefaultOps
	client     *s3.S3
	bucketName string
	prefix     string
}

// NewS3Shim creates a new S3Shim.
func NewS3Shim(endpoint, accessKey, secretKey, region, bucketName, prefix string) (*S3Shim, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String(region),
		Endpoint:         aws.String(endpoint),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 session: %w", err)
	}

	client := s3.New(sess)
	return &S3Shim{
		client:     client,
		bucketName: bucketName,
		prefix:     prefix,
	}, nil
}

func (s *S3Shim) keyToPath(key []byte) string {
	if s.prefix == "" {
		return string(key)
	}
	return s.prefix + "/" + string(key)
}

func (s *S3Shim) Get(key []byte) ([]byte, error) {
	out, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(s.keyToPath(key)),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(out.Body)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *S3Shim) Keys() [][]byte {
	var keys [][]byte
	panic("not implemented")
	return keys
}

func (s *S3Shim) Put(key []byte, val []byte) error {
	_, err := s.client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(s.keyToPath(key)),
		Body:   bytes.NewReader(val),
	})
	return err
}

func (s *S3Shim) Exists(key []byte) bool {
	_, err := s.client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(s.keyToPath(key)),
	})
	return err == nil
}

func (s *S3Shim) Delete(key []byte) error {
	_, err := s.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(s.keyToPath(key)),
	})
	return err
}

func (s *S3Shim) Flush() error {
	// S3 is immediately consistent for new objects so nothing needed
	return nil
}

func (s *S3Shim) Close() error {
	// S3 client doesn't need explicit closing
	return nil
}

func (s *S3Shim) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	keys := make(map[string]bool)

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucketName),
		Prefix: aws.String(s.prefix),
	}
	err := s.client.ListObjectsV2Pages(input, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, obj := range page.Contents {
			keyStr := *obj.Key
			key := []byte(keyStr)
			if s.prefix != "" && len(keyStr) > len(s.prefix)+1 {
				key = []byte(keyStr[len(s.prefix)+1:])
			}
			val, err := s.Get(key)
			if err != nil {
				continue
			}
			keys[string(key)] = true
			_ = f(key, val)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

func (s *S3Shim) Size() int64 {
	// S3 doesn't provide a direct way to count objects
	// This is a placeholder implementation
	return 0
}

func (s *S3Shim) MapPrefixFunc(prefix []byte, f func([]byte, []byte) error) (map[string]bool, error) {
	keys := make(map[string]bool)

	// Construct the S3 prefix by combining our base prefix with the search prefix
	s3Prefix := s.prefix
	if s.prefix != "" {
		s3Prefix = s.prefix + "/" + string(prefix)
	} else {
		s3Prefix = string(prefix)
	}

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucketName),
		Prefix: aws.String(s3Prefix),
	}

	err := s.client.ListObjectsV2Pages(input, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, obj := range page.Contents {
			keyStr := *obj.Key
			key := []byte(keyStr)

			// Remove our base prefix to get the actual key
			if s.prefix != "" && len(keyStr) > len(s.prefix)+1 {
				key = []byte(keyStr[len(s.prefix)+1:])
			}

			// Check if it still has the desired prefix after removing base prefix
			if bytes.HasPrefix(key, prefix) {
				val, err := s.Get(key)
				if err != nil {
					continue
				}
				keys[string(key)] = true
				_ = f(key, val)
			}
		}
		return true
	})

	if err != nil {
		return nil, err
	}
	return keys, nil
}

// S3Creator is the creator function for S3Shim
func S3Creator(endpoint, accessKey, secretKey, region, bucketName, prefix string) CreatorFunc {
	return func(_ string, _ int64, _ int64) (KvLike, error) {
		return NewS3Shim(endpoint, accessKey, secretKey, region, bucketName, prefix)
	}
}
