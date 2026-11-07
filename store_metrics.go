package ensemblekv

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	bolt "go.etcd.io/bbolt"
)

type storeMetrics struct {
	count int64
	bytes int64
}

func countLiveKeys(store KvLike) (int64, error) {
	var count int64
	err := forEachLiveKey(store, func(key []byte) error {
		if count == math.MaxInt64 {
			return fmt.Errorf("live key count exceeds %d", int64(math.MaxInt64))
		}
		count++
		return nil
	})
	return count, err
}

func liveSizeBytes(store KvLike) (int64, error) {
	var result storeMetrics
	err := forEachLiveKey(store, func(key []byte) error {
		value, err := store.Get(key)
		if err != nil {
			return fmt.Errorf("measure live key %q in %T: %w", key, store, err)
		}
		return result.add(int64(len(key)), int64(len(value)))
	})
	return result.bytes, err
}

func (s *ExtentKeyValStore) Count() (int64, error)     { return countLiveKeys(s) }
func (s *ExtentKeyValStore) SizeBytes() (int64, error) { return liveSizeBytes(s) }
func (s *SingleFileLSM) Count() (int64, error)         { return countLiveKeys(s) }
func (s *SingleFileLSM) SizeBytes() (int64, error)     { return liveSizeBytes(s) }
func (s *MmapSingleKV) Count() (int64, error)          { return countLiveKeys(s) }
func (s *MmapSingleKV) SizeBytes() (int64, error)      { return liveSizeBytes(s) }
func (s *MegaPool) Count() (int64, error)              { return countLiveKeys(s) }
func (s *MegaPool) SizeBytes() (int64, error)          { return liveSizeBytes(s) }
func (s *Linelsm) Count() (int64, error)               { return countLiveKeys(s) }
func (s *Linelsm) SizeBytes() (int64, error)           { return liveSizeBytes(s) }
func (s *TreeLSM) Count() (int64, error)               { return countLiveKeys(s) }
func (s *TreeLSM) SizeBytes() (int64, error)           { return liveSizeBytes(s) }
func (s *StarLSM) Count() (int64, error)               { return countLiveKeys(s) }
func (s *StarLSM) SizeBytes() (int64, error)           { return liveSizeBytes(s) }

func (m *storeMetrics) add(keyBytes, valueBytes int64) error {
	if keyBytes < 0 || valueBytes < 0 || valueBytes > math.MaxInt64-keyBytes ||
		m.bytes > math.MaxInt64-keyBytes-valueBytes || m.count == math.MaxInt64 {
		return fmt.Errorf("store metrics overflow: count=%d bytes=%d key bytes=%d value bytes=%d", m.count, m.bytes, keyBytes, valueBytes)
	}
	m.count++
	m.bytes += keyBytes + valueBytes
	return nil
}

func (s *BoltDbShim) metrics() (storeMetrics, error) {
	var result storeMetrics
	err := s.boltHandle.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("Blocks"))
		if bucket == nil {
			return fmt.Errorf("Bolt store %s has no Blocks bucket", s.directory)
		}
		return bucket.ForEach(func(key, value []byte) error {
			if value == nil {
				return fmt.Errorf("Bolt store %s contains nested bucket %q", s.directory, key)
			}
			return result.add(int64(len(key)), int64(len(value)))
		})
	})
	return result, err
}

func (s *BoltDbShim) Count() (int64, error) {
	result, err := s.metrics()
	return result.count, err
}

func (s *BoltDbShim) SizeBytes() (int64, error) {
	result, err := s.metrics()
	return result.bytes, err
}

func (s *SQLiteKV) Count() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var count int64
	err := s.db.QueryRow(`SELECT COUNT(*) FROM kv_store`).Scan(&count)
	return count, err
}

func (s *SQLiteKV) SizeBytes() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var size int64
	err := s.db.QueryRow(`SELECT COALESCE(SUM(LENGTH(CAST(key AS BLOB)) + LENGTH(value)), 0) FROM kv_store`).Scan(&size)
	return size, err
}

func (s *JsonKV) Count() (int64, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return int64(len(s.data)), nil
}

func (s *JsonKV) SizeBytes() (int64, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	var result storeMetrics
	for key, value := range s.data {
		if err := result.add(int64(len(key)), int64(len(value))); err != nil {
			return 0, err
		}
	}
	return result.bytes, nil
}

func (s *nuDbShim) metrics() (storeMetrics, error) {
	var result storeMetrics
	if err := s.dbHandle.Flush(); err != nil {
		return result, fmt.Errorf("flush NuDB store %s before measuring committed records: %w", s.filename, err)
	}
	scanner := s.dbHandle.RecordScanner()
	for scanner.Next() {
		if scanner.IsData() {
			if err := result.add(int64(len(scanner.Key())), scanner.Size()); err != nil {
				return result, errors.Join(err, scanner.Close())
			}
		}
	}
	return result, errors.Join(scanner.Err(), scanner.Close())
}

func (s *nuDbShim) Count() (int64, error) {
	result, err := s.metrics()
	return result.count, err
}

func (s *nuDbShim) SizeBytes() (int64, error) {
	result, err := s.metrics()
	return result.bytes, err
}

func (s *PudgeShim) Count() (int64, error) {
	if _, err := s.db.FileSize(); err != nil {
		return 0, err
	}
	count, err := s.db.Count()
	return int64(count), err
}

func (s *PudgeShim) SizeBytes() (int64, error) {
	if _, err := s.db.FileSize(); err != nil {
		return 0, err
	}
	var result storeMetrics
	for offset := 0; ; offset += 256 {
		keys, err := s.db.Keys(nil, 256, offset, true)
		if err != nil {
			return 0, err
		}
		for _, key := range keys {
			value, err := s.Get(key)
			if err != nil {
				return 0, err
			}
			if err := result.add(int64(len(key)), int64(len(value))); err != nil {
				return 0, err
			}
		}
		if len(keys) < 256 {
			return result.bytes, nil
		}
	}
}

func (s *S3Shim) metrics() (storeMetrics, error) {
	var result storeMetrics
	prefix := s.prefix
	if prefix != "" {
		prefix += "/"
	}
	input := &s3.ListObjectsV2Input{Bucket: aws.String(s.bucketName), Prefix: aws.String(prefix)}
	var measurementError error
	err := s.client.ListObjectsV2Pages(input, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, object := range page.Contents {
			if object.Key == nil || object.Size == nil || !strings.HasPrefix(*object.Key, prefix) {
				measurementError = fmt.Errorf("S3 bucket %q returned an invalid object for prefix %q", s.bucketName, prefix)
				return false
			}
			key := strings.TrimPrefix(*object.Key, prefix)
			if err := result.add(int64(len(key)), *object.Size); err != nil {
				measurementError = err
				return false
			}
		}
		return true
	})
	return result, errors.Join(err, measurementError)
}

func (s *S3Shim) Count() (int64, error) {
	result, err := s.metrics()
	return result.count, err
}

func (s *S3Shim) SizeBytes() (int64, error) {
	result, err := s.metrics()
	return result.bytes, err
}
