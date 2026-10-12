package ensemblekv

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

var diskKeyBucket = []byte("keys")
var diskStateBucket = []byte("states")

// Full keys are values: the backing index's key-length limit must not restrict storage keys.
type diskKeyIndex struct {
	db   *bolt.DB
	path string
}

func newDiskKeyIndex(parent string) (*diskKeyIndex, error) {
	path, err := os.MkdirTemp(parent, ".ensemble-keys-")
	if err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(path, "keys.db"), 0600, &bolt.Options{NoSync: true, NoFreelistSync: true})
	if err != nil {
		return nil, errors.Join(err, os.RemoveAll(path))
	}
	index := &diskKeyIndex{db: db, path: path}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucket(diskKeyBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucket(diskStateBucket)
		return err
	})
	if err != nil {
		return nil, errors.Join(err, index.close())
	}
	return index, nil
}

func diskKeyID(tx *bolt.Tx, key []byte) ([]byte, bool) {
	digest := sha256.Sum256(key)
	var id [40]byte
	copy(id[:], digest[:])
	cursor := tx.Bucket(diskKeyBucket).Cursor()
	for storedID, storedKey := cursor.Seek(digest[:]); bytes.HasPrefix(storedID, digest[:]); storedID, storedKey = cursor.Next() {
		if bytes.Equal(storedKey, key) {
			return storedID, true
		}
		binary.BigEndian.PutUint64(id[32:], binary.BigEndian.Uint64(storedID[32:])+1)
	}
	return id[:], false
}

func setDiskKey(tx *bolt.Tx, key []byte, state compactionKeyState) error {
	id, exists := diskKeyID(tx, key)
	if !exists {
		if err := tx.Bucket(diskKeyBucket).Put(id, bytes.Clone(key)); err != nil {
			return err
		}
	}
	return tx.Bucket(diskStateBucket).Put(id, []byte{byte(state)})
}

func (index *diskKeyIndex) set(key []byte, state compactionKeyState) error {
	return index.db.Update(func(tx *bolt.Tx) error { return setDiskKey(tx, key, state) })
}

func (index *diskKeyIndex) get(key []byte) (compactionKeyState, error) {
	var state compactionKeyState
	err := index.db.View(func(tx *bolt.Tx) error {
		id, exists := diskKeyID(tx, key)
		if exists {
			encoded := tx.Bucket(diskStateBucket).Get(id)
			if len(encoded) != 1 {
				return fmt.Errorf("scratch index %s has invalid state for key %x", index.path, key)
			}
			state = compactionKeyState(encoded[0])
		}
		return nil
	})
	return state, err
}

func (index *diskKeyIndex) collect(store KvLike) (result error) {
	tx, err := index.db.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if tx != nil {
			result = errors.Join(result, tx.Rollback())
		}
	}()
	var records, bytesWritten int
	err = streamKeyRecordsAt(store, index.path, func(key []byte, live bool) error {
		state := compactionDeleted
		if live {
			state = compactionPending
		}
		if err := setDiskKey(tx, key, state); err != nil {
			return err
		}
		records++
		bytesWritten += len(key)
		if records >= 128 || bytesWritten >= 1<<20 {
			err := tx.Commit()
			tx = nil
			if err != nil {
				return err
			}
			tx, err = index.db.Begin(true)
			records, bytesWritten = 0, 0
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	err = tx.Commit()
	tx = nil
	return err
}

// Release each read transaction before the callback, which may grow this same index during dual writes.
func (index *diskKeyIndex) each(visit func([]byte, compactionKeyState) error) error {
	var after []byte
	for {
		var key []byte
		var state compactionKeyState
		var found bool
		err := index.db.View(func(tx *bolt.Tx) error {
			cursor := tx.Bucket(diskKeyBucket).Cursor()
			id, value := cursor.First()
			if after != nil {
				id, value = cursor.Seek(after)
				if bytes.Equal(id, after) {
					id, value = cursor.Next()
				}
			}
			if id == nil {
				return nil
			}
			encoded := tx.Bucket(diskStateBucket).Get(id)
			if len(encoded) != 1 {
				return fmt.Errorf("scratch index %s has invalid state at %x", index.path, id)
			}
			after = bytes.Clone(id)
			key, state, found = bytes.Clone(value), compactionKeyState(encoded[0]), true
			return nil
		})
		if err != nil || !found {
			return err
		}
		if err := visit(key, state); err != nil {
			return err
		}
	}
}

func (index *diskKeyIndex) close() error {
	if index == nil || index.db == nil {
		return nil
	}
	if err := index.db.Close(); err != nil {
		return err
	}
	index.db = nil
	return os.RemoveAll(index.path)
}

func forEachLiveKey(store KvLike, visit func([]byte) error) error {
	return forEachLiveKeyAt(store, "", visit)
}

func forEachLiveKeyAt(store KvLike, scratchParent string, visit func([]byte) error) (result error) {
	index, err := newDiskKeyIndex(scratchParent)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, index.close()) }()
	if err := index.collect(store); err != nil {
		return err
	}
	return index.each(func(key []byte, state compactionKeyState) error {
		if state == compactionDeleted {
			return nil
		}
		return visit(key)
	})
}
