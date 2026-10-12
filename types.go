package ensemblekv

import "errors"

var ErrKeyNotFound = errors.New("key not found")

// KeyRecordStreamer emits records from oldest to newest; false marks deletion.
// Keys remain valid only during the callback, whose errors stop iteration and are returned.
type KeyRecordStreamer interface {
	StreamKeyRecords(func([]byte, bool) error) error
}

// StoreMetrics separates current live-key counts from current logical key and value bytes.
// SizeBytes excludes deleted records, superseded values, storage metadata, and reserved capacity.
type StoreMetrics interface {
	Count() (int64, error)
	SizeBytes() (int64, error)
}

func StoreCount(store KvLike) (int64, error) {
	if metrics, ok := store.(StoreMetrics); ok {
		return metrics.Count()
	}
	return countLiveKeys(store)
}

func StoreSizeBytes(store KvLike) (int64, error) {
	if metrics, ok := store.(StoreMetrics); ok {
		return metrics.SizeBytes()
	}
	return liveSizeBytes(store)
}
