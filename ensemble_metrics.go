package ensemblekv

import (
	"errors"
	"fmt"
	"math"
)

func checkedKeyExists(store KvLike, key []byte) (bool, error) {
	_, err := store.Get(key)
	if errors.Is(err, errKeyNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (s *EnsembleKv) countMutation(id int, store KvLike, key []byte, mutate func() error) error {
	before, err := checkedKeyExists(store, key)
	if err != nil {
		return fmt.Errorf("inspect substore %d key %x before mutation: %w", id, key, err)
	}
	mutationErr := mutate()
	s.flushStore.Store(id, true)
	after, err := checkedKeyExists(store, key)
	if err != nil {
		countErr := fmt.Errorf("inspect substore %d key %x after mutation: %w", id, key, err)
		s.countErrors.Store(id, countErr)
		return errors.Join(mutationErr, countErr)
	}
	if before != after {
		if after {
			s.totalKeys.Add(1)
		} else {
			s.totalKeys.Add(-1)
		}
	}
	return mutationErr
}

func (s *EnsembleKv) Count() (int64, error) {
	if !s.running.Load() {
		return 0, fmt.Errorf("Ensemble store %s is closed", s.directory)
	}
	for id := 0; id < int(s.N); id++ {
		if err, failed := s.unavailable.Load(id); failed {
			return 0, fmt.Errorf("count substore %d: %w", id, err)
		}
		if err, failed := s.countErrors.Load(id); failed {
			return 0, fmt.Errorf("key count is unknown until reopen: %w", err)
		}
	}
	return s.totalKeys.Load(), nil
}

func (s *EnsembleKv) SizeBytes() (int64, error) {
	var total int64
	for id := 0; id < int(s.N); id++ {
		err := s.runInSubstoreWithErr(id, func(store KvLike) error {
			size, err := StoreSizeBytes(store)
			if err != nil {
				return err
			}
			if size < 0 || total > math.MaxInt64-size {
				return fmt.Errorf("Ensemble byte size exceeds %d", int64(math.MaxInt64))
			}
			total += size
			return nil
		})
		if err != nil {
			return 0, fmt.Errorf("measure substore %d: %w", id, err)
		}
	}
	return total, nil
}
