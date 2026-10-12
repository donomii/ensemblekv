package ensemblekv

import (
	"bytes"
	"fmt"
)

func streamEnsembleKeys(source *EnsembleKv, scratchParent string, visit func([]byte, bool) error) error {
	if !source.running.Load() {
		return fmt.Errorf("Ensemble compaction source %s is closed", source.directory)
	}
	for index := int64(0); index < source.N; index++ {
		err := source.runInSubstoreWithErr(int(index), func(child KvLike) error {
			return streamKeyRecordsAt(child, scratchParent, visit)
		})
		if err != nil {
			return fmt.Errorf("Ensemble substore %d: %w", index, err)
		}
	}
	return nil
}

func streamLineKeys(source *Linelsm, scratchParent string, visit func([]byte, bool) error) error {
	source.mutex.RLock()
	defer source.mutex.RUnlock()
	for level := len(source.tiers) - 1; level >= 0; level-- {
		tier := source.tiers[level]
		for index := len(tier) - 1; index >= 0; index-- {
			// Within a store, a tombstone takes precedence over data for the same logical key.
			for _, prefix := range []string{dataPrefix, tombstonePrefix} {
				err := forEachLiveKeyAt(tier[index], scratchParent, func(key []byte) error {
					if bytes.HasPrefix(key, []byte(prefix)) {
						return visit(key[len(prefix):], prefix == dataPrefix)
					}
					return nil
				})
				if err != nil {
					return fmt.Errorf("LineLSM tier %d store %d: %w", level, index, err)
				}
			}
		}
	}
	return nil
}

func streamTreeKeys(source *TreeLSM, scratchParent string, visit func([]byte, bool) error) error {
	source.mutex.RLock()
	defer source.mutex.RUnlock()
	if err := source.available(); err != nil {
		return err
	}
	if !source.isReadOnly {
		return streamKeyRecordsAt(source.currentStore, scratchParent, visit)
	}
	if len(source.subStores) != 16 {
		return fmt.Errorf("TreeLSM compaction source %s has %d children; a completed split requires 16", source.directory, len(source.subStores))
	}
	for partition := 0; partition < 16; partition++ {
		prefix := fmt.Sprintf("%s%x", source.prefix, partition)
		child := source.subStores[prefix]
		if child == nil {
			return fmt.Errorf("TreeLSM compaction source %s is missing child %s", source.directory, prefix)
		}
		if err := streamTreeKeys(child, scratchParent, visit); err != nil {
			return fmt.Errorf("TreeLSM child %q: %w", prefix, err)
		}
	}
	return nil
}

func streamStarKeys(source *StarLSM, scratchParent string, visit func([]byte, bool) error) error {
	source.mutex.RLock()
	defer source.mutex.RUnlock()
	if err := source.available(); err != nil {
		return err
	}
	if !source.isReadOnly {
		return streamKeyRecordsAt(source.currentStore, scratchParent, visit)
	}
	if len(source.subStores) != 16 {
		return fmt.Errorf("StarLSM compaction source %s has %d children; a completed split requires 16", source.directory, len(source.subStores))
	}
	for partition := 0; partition < 16; partition++ {
		prefix := fmt.Sprintf("%s%x", source.prefix, partition)
		child := source.subStores[prefix]
		if child == nil {
			return fmt.Errorf("StarLSM compaction source %s is missing child %s", source.directory, prefix)
		}
		if err := streamStarKeys(child, scratchParent, visit); err != nil {
			return fmt.Errorf("StarLSM child %q: %w", prefix, err)
		}
	}
	return nil
}
