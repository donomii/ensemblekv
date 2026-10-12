/*
lsmkv - A Multi-Tier Log-Structured Merge (LSM) Key-Value Store

This package implements an LSM key–value store with multiple tiers designed for efficient write buffering
and on-demand compaction. Data is initially written to Tier 0 (the active store) and is flushed upward to higher tiers
when the configured size limits are exceeded.

Key Features:
  - **Prefixed Keys:**
    Every key is automatically stored with a prefix to avoid conflicts. Data entries are prefixed with "d:"
    and tombstones (deletion markers) with "t:". This ensures that a user’s key never accidentally conflicts
    with a tombstone marker.

  - **On-Demand Compaction:**
    Instead of running a continuous background process, this implementation checks after every write. If the active tier
    exceeds its maximum size, the store immediately triggers a flush (merge) into the next tier.

  - **Multi-Tier Architecture:**
    Data is organized into multiple tiers (by default, 4 tiers). New writes are inserted into Tier 0, and when
    a tier's size limit is reached, its contents are merged upward to higher tiers.

  - **Tombstone-Based Deletion:**
    Deletions are handled by inserting tombstones rather than removing keys immediately. This mechanism ensures
    that deleted keys are not returned during lookups, even if remnants exist in older tiers.

Usage:
  1. **Initialization:**
     Call `NewLinelsm(directory, blockSize, maxKeys, createStore)` to create a new LSM store instance.
     The `createStore` function should return an implementation of the `KvLike` interface, which provides basic
     operations such as Put, Get, Delete, Exists, Size, MapFunc, and Close.

  2. **Operations:**
     - `Put(key, value []byte) error`: Inserts or updates key–value pairs in Tier 0, automatically applying the
       appropriate prefix to the key.
     - `Get(key []byte) ([]byte, error)`: Retrieves the value associated with a key, ensuring that any tombstone
       marker is respected.
     - `Delete(key []byte) error`: Marks a key as deleted by inserting a tombstone.
     - `Flush() error`: Manually triggers a flush (merge) of all tiers.
     - `Size() int64`: Returns the total size of all data across tiers.
     - `Close() error`: Shuts down the store gracefully, ensuring all pending data is flushed and metadata is persisted.

This file contains the core implementation of the LSM store, including:
  - Automatic key prefixing to distinguish between data and tombstones.
  - On-demand compaction triggered during write operations.
  - Flush and merge operations across tiers.
  - Persistent metadata management (stored in "metadata.json").
*/

package ensemblekv

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Constants for store configuration
const (
	dataPrefix      = "d:" // Prefix for data entries
	tombstonePrefix = "t:" // Prefix for tombstones
	metadataFile    = "metadata.json"
)

// TierMetadata stores information about a single tier
type TierMetadata struct {
	Level     int      `json:"level"`
	StoreIds  []string `json:"store_ids"`
	TotalSize int64    `json:"total_size"`
}

// StoreMetadata represents the persistent state of the LSM store
type StoreMetadata struct {
	Tiers        []TierMetadata `json:"tiers"`
	MaxTierSizes []int64        `json:"max_tier_sizes"`
	LastFlush    time.Time      `json:"last_flush"`
}

// Linelsm is the core LSM store structure.
type Linelsm struct {
	DefaultOps
	directory    string
	tiers        [][]KvLike
	metadata     StoreMetadata
	mutex        sync.RWMutex
	maxKeys      int64
	blockSize    int64 // Block size for each underlying store
	fileSize     int64 // File size for each underlying store
	maxTierSizes []int64
	createStore  CreatorFunc
	// Removed background maintenance members.
}

// NewLinelsm initializes the LSM store with the given configuration.
func NewLinelsm(
	directory string,
	blockSize int64,
	maxKeys int64,
	fileSize int64,
	createStore CreatorFunc,
) (*Linelsm, error) {
	store := &Linelsm{
		directory:   directory,
		maxKeys:     maxKeys,
		createStore: createStore,
		tiers:       make([][]KvLike, 4), // Start with 4 tiers
		maxTierSizes: []int64{
			64 * 1024 * 1024,   // Tier 0: 64MB
			256 * 1024 * 1024,  // Tier 1: 256MB
			1024 * 1024 * 1024, // Tier 2: 1GB
			4096 * 1024 * 1024, // Tier 3: 4GB
		},
		blockSize: blockSize,
		fileSize:  fileSize,
	}

	// Ensure the root directory exists
	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create root directory: %w", err)
	}

	// Load or initialize metadata
	if err := store.loadMetadata(); err != nil {
		for _, tier := range store.tiers {
			for _, backingStore := range tier {
				backingStore.Close()
			}
		}
		return nil, fmt.Errorf("failed to load metadata: %w", err)
	}

	// Background maintenance has been removed.
	return store, nil
}

// getOrCreateActiveStore returns the active store for the given tier,
// creating one if necessary.
func (l *Linelsm) getOrCreateActiveStore(tier int) (KvLike, error) {
	if tier >= len(l.tiers) {
		return nil, fmt.Errorf("tier %d exceeds maximum tier count", tier)
	}

	if len(l.tiers[tier]) == 0 {
		storeID := fmt.Sprintf("tier-%d-store-%d", tier, time.Now().UnixNano())
		storePath := filepath.Join(l.directory, storeID)
		newStore, err := l.createStore(storePath, l.blockSize, l.fileSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create store: %w", err)
		}
		l.tiers[tier] = append(l.tiers[tier], newStore)
		l.metadata.Tiers[tier].StoreIds = append(l.metadata.Tiers[tier].StoreIds, storeID)
		if err := l.saveMetadata(); err != nil {
			l.tiers[tier] = nil
			l.metadata.Tiers[tier].StoreIds = nil
			newStore.Close()
			return nil, fmt.Errorf("failed to save new tier %d store metadata: %w", tier, err)
		}
	}
	return l.tiers[tier][0], nil
}

// Put stores a key-value pair in the active store of Tier 0.
// The provided key is automatically prefixed with "d:".
// If a tombstone exists for the key, it is removed.
// After the write, a flush is triggered if needed.
func (l *Linelsm) Put(key, value []byte) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// Get or create the active store in Tier 0
	activeStore, err := l.getOrCreateActiveStore(0)
	if err != nil {
		return fmt.Errorf("failed to get active store: %w", err)
	}

	// Store with data prefix.
	dataKey := append([]byte(dataPrefix), key...)
	if err := activeStore.Put(dataKey, value); err != nil {
		return fmt.Errorf("failed to put data: %w", err)
	}

	// Remove any existing tombstone when putting new data.
	tombstoneKey := append([]byte(tombstonePrefix), key...)
	if activeStore.Exists(tombstoneKey) {
		if err := activeStore.Delete(tombstoneKey); err != nil {
			return fmt.Errorf("failed to remove tombstone: %w", err)
		}
	}

	// Check if we need to trigger a flush on Tier 0.
	shouldFlush, err := l.shouldFlushTier(0)
	if err != nil {
		return fmt.Errorf("failed to measure tier 0: %w", err)
	}
	if shouldFlush {
		if err := l.flushTier(0); err != nil {
			return fmt.Errorf("failed to flush tier 0: %w", err)
		}
	}

	return nil
}

// Get retrieves the value for a key, respecting tombstones.
// The provided key is automatically prefixed with "d:" (for data)
// and "t:" (for tombstone) is used to check for deletions.
func (l *Linelsm) Get(key []byte) ([]byte, error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	dataKey := append([]byte(dataPrefix), key...)
	tombstoneKey := append([]byte(tombstonePrefix), key...)

	// Search through all tiers from newest to oldest.
	for i := 0; i < len(l.tiers); i++ {
		for _, store := range l.tiers[i] {
			// Check for tombstone.
			if _, err := store.Get(tombstoneKey); err == nil {
				return nil, fmt.Errorf("key %q deleted: %w", key, errKeyNotFound)
			} else if !errors.Is(err, errKeyNotFound) {
				return nil, fmt.Errorf("read LineLSM tier %d tombstone for key %q: %w", i, key, err)
			}

			// The first value is newer than entries in the remaining tiers.
			if value, err := store.Get(dataKey); err == nil {
				return value, nil
			} else if !errors.Is(err, errKeyNotFound) {
				return nil, fmt.Errorf("read LineLSM tier %d value for key %q: %w", i, key, err)
			}
		}
	}

	return nil, fmt.Errorf("key %q: %w", key, errKeyNotFound)
}

// Delete marks a key as deleted by inserting a tombstone.
// The provided key is automatically prefixed with "t:".
// After the write, a flush is triggered if needed.
func (l *Linelsm) Delete(key []byte) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	activeStore, err := l.getOrCreateActiveStore(0)
	if err != nil {
		return fmt.Errorf("failed to get active store: %w", err)
	}

	// Insert tombstone.
	tombstoneKey := append([]byte(tombstonePrefix), key...)
	if err := activeStore.Put(tombstoneKey, []byte{1}); err != nil {
		return fmt.Errorf("failed to put tombstone: %w", err)
	}

	// Check flush condition on Tier 0.
	shouldFlush, err := l.shouldFlushTier(0)
	if err != nil {
		return fmt.Errorf("failed to measure tier 0: %w", err)
	}
	if shouldFlush {
		if err := l.flushTier(0); err != nil {
			return fmt.Errorf("failed to flush tier 0: %w", err)
		}
	}

	return nil
}

// Exists checks if a key exists and is not tombstoned.
func (l *Linelsm) Exists(key []byte) bool {
	_, err := l.Get(key)
	return err == nil
}

// Flush triggers an immediate flush (merge) of all tiers.
func (l *Linelsm) Flush() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	for tier := 0; tier < len(l.tiers); tier++ {
		if err := l.flushTier(tier); err != nil {
			return fmt.Errorf("failed to flush tier %d: %w", tier, err)
		}
	}
	for _, tier := range l.tiers {
		for _, store := range tier {
			if err := store.Flush(); err != nil {
				return fmt.Errorf("failed to flush backing store: %w", err)
			}
		}
	}
	return nil
}

func (l *Linelsm) Keys() [][]byte {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	keys := make([][]byte, 0)
	for key, exists := range l.keyStates() {
		if exists {
			keys = append(keys, []byte(key))
		}
	}

	return keys
}

// shouldFlushTier determines if a tier needs flushing based on its total size.
func (l *Linelsm) shouldFlushTier(tier int) (bool, error) {
	if tier >= len(l.maxTierSizes) {
		return false, nil
	}

	var totalSize int64
	for _, store := range l.tiers[tier] {
		size, err := StoreSizeBytes(store)
		if err != nil {
			return false, err
		}
		totalSize += size
	}

	return totalSize > l.maxTierSizes[tier], nil
}

// flushTier merges data from the given tier into the next tier.
func (l *Linelsm) flushTier(tier int) error {
	if tier+1 >= len(l.tiers) || len(l.tiers[tier]) == 0 {
		return nil // No higher tier to flush into.
	}

	// Create a new store in the next tier.
	nextTierStore, err := l.getOrCreateActiveStore(tier + 1)
	if err != nil {
		return fmt.Errorf("failed to create next tier store: %w", err)
	}

	// Merge all stores in the current tier into the next tier.
	for index := len(l.tiers[tier]) - 1; index >= 0; index-- {
		if err := l.mergeStore(l.tiers[tier][index], nextTierStore); err != nil {
			return fmt.Errorf("failed to merge store: %w", err)
		}
	}
	if err := nextTierStore.Flush(); err != nil {
		return fmt.Errorf("failed to flush merged store: %w", err)
	}

	oldStoreIDs := l.metadata.Tiers[tier].StoreIds
	l.metadata.Tiers[tier].StoreIds = nil

	// Update metadata.
	l.metadata.LastFlush = time.Now()
	if err := l.saveMetadata(); err != nil {
		l.metadata.Tiers[tier].StoreIds = oldStoreIDs
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	// Clear the current tier.
	oldStores := l.tiers[tier]
	l.tiers[tier] = nil

	// Close and clean up old stores.
	for _, store := range oldStores {
		if err := store.Close(); err != nil {
			return fmt.Errorf("failed to close store: %w", err)
		}
	}

	return nil
}

// mergeStore combines data from one store into another, handling tombstones.
func (l *Linelsm) mergeStore(from, to KvLike) error {
	// Track processed keys to handle overwrites correctly.
	processedKeys := make(map[string]bool)

	// First pass: handle tombstones.
	_, err := from.MapFunc(func(k, v []byte) error {
		keyStr := string(k)
		if strings.HasPrefix(keyStr, tombstonePrefix) {
			originalKey := strings.TrimPrefix(keyStr, tombstonePrefix)
			processedKeys[originalKey] = true
			return to.Put(k, v)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Second pass: handle data entries.
	_, err = from.MapFunc(func(k, v []byte) error {
		keyStr := string(k)
		if strings.HasPrefix(keyStr, dataPrefix) {
			originalKey := strings.TrimPrefix(keyStr, dataPrefix)
			// Only copy data if the key wasn't tombstoned.
			if !processedKeys[originalKey] {
				processedKeys[originalKey] = true
				if err := to.Put(k, v); err != nil {
					return err
				}
				tombstoneKey := []byte(tombstonePrefix + originalKey)
				if to.Exists(tombstoneKey) {
					if err := to.Delete(tombstoneKey); err != nil {
						return fmt.Errorf("failed to remove destination tombstone for %q: %w", originalKey, err)
					}
				}
			}
		}
		return nil
	})

	return err
}

// MapFunc applies a function to every key-value pair in all tiers.
func (l *Linelsm) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	l.mutex.RLock()
	states := l.keyStates()
	l.mutex.RUnlock()

	visited := make(map[string]bool)
	for key, exists := range states {
		if !exists {
			visited[key] = false
			continue
		}
		value, err := l.Get([]byte(key))
		if err != nil {
			return visited, err
		}
		visited[key] = true
		if err := f([]byte(key), value); err != nil {
			return visited, err
		}
	}
	return visited, nil
}

func (l *Linelsm) keyStates() map[string]bool {
	visited := make(map[string]bool)
	for _, tier := range l.tiers {
		for _, store := range tier {
			keys := store.Keys()
			for _, key := range keys {
				if strings.HasPrefix(string(key), tombstonePrefix) {
					originalKey := string(key[len(tombstonePrefix):])
					if _, exists := visited[originalKey]; !exists {
						visited[originalKey] = false
					}
				}
			}
			for _, k := range keys {
				if !strings.HasPrefix(string(k), dataPrefix) {
					continue
				}
				key := k[len(dataPrefix):]
				if _, exists := visited[string(key)]; exists {
					continue
				}
				visited[string(key)] = true
			}
		}
	}
	return visited
}

// loadMetadata reads the store metadata from disk.
func (l *Linelsm) loadMetadata() error {
	path := filepath.Join(l.directory, metadataFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			if err := l.checkUnreferencedStores(); err != nil {
				return err
			}
			// Initialize new metadata.
			l.metadata = StoreMetadata{
				Tiers:        make([]TierMetadata, len(l.tiers)),
				MaxTierSizes: l.maxTierSizes,
				LastFlush:    time.Now(),
			}
			for level := range l.metadata.Tiers {
				l.metadata.Tiers[level].Level = level
			}
			return nil
		}
		return err
	}

	if err := json.Unmarshal(data, &l.metadata); err != nil {
		return fmt.Errorf("failed to unmarshal metadata: %w", err)
	}
	if len(l.metadata.Tiers) == 0 {
		if err := l.checkUnreferencedStores(); err != nil {
			return err
		}
	}
	loadedTiers := l.metadata.Tiers
	l.metadata.Tiers = make([]TierMetadata, len(l.tiers))
	for level := range l.metadata.Tiers {
		l.metadata.Tiers[level].Level = level
	}
	seenLevels := make(map[int]bool)
	seenStores := make(map[string]bool)
	for _, tier := range loadedTiers {
		if tier.Level < 0 || tier.Level >= len(l.tiers) {
			return fmt.Errorf("invalid tier level %d; expected 0 through %d", tier.Level, len(l.tiers)-1)
		}
		if seenLevels[tier.Level] {
			return fmt.Errorf("duplicate tier level %d in metadata", tier.Level)
		}
		seenLevels[tier.Level] = true
		for _, storeID := range tier.StoreIds {
			if filepath.Base(storeID) != storeID || !strings.HasPrefix(storeID, fmt.Sprintf("tier-%d-store-", tier.Level)) {
				return fmt.Errorf("invalid store ID %q for tier %d", storeID, tier.Level)
			}
			if seenStores[storeID] {
				return fmt.Errorf("duplicate store ID %q in metadata", storeID)
			}
			seenStores[storeID] = true
			storePath := filepath.Join(l.directory, storeID)
			if _, err := os.Stat(storePath); err != nil {
				return fmt.Errorf("failed to find tier %d store %q: %w", tier.Level, storeID, err)
			}
			store, err := l.createStore(storePath, l.blockSize, l.fileSize)
			if err != nil {
				return fmt.Errorf("failed to open tier %d store %q: %w", tier.Level, storeID, err)
			}
			l.tiers[tier.Level] = append(l.tiers[tier.Level], store)
		}
		l.metadata.Tiers[tier.Level] = tier
	}

	return nil
}

func (l *Linelsm) checkUnreferencedStores() error {
	entries, err := os.ReadDir(l.directory)
	if err != nil {
		return fmt.Errorf("failed to inspect tier directories: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "tier-") {
			return fmt.Errorf("tier directory %q exists without tier references in %q", entry.Name(), filepath.Join(l.directory, metadataFile))
		}
	}
	return nil
}

// saveMetadata writes the store metadata to disk.
func (l *Linelsm) saveMetadata() error {
	path := filepath.Join(l.directory, metadataFile)
	data, err := json.Marshal(l.metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	temporaryPath := path + ".tmp"
	file, err := os.OpenFile(temporaryPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to create metadata file %q: %w", temporaryPath, err)
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return fmt.Errorf("failed to write metadata file %q: %w", temporaryPath, err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("failed to sync metadata file %q: %w", temporaryPath, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close metadata file %q: %w", temporaryPath, err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("failed to replace metadata file %q: %w", path, err)
	}
	directory, err := os.Open(l.directory)
	if err != nil {
		return fmt.Errorf("failed to open metadata directory %q: %w", l.directory, err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("failed to sync metadata directory %q: %w", l.directory, err)
	}
	return nil
}

// Close shuts down the store gracefully by flushing all tiers,
// closing all underlying stores, and saving metadata.
func (l *Linelsm) Close() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// Flush all tiers.
	for tier := 0; tier < len(l.tiers); tier++ {
		if err := l.flushTier(tier); err != nil {
			return fmt.Errorf("failed to flush tier %d during shutdown: %w", tier, err)
		}
	}

	// Close all remaining stores.
	for _, tier := range l.tiers {
		for _, store := range tier {
			if err := store.Close(); err != nil {
				return fmt.Errorf("failed to close store: %w", err)
			}
		}
	}

	// Save final metadata state.
	return l.saveMetadata()
}

// Size returns the total size of all data across tiers.
func (l *Linelsm) Size() int64 {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	var total int64
	for _, tier := range l.tiers {
		for _, store := range tier {
			total += store.Size()
		}
	}
	return total
}

func (l *Linelsm) KeyHistory(key []byte) ([][]byte, error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	allHistory := make([][]byte, 0)

	// Linelsm uses prefixed keys
	dataKey := append([]byte(dataPrefix), key...)

	// Check all tiers
	for _, tier := range l.tiers {
		for _, store := range tier {
			// Try to get value for data key
			value, err := store.Get(dataKey)
			if err == nil {
				allHistory = append(allHistory, value)
			}
		}
	}

	return allHistory, nil
}

func (l *Linelsm) MapPrefixFunc(prefix []byte, f func([]byte, []byte) error) (map[string]bool, error) {
	keys := make(map[string]bool)
	visited, err := l.MapFunc(func(key, value []byte) error {
		if strings.HasPrefix(string(key), string(prefix)) {
			return f(key, value)
		}
		return nil
	})
	for key, exists := range visited {
		if strings.HasPrefix(key, string(prefix)) {
			keys[key] = exists
		}
	}
	return keys, err
}
