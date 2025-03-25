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
	maxKeys      int
	blockSize    int // Block size for each underlying store
	maxTierSizes []int64
	createStore  func(path string, blockSize int) (KvLike, error)
	// Removed background maintenance members.
}

// NewLinelsm initializes the LSM store with the given configuration.
func NewLinelsm(
	directory string,
	blockSize int,
	maxKeys int,
	createStore func(path string, blockSize int) (KvLike, error),
) (*Linelsm, error) {
	store := &Linelsm{
		directory:    directory,
		maxKeys:      maxKeys,
		createStore:  createStore,
		tiers:        make([][]KvLike, 4), // Start with 4 tiers
		maxTierSizes: []int64{
			64 * 1024 * 1024,   // Tier 0: 64MB
			256 * 1024 * 1024,  // Tier 1: 256MB
			1024 * 1024 * 1024, // Tier 2: 1GB
			4096 * 1024 * 1024, // Tier 3: 4GB
		},
		blockSize: blockSize,
	}

	// Ensure the root directory exists
	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create root directory: %w", err)
	}

	// Load or initialize metadata
	if err := store.loadMetadata(); err != nil {
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
		storePath := filepath.Join(l.directory, fmt.Sprintf("tier-%d-store-%d", tier, time.Now().UnixNano()))
		newStore, err := l.createStore(storePath, l.blockSize) // Use the stored blockSize
		if err != nil {
			return nil, fmt.Errorf("failed to create store: %w", err)
		}
		l.tiers[tier] = append(l.tiers[tier], newStore)
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

	// Remove any existing tombstone when putting new data.
	tombstoneKey := append([]byte(tombstonePrefix), key...)
	if activeStore.Exists(tombstoneKey) {
		if err := activeStore.Delete(tombstoneKey); err != nil {
			return fmt.Errorf("failed to remove tombstone: %w", err)
		}
	}

	// Store with data prefix.
	dataKey := append([]byte(dataPrefix), key...)
	if err := activeStore.Put(dataKey, value); err != nil {
		return fmt.Errorf("failed to put data: %w", err)
	}

	// Check if we need to trigger a flush on Tier 0.
	if l.shouldFlushTier(0) {
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

	var latestValue []byte
	var foundValue bool
	var tombstoneFound bool

	// Search through all tiers from newest to oldest.
	for i := 0; i < len(l.tiers); i++ {
		for _, store := range l.tiers[i] {
			// Check for tombstone.
			if store.Exists(tombstoneKey) {
				tombstoneFound = true
				break
			}

			// Look for value if we haven't found one yet.
			if !foundValue {
				if value, err := store.Get(dataKey); err == nil {
					latestValue = value
					foundValue = true
					break
				}
			}
		}

		// If a tombstone is found, consider the key deleted.
		if tombstoneFound {
			return nil, fmt.Errorf("key deleted")
		}
	}

	if !foundValue {
		return nil, fmt.Errorf("key not found")
	}

	return latestValue, nil
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
	if err := activeStore.Put(tombstoneKey, nil); err != nil {
		return fmt.Errorf("failed to put tombstone: %w", err)
	}

	// Check flush condition on Tier 0.
	if l.shouldFlushTier(0) {
		if err := l.flushTier(0); err != nil {
			return fmt.Errorf("failed to flush tier 0: %w", err)
		}
	}

	return nil
}

// Exists checks if a key exists and is not tombstoned.
func (l *Linelsm) Exists(key []byte) bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	data, err := l.Get(key)
	return err == nil && data != nil
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
	return nil
}

// shouldFlushTier determines if a tier needs flushing based on its total size.
func (l *Linelsm) shouldFlushTier(tier int) bool {
	if tier >= len(l.maxTierSizes) {
		return false
	}

	var totalSize int64
	for _, store := range l.tiers[tier] {
		totalSize += store.Size()
	}

	return totalSize > l.maxTierSizes[tier]
}

// flushTier merges data from the given tier into the next tier.
func (l *Linelsm) flushTier(tier int) error {
	if tier+1 >= len(l.tiers) {
		return nil // No higher tier to flush into.
	}

	// Create a new store in the next tier.
	nextTierStore, err := l.getOrCreateActiveStore(tier + 1)
	if err != nil {
		return fmt.Errorf("failed to create next tier store: %w", err)
	}

	// Merge all stores in the current tier into the next tier.
	for _, store := range l.tiers[tier] {
		if err := l.mergeStore(store, nextTierStore); err != nil {
			return fmt.Errorf("failed to merge store: %w", err)
		}
	}

	// Close and clean up old stores.
	for _, store := range l.tiers[tier] {
		if err := store.Close(); err != nil {
			return fmt.Errorf("failed to close store: %w", err)
		}
	}

	// Clear the current tier.
	l.tiers[tier] = nil

	// Update metadata.
	l.metadata.LastFlush = time.Now()
	if err := l.saveMetadata(); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
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
				return to.Put(k, v)
			}
		}
		return nil
	})

	return err
}

// MapFunc applies a function to every key-value pair in all tiers.
func (l *Linelsm) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	visited := make(map[string]bool)
	for _, tier := range l.tiers {
		for _, store := range tier {
			_, err := store.MapFunc(func(k, v []byte) error {
				visited[string(k)] = true
				return f(k, v)
			})
			if err != nil {
				return nil, err
			}
		}
	}
	return visited, nil
}

// loadMetadata reads the store metadata from disk.
func (l *Linelsm) loadMetadata() error {
	path := filepath.Join(l.directory, metadataFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Initialize new metadata.
			l.metadata = StoreMetadata{
				MaxTierSizes: l.maxTierSizes,
				LastFlush:    time.Now(),
			}
			return nil
		}
		return err
	}

	if err := json.Unmarshal(data, &l.metadata); err != nil {
		return fmt.Errorf("failed to unmarshal metadata: %w", err)
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

	return os.WriteFile(path, data, 0644)
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