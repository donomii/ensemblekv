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
	dataPrefix      = "d:" // Shortened prefix for data entries
	tombstonePrefix = "t:" // Shortened prefix for tombstones
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

// And in the Linelsm struct, add blockSize:
type Linelsm struct {
	DefaultOps
	directory     string
	tiers         [][]KvLike
	metadata      StoreMetadata
	mutex         sync.RWMutex
	maxKeys       int
	blockSize     int // Added this field
	maxTierSizes  []int64
	createStore   func(path string, blockSize int) (KvLike, error)
	flushInterval time.Duration
	closed        chan struct{}
}

// NewLinelsm initializes the LSM store with improved configuration
func NewLinelsm(
	directory string,
	blockSize int,
	maxKeys int,
	createStore func(path string, blockSize int) (KvLike, error),
) (*Linelsm, error) {
	store := &Linelsm{
		directory:     directory,
		maxKeys:       maxKeys,
		createStore:   createStore,
		tiers:         make([][]KvLike, 4), // Start with 4 tiers
		flushInterval: time.Minute * 5,
		closed:        make(chan struct{}),
		maxTierSizes: []int64{
			64 * 1024 * 1024,   // Tier 0: 64MB
			256 * 1024 * 1024,  // Tier 1: 256MB
			1024 * 1024 * 1024, // Tier 2: 1GB
			4096 * 1024 * 1024, // Tier 3: 4GB
		},
	}

	// Store blockSize for use in createStore calls
	store.blockSize = blockSize

	// Ensure the root directory exists
	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create root directory: %w", err)
	}

	// Load or initialize metadata
	if err := store.loadMetadata(); err != nil {
		return nil, fmt.Errorf("failed to load metadata: %w", err)
	}

	// Start background maintenance
	go store.backgroundMaintenance()

	return store, nil
}

// Update getOrCreateActiveStore to use blockSize:
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

// Put stores a key-value pair in the active store of Tier 0
func (l *Linelsm) Put(key, value []byte) error {
    l.mutex.Lock()
    defer l.mutex.Unlock()

    // Get or create the active store in Tier 0
    activeStore, err := l.getOrCreateActiveStore(0)
    if err != nil {
        return fmt.Errorf("failed to get active store: %w", err)
    }

    // Remove any existing tombstone when putting new data
    tombstoneKey := append([]byte(tombstonePrefix), key...)
    if activeStore.Exists(tombstoneKey) {
        if err := activeStore.Delete(tombstoneKey); err != nil {
            return fmt.Errorf("failed to remove tombstone: %w", err)
        }
    }

    // Store with data prefix
    dataKey := append([]byte(dataPrefix), key...)
    if err := activeStore.Put(dataKey, value); err != nil {
        return fmt.Errorf("failed to put data: %w", err)
    }

    // Check if we need to trigger a flush
    if l.shouldFlushTier(0) {
        return l.flushTier(0)
    }

    return nil
}

// Get retrieves the value for a key, respecting tombstones
func (l *Linelsm) Get(key []byte) ([]byte, error) {
    l.mutex.RLock()
    defer l.mutex.RUnlock()

    dataKey := append([]byte(dataPrefix), key...)
    tombstoneKey := append([]byte(tombstonePrefix), key...)
    
    var latestValue []byte
    var foundValue bool
    var tombstoneFound bool
    
    // Search through all tiers from newest to oldest
    for i := 0; i < len(l.tiers); i++ {
        for _, store := range l.tiers[i] {
            // Check for tombstone
            if store.Exists(tombstoneKey) {
                tombstoneFound = true
                break
            }
            
            // Look for value if we haven't found one yet
            if !foundValue {
                if value, err := store.Get(dataKey); err == nil {
                    latestValue = value
                    foundValue = true
                    break
                }
            }
        }
        
        // If we found a tombstone, the key is deleted
        if tombstoneFound {
            return nil, fmt.Errorf("key deleted")
        }
    }
    
    if !foundValue {
        return nil, fmt.Errorf("key not found")
    }
    
    return latestValue, nil
}

// Delete marks a key as deleted using a tombstone
func (l *Linelsm) Delete(key []byte) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	activeStore, err := l.getOrCreateActiveStore(0)
	if err != nil {
		return fmt.Errorf("failed to get active store: %w", err)
	}

	// Add tombstone
	tombstoneKey := append([]byte(tombstonePrefix), key...)
	if err := activeStore.Put(tombstoneKey, nil); err != nil {
		return fmt.Errorf("failed to put tombstone: %w", err)
	}

	return nil
}

// Exists checks if a key exists and is not tombstoned
func (l *Linelsm) Exists(key []byte) bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	// Check for tombstone first
	tombstoneKey := append([]byte(tombstonePrefix), key...)
	for i := len(l.tiers) - 1; i >= 0; i-- {
		for _, store := range l.tiers[i] {
			if store.Exists(tombstoneKey) {
				return false
			}
		}
	}

	// Check for actual key
	dataKey := append([]byte(dataPrefix), key...)
	for i := len(l.tiers) - 1; i >= 0; i-- {
		for _, store := range l.tiers[i] {
			if store.Exists(dataKey) {
				return true
			}
		}
	}

	return false
}

// Flush triggers an immediate flush of all tiers
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

// shouldFlushTier determines if a tier needs flushing based on size
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

// flushTier moves data from one tier to the next
func (l *Linelsm) flushTier(tier int) error {
	if tier+1 >= len(l.tiers) {
		return nil // No higher tier to flush into
	}

	// Create new store in the next tier
	nextTierStore, err := l.getOrCreateActiveStore(tier + 1)
	if err != nil {
		return fmt.Errorf("failed to create next tier store: %w", err)
	}

	// Merge all stores in current tier into the next tier
	for _, store := range l.tiers[tier] {
		if err := l.mergeStore(store, nextTierStore); err != nil {
			return fmt.Errorf("failed to merge store: %w", err)
		}
	}

	// Close and clean up old stores
	for _, store := range l.tiers[tier] {
		if err := store.Close(); err != nil {
			return fmt.Errorf("failed to close store: %w", err)
		}
	}

	// Clear the current tier
	l.tiers[tier] = nil

	// Update metadata
	l.metadata.LastFlush = time.Now()
	if err := l.saveMetadata(); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return nil
}

// mergeStore combines data from one store into another, handling tombstones
func (l *Linelsm) mergeStore(from, to KvLike) error {
    // Track processed keys to handle overwrites correctly
    processedKeys := make(map[string]bool)
    
    // First pass: handle tombstones
    _, err := from.MapFunc(func(k, v []byte) error {
        keyStr := string(k)
        if strings.HasPrefix(keyStr, tombstonePrefix) {
            // Mark the original key as tombstoned
            originalKey := strings.TrimPrefix(keyStr, tombstonePrefix)
            processedKeys[originalKey] = true
            return to.Put(k, v)
        }
        return nil
    })
    if err != nil {
        return err
    }
    
    // Second pass: handle data entries
    _, err = from.MapFunc(func(k, v []byte) error {
        keyStr := string(k)
        if strings.HasPrefix(keyStr, dataPrefix) {
            originalKey := strings.TrimPrefix(keyStr, dataPrefix)
            // Only copy data if the key wasn't tombstoned
            if !processedKeys[originalKey] {
                processedKeys[originalKey] = true
                return to.Put(k, v)
            }
        }
        return nil
    })
    
    return err
}

// MapFunc implements the KvLike interface
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

// backgroundMaintenance performs periodic maintenance tasks
func (l *Linelsm) backgroundMaintenance() {
	ticker := time.NewTicker(l.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			l.mutex.Lock()
			for tier := 0; tier < len(l.tiers); tier++ {
				if l.shouldFlushTier(tier) {
					if err := l.flushTier(tier); err != nil {
						fmt.Printf("Error during maintenance flush: %v\n", err)
					}
				}
			}
			l.mutex.Unlock()
		case <-l.closed:
			return
		}
	}
}

// loadMetadata reads the store metadata from disk
func (l *Linelsm) loadMetadata() error {
	path := filepath.Join(l.directory, metadataFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Initialize new metadata
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

// saveMetadata writes the store metadata to disk
func (l *Linelsm) saveMetadata() error {
	path := filepath.Join(l.directory, metadataFile)
	data, err := json.Marshal(l.metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	return os.WriteFile(path, data, 0644)
}

// Close shuts down the store and persists metadata
func (l *Linelsm) Close() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// Signal background maintenance to stop
	close(l.closed)

	// Flush all tiers
	for tier := 0; tier < len(l.tiers); tier++ {
		if err := l.flushTier(tier); err != nil {
			return fmt.Errorf("failed to flush tier %d during shutdown: %w", tier, err)
		}
	}

	// Close all remaining stores
	for _, tier := range l.tiers {
		for _, store := range tier {
			if err := store.Close(); err != nil {
				return fmt.Errorf("failed to close store: %w", err)
			}
		}
	}

	// Save final metadata state
	return l.saveMetadata()
}

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