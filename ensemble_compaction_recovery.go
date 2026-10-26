package ensemblekv

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type compactionRecoveryEntry struct {
	directory bool
	size      int64
}

type compactionRecoveryInventory struct {
	entries  map[string]compactionRecoveryEntry
	layouts  map[string]string
	children map[string][]string
}

func inspectCompactionRecovery(path string) (*compactionRecoveryInventory, error) {
	inventory := &compactionRecoveryInventory{
		entries:  make(map[string]compactionRecoveryEntry),
		layouts:  make(map[string]string),
		children: make(map[string][]string),
	}
	err := filepath.WalkDir(path, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && !entry.Type().IsRegular() {
			return fmt.Errorf("recovery path %s is neither a regular file nor a directory", current)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		inventory.entries[current] = compactionRecoveryEntry{directory: entry.IsDir(), size: info.Size()}
		if entry.IsDir() {
			parent := filepath.Dir(current)
			inventory.children[parent] = append(inventory.children[parent], current)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("inspect existing compaction recovery path %s: %w", path, err)
	}
	if err := inventory.inspectStore(path); err != nil {
		return nil, err
	}
	return inventory, nil
}

func (inventory *compactionRecoveryInventory) requireFile(path string, minimumSize int64) error {
	entry, exists := inventory.entries[path]
	if !exists || entry.directory || entry.size < minimumSize {
		return fmt.Errorf("recovery requires existing regular file %s with at least %d bytes; present=%t, directory=%t, bytes=%d", path, minimumSize, exists, entry.directory, entry.size)
	}
	return nil
}

func (inventory *compactionRecoveryInventory) inspectStore(path string) error {
	entry, exists := inventory.entries[path]
	if !exists || !entry.directory {
		return fmt.Errorf("recovery requires existing store directory %s", path)
	}
	leafFiles := map[string]map[string]int64{
		"json":       {"store.json": 2},
		"bolt":       {"bolt.db": 1},
		"sqlite":     {"kv.sqlite": 100},
		"singlefile": {"singlefilekv": singleFileHeaderSize},
		"mmap":       {"data.log": headerSize},
		"mega":       {"megapool.db": 256},
		"extent":     {"keys.dat": 0, "values.dat": 0, "keys.index": 8, "values.index": 8},
		"pudge":      {"pudge.db": 0, "pudge.db.idx": 0},
	}
	var layout string
	for candidate, files := range leafFiles {
		for name := range files {
			if _, exists := inventory.entries[filepath.Join(path, name)]; exists {
				if layout != "" && layout != candidate {
					return fmt.Errorf("ambiguous recovery store %s contains %s and %s layouts", path, layout, candidate)
				}
				layout = candidate
			}
		}
	}
	_, metadata := inventory.entries[filepath.Join(path, metadataFile)]
	branch := false
	for _, child := range inventory.children[path] {
		name := filepath.Base(child)
		if name == "current" || strings.HasPrefix(name, "substore_") || (len(name) == 1 && isHexString(name)) {
			branch = true
		}
	}
	if layout != "" {
		if metadata || branch {
			return fmt.Errorf("ambiguous recovery store %s contains native and manager layouts", path)
		}
		for name, minimumSize := range leafFiles[layout] {
			if err := inventory.requireFile(filepath.Join(path, name), minimumSize); err != nil {
				return err
			}
		}
		inventory.layouts[path] = layout
		return nil
	}
	if metadata {
		return inventory.inspectMetadataStore(path)
	}
	if branch {
		inventory.layouts[path] = "branch"
		if err := inventory.inspectStore(filepath.Join(path, "current")); err != nil {
			return err
		}
		for _, child := range inventory.children[path] {
			name := filepath.Base(child)
			if strings.HasPrefix(name, "substore_") || (len(name) == 1 && isHexString(name)) {
				if err := inventory.inspectStore(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return fmt.Errorf("recovery store %s has no recognized existing storage files", path)
}

func (inventory *compactionRecoveryInventory) inspectMetadataStore(path string) error {
	metadataPath := filepath.Join(path, metadataFile)
	if err := inventory.requireFile(metadataPath, 2); err != nil {
		return err
	}
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return fmt.Errorf("read recovery metadata %s: %w", metadataPath, err)
	}
	if _, exists := fields["tiers"]; exists {
		var metadata StoreMetadata
		if err := json.Unmarshal(data, &metadata); err != nil {
			return err
		}
		if len(metadata.Tiers) == 0 {
			return fmt.Errorf("recovery metadata %s has no tier descriptions", metadataPath)
		}
		inventory.layouts[path] = "line"
		for _, tier := range metadata.Tiers {
			for _, storeID := range tier.StoreIds {
				if filepath.Base(storeID) != storeID || !strings.HasPrefix(storeID, fmt.Sprintf("tier-%d-store-", tier.Level)) {
					return fmt.Errorf("recovery metadata %s contains invalid tier store %q", metadataPath, storeID)
				}
				if err := inventory.inspectStore(filepath.Join(path, storeID)); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if _, exists := fields["n"]; exists {
		var metadata Metadata
		if err := json.Unmarshal(data, &metadata); err != nil {
			return err
		}
		if metadata.N <= 0 || metadata.Generation < 0 {
			return fmt.Errorf("recovery metadata %s has invalid shard count %d or generation %d", metadataPath, metadata.N, metadata.Generation)
		}
		inventory.layouts[path] = "ensemble"
		for index := int64(0); index < metadata.N; index++ {
			if err := inventory.inspectStore(filepath.Join(path, fmt.Sprint(metadata.Generation), fmt.Sprint(index))); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("recovery metadata %s does not describe a supported manager", metadataPath)
}

func validateCompactionRecoveryStore(path string, store KvLike, inventory *compactionRecoveryInventory) error {
	var expected string
	switch source := store.(type) {
	case *JsonKV:
		expected = "json"
	case *BoltDbShim:
		expected = "bolt"
	case *SQLiteKV:
		expected = "sqlite"
	case *SingleFileLSM:
		expected = "singlefile"
	case *MmapSingleKV:
		expected = "mmap"
	case *MegaPool:
		expected = "mega"
	case *ExtentKeyValStore:
		expected = "extent"
	case *PudgeShim:
		expected = "pudge"
	case *Linelsm:
		expected = "line"
		for level, stores := range source.tiers {
			for index, child := range stores {
				childPath := filepath.Join(path, source.metadata.Tiers[level].StoreIds[index])
				if err := validateCompactionRecoveryStore(childPath, child, inventory); err != nil {
					return err
				}
			}
		}
	case *EnsembleKv:
		expected = "ensemble"
		for index := int64(0); index < source.N; index++ {
			child, exists := source.substores.Load(int(index))
			if !exists {
				return fmt.Errorf("opened recovery store %s has no shard %d", path, index)
			}
			childPath := filepath.Join(path, fmt.Sprint(source.generation), fmt.Sprint(index))
			if err := validateCompactionRecoveryStore(childPath, child, inventory); err != nil {
				return err
			}
		}
	case *TreeLSM:
		expected = "branch"
		if err := validateCompactionRecoveryStore(filepath.Join(path, "current"), source.currentStore, inventory); err != nil {
			return err
		}
		for _, child := range source.subStores {
			if err := validateCompactionRecoveryStore(child.directory, child, inventory); err != nil {
				return err
			}
		}
	case *StarLSM:
		expected = "branch"
		if err := validateCompactionRecoveryStore(filepath.Join(path, "current"), source.currentStore, inventory); err != nil {
			return err
		}
		for _, child := range source.subStores {
			if err := validateCompactionRecoveryStore(child.directory, child, inventory); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("recovery of backend %T requires a supported existing-store layout", store)
	}
	if inventory.layouts[path] != expected {
		return fmt.Errorf("opened recovery backend %T at %s requires %s storage; existing layout was %q", store, path, expected, inventory.layouts[path])
	}
	return nil
}
