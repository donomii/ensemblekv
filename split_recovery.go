package ensemblekv

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const splitPending = ".split.pending"
const splitCommitted = ".split"

type splitLayout struct {
	Version int    `json:"version"`
	Kind    string `json:"kind"`
	Prefix  string `json:"prefix"`
	Depth   int64  `json:"depth"`
}

func splitChildName(kind, prefix string, partition int) string {
	if kind == "tree" {
		return fmt.Sprintf("substore_%x", fmt.Sprintf("%s%x", prefix, partition))
	}
	return fmt.Sprintf("%x", partition)
}

func inspectSplit(directory string, layout splitLayout) (string, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "substore_") || (len(entry.Name()) == 1 && isHexString(entry.Name())) {
			return "", fmt.Errorf("store %s has legacy split child %s without a durable completion record; parent and child authority is ambiguous", directory, entry.Name())
		}
	}
	path := filepath.Join(directory, splitCommitted)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return "", nil
	} else if err != nil {
		return "", err
	}
	data, err := os.ReadFile(filepath.Join(path, "layout.json"))
	if err != nil {
		return "", fmt.Errorf("read committed split layout %s: %w", path, err)
	}
	var saved splitLayout
	if err := json.Unmarshal(data, &saved); err != nil {
		return "", fmt.Errorf("decode committed split layout %s: %w", path, err)
	}
	if saved != layout {
		return "", fmt.Errorf("split layout in %s is %+v; expected %+v", path, saved, layout)
	}
	children, err := os.ReadDir(path)
	if err != nil {
		return "", err
	}
	if len(children) != 17 {
		return "", fmt.Errorf("committed split %s has %d entries; expected layout.json and 16 children", path, len(children))
	}
	for partition := 0; partition < 16; partition++ {
		child := filepath.Join(path, splitChildName(layout.Kind, layout.Prefix, partition))
		if _, err := inspectCompactionRecovery(child); err != nil {
			return "", fmt.Errorf("validate committed split child %s: %w", child, err)
		}
	}
	return path, nil
}

func openSplitCurrent(directory string, blockSize, fileSize int64, creator CreatorFunc) (KvLike, error) {
	if err := os.RemoveAll(filepath.Join(directory, ".split.check")); err != nil {
		return nil, err
	}
	var inventory *compactionRecoveryInventory
	for _, name := range []string{splitPending, splitCommitted} {
		if _, err := os.Stat(filepath.Join(directory, name)); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return nil, err
		}
		var err error
		inventory, err = inspectCompactionRecovery(filepath.Join(directory, "current"))
		if err != nil {
			return nil, fmt.Errorf("recover split source in %s: %w", directory, err)
		}
		break
	}
	current, err := creator(filepath.Join(directory, "current"), blockSize, fileSize)
	if err != nil {
		return nil, err
	}
	if inventory != nil {
		if err := validateCompactionRecoveryStore(filepath.Join(directory, "current"), current, inventory); err != nil {
			return nil, errors.Join(err, current.Close())
		}
		if err := validateSplitSource(current, directory); err != nil {
			return nil, errors.Join(fmt.Errorf("validate split source in %s: %w", directory, err), current.Close())
		}
	}
	return current, nil
}

func prepareSplit(directory string, layout splitLayout, source KvLike) (string, error) {
	if err := validateSplitSource(source, directory); err != nil {
		return "", fmt.Errorf("validate split source in %s: %w", directory, err)
	}
	if err := source.Flush(); err != nil {
		return "", fmt.Errorf("flush split source in %s: %w", directory, err)
	}
	if err := syncCompactionTree(filepath.Join(directory, "current")); err != nil {
		return "", err
	}
	path := filepath.Join(directory, splitPending)
	if err := os.RemoveAll(path); err != nil {
		return "", fmt.Errorf("remove abandoned split %s: %w", path, err)
	}
	if err := os.Mkdir(path, 0755); err != nil {
		return "", err
	}
	data, err := json.Marshal(layout)
	if err != nil {
		return "", err
	}
	file, err := os.OpenFile(filepath.Join(path, "layout.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", err
	}
	_, writeErr := file.Write(data)
	if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		return "", fmt.Errorf("write split layout in %s: %w", path, err)
	}
	return path, syncCompactionPath(directory)
}

func validateSplitSource(source KvLike, directory string) (result error) {
	scratchParent := filepath.Join(directory, ".split.check")
	if err := os.MkdirAll(scratchParent, 0700); err != nil {
		return err
	}
	defer func() { result = errors.Join(result, os.RemoveAll(scratchParent)) }()
	return forEachLiveKeyAt(source, scratchParent, func(key []byte) error {
		if _, err := source.Get(key); err != nil {
			return fmt.Errorf("read split source key %x: %w", key, err)
		}
		return nil
	})
}

func copySplit(source KvLike, scratchParent string, destination func([]byte) KvLike) error {
	return forEachLiveKeyAt(source, scratchParent, func(key []byte) error {
		value, err := source.Get(key)
		if err != nil {
			return fmt.Errorf("read split key %x: %w", key, err)
		}
		if value == nil {
			value = []byte{}
		}
		target := destination(key)
		if target == nil {
			return fmt.Errorf("split key %x does not belong to an available destination prefix", key)
		}
		return target.Put(key, value)
	})
}

func splitNeeded(source KvLike, key, value []byte, limit int64) (bool, error) {
	size, err := StoreSizeBytes(source)
	if err != nil {
		return false, err
	}
	projected := size + int64(len(key)) + int64(len(value))
	if projected <= limit {
		return false, nil
	}
	previous, readErr := source.Get(key)
	exists := readErr == nil
	if exists {
		projected = size - int64(len(previous)) + int64(len(value))
	} else if !errors.Is(readErr, errKeyNotFound) {
		return false, fmt.Errorf("read split candidate key %x: %w", key, readErr)
	}
	if projected <= limit {
		return false, nil
	}
	count, err := StoreCount(source)
	if err != nil {
		return false, err
	}
	if !exists {
		count++
	}
	// Moving a single oversized record cannot reduce the destination's size.
	return count > 1, nil
}

func publishSplit(directory string) (bool, error) {
	pending := filepath.Join(directory, splitPending)
	if err := syncCompactionTree(pending); err != nil {
		return false, err
	}
	if err := os.Rename(pending, filepath.Join(directory, splitCommitted)); err != nil {
		return false, fmt.Errorf("publish split in %s: %w", directory, err)
	}
	// Publication must reach disk before a later child write can be acknowledged.
	return true, syncCompactionPath(directory)
}
