package ensemblekv

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

type compactionKeyState uint8

const compactionMarker = "ensemble-compaction-v1\n"

const (
	compactionCopied compactionKeyState = iota + 1
	compactionDeleted
	compactionPending
)

type compactionState struct {
	target       KvLike
	keys         *diskKeyIndex
	failure      error
	subPath      string
	targetClosed bool
	sourceClosed bool
	sourceMoved  bool
	targetMoved  bool
	finished     bool
}

func (s *EnsembleKv) compactSubstore(id int) (result error) {
	state, keys, err := s.prepareCompaction(id)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, s.abortCompaction(id, state)) }()
	// Iterate over keys from original store
	if err := keys.each(func(key []byte, status compactionKeyState) error {
		return s.copyCompactionKey(id, state, key)
	}); err != nil {
		return err
	}
	return s.finishCompaction(id, state)
}

func (s *EnsembleKv) prepareCompaction(id int) (*compactionState, *diskKeyIndex, error) {
	state := &compactionState{
		subPath: filepath.Join(s.directory, fmt.Sprint(s.generation), fmt.Sprint(id)),
	}
	err := s.runInSubstoreWithErr(id, func(source KvLike) error {
		if _, active := s.compactingStores.Load(id); active {
			return fmt.Errorf("substore %d already has an active compaction", id)
		}
		if exists, err := compactionPathExists(state.subPath + ".old"); err != nil || exists {
			return errors.Join(fmt.Errorf("source %s has an unresolved backup", state.subPath), err)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	for _, suffix := range []string{".compacting", ".retired", ".compaction-keys"} {
		if err := os.RemoveAll(state.subPath + suffix); err != nil {
			return nil, nil, fmt.Errorf("remove stale compaction path %s: %w", state.subPath+suffix, err)
		}
	}
	// Create temporary substore
	state.target, err = s.createStore(state.subPath+".compacting", s.maxBlock, s.filesize)
	if err != nil {
		return nil, nil, fmt.Errorf("create compaction target %s: %w", state.subPath, err)
	}
	if err := os.MkdirAll(state.subPath+".compaction-keys", 0700); err != nil {
		return nil, nil, errors.Join(err, s.abortCompaction(id, state))
	}
	state.keys, err = newDiskKeyIndex(state.subPath + ".compaction-keys")
	if err != nil {
		return nil, nil, errors.Join(err, s.abortCompaction(id, state))
	}
	err = s.runInSubstoreWithErr(id, func(source KvLike) error {
		if err := state.keys.collect(source); err != nil {
			return fmt.Errorf("enumerate source %s: %w", state.subPath, err)
		}
		// Register compact store for dual-writes
		s.compactingStores.Store(id, state)
		return nil
	})
	if err != nil {
		return nil, nil, errors.Join(err, s.abortCompaction(id, state))
	}
	return state, state.keys, nil
}

func (s *EnsembleKv) copyCompactionKey(id int, state *compactionState, key []byte) error {
	return s.runInSubstoreWithErr(id, func(source KvLike) error {
		if err := s.checkCompaction(id, state); err != nil {
			return err
		}
		status, err := state.keys.get(key)
		if err != nil {
			state.failure = err
			return err
		}
		if status != compactionPending {
			return nil
		}
		value, err := source.Get(key)
		if err != nil {
			state.failure = fmt.Errorf("read compaction source %s, key %x: %w", state.subPath, key, err)
			return state.failure
		}
		if value == nil {
			value = []byte{}
		}
		if err := state.target.Put(key, value); err != nil {
			state.failure = fmt.Errorf("copy key %x to %s.compacting: %w", key, state.subPath, err)
			return state.failure
		}
		state.failure = state.keys.set(key, compactionCopied)
		return state.failure
	})
}

func (s *EnsembleKv) checkCompaction(id int, state *compactionState) error {
	active, ok := s.compactingStores.Load(id)
	if !ok || active != state || state.finished {
		return fmt.Errorf("compaction for substore %d is no longer active", id)
	}
	return state.failure
}

func (s *EnsembleKv) finishCompaction(id int, state *compactionState) error {
	return s.runInSubstoreWithErr(id, func(source KvLike) error {
		if err := s.checkCompaction(id, state); err != nil {
			return err
		}
		// No dual writes may reach a closed target after a failed publication releases this lock.
		s.compactingStores.Delete(id)
		keys, err := newDiskKeyIndex(state.subPath + ".compaction-keys")
		if err != nil {
			return err
		}
		// Some backends interpret Put(key, nil) as deletion; compare the actual final live set.
		if err := keys.collect(source); err != nil {
			return errors.Join(fmt.Errorf("validate final source keys in %s: %w", state.subPath, err), keys.close())
		}
		if err := state.keys.close(); err != nil {
			return errors.Join(err, keys.close())
		}
		state.keys = keys
		if err := source.Flush(); err != nil {
			return fmt.Errorf("flush source %s: %w", state.subPath, err)
		}
		if err := state.target.Flush(); err != nil {
			return fmt.Errorf("flush target %s.compacting: %w", state.subPath, err)
		}
		if err := state.target.Close(); err != nil {
			return fmt.Errorf("close target %s.compacting: %w", state.subPath, err)
		}
		state.targetClosed = true
		if err := syncCompactionTree(state.subPath + ".compacting"); err != nil {
			return err
		}
		state.sourceClosed = true
		if err := source.Close(); err != nil {
			s.unavailable.Store(id, err)
			return fmt.Errorf("close source %s: %w", state.subPath, err)
		}
		if err := syncCompactionTree(state.subPath); err != nil {
			return errors.Join(err, s.restoreCompaction(id, state))
		}
		return s.publishCompaction(id, state)
	})
}

func (s *EnsembleKv) publishCompaction(id int, state *compactionState) error {
	if err := writeCompactionMarker(state.subPath); err != nil {
		return errors.Join(err, s.restoreCompaction(id, state))
	}
	// Rename primary to old
	if err := os.Rename(state.subPath, state.subPath+".old"); err != nil {
		return errors.Join(fmt.Errorf("rename source to %s.old: %w", state.subPath, err), s.restoreCompaction(id, state))
	}
	state.sourceMoved = true
	if err := syncCompactionPath(filepath.Dir(state.subPath)); err != nil {
		return errors.Join(err, s.restoreCompaction(id, state))
	}
	// Rename compacting to primary
	if err := os.Rename(state.subPath+".compacting", state.subPath); err != nil {
		return errors.Join(fmt.Errorf("publish target at %s: %w", state.subPath, err), s.restoreCompaction(id, state))
	}
	state.targetMoved = true
	if err := syncCompactionPath(filepath.Dir(state.subPath)); err != nil {
		return errors.Join(err, s.restoreCompaction(id, state))
	}
	// Open new substore
	replacement, err := s.createStore(state.subPath, s.maxBlock, s.filesize)
	if err != nil {
		return errors.Join(fmt.Errorf("open compacted source %s: %w", state.subPath, err), s.restoreCompaction(id, state))
	}
	if err := validateCompactionReplacement(replacement, state); err != nil {
		return s.rejectCompactionReplacement(id, state, replacement, err)
	}
	if err := os.Rename(state.subPath+".old", state.subPath+".retired"); err != nil {
		return s.rejectCompactionReplacement(id, state, replacement, fmt.Errorf("commit compacted source %s: %w", state.subPath, err))
	}
	// A durable .retired name commits the new primary before new writes can be acknowledged.
	s.substores.Store(id, replacement)
	state.finished = true
	s.compactingStores.Delete(id)
	if err := syncCompactionPath(filepath.Dir(state.subPath)); err != nil {
		s.unavailable.Store(id, err)
		return err
	}
	s.flushStore.Store(id, false)
	if err := os.RemoveAll(state.subPath + ".retired"); err != nil {
		return fmt.Errorf("remove committed backup %s.retired: %w", state.subPath, err)
	}
	if err := syncCompactionPath(filepath.Dir(state.subPath)); err != nil {
		return err
	}
	return removeCompactionMarker(state.subPath)
}

func validateCompactionReplacement(replacement KvLike, state *compactionState) error {
	var expected, received int64
	if err := state.keys.each(func(key []byte, status compactionKeyState) error {
		if status != compactionDeleted {
			expected++
		}
		return nil
	}); err != nil {
		return err
	}
	err := forEachLiveKeyAt(replacement, state.subPath+".compaction-keys", func(key []byte) error {
		status, err := state.keys.get(key)
		if err != nil {
			return err
		}
		if status != compactionPending {
			return fmt.Errorf("reopened compacted source %s contains unexpected key %x", state.subPath, key)
		}
		received++
		return nil
	})
	if err != nil {
		return fmt.Errorf("validate reopened compacted source %s: %w", state.subPath, err)
	}
	if received != expected {
		return fmt.Errorf("reopened compacted source %s contains %d keys, expected %d", state.subPath, received, expected)
	}
	return nil
}

func (s *EnsembleKv) rejectCompactionReplacement(id int, state *compactionState, replacement KvLike, cause error) error {
	if err := replacement.Close(); err != nil {
		s.substores.Store(id, replacement)
		s.unavailable.Store(id, errors.Join(cause, err))
		return errors.Join(cause, err)
	}
	return errors.Join(cause, s.restoreCompaction(id, state))
}

func (s *EnsembleKv) restoreCompaction(id int, state *compactionState) (result error) {
	defer func() {
		if result != nil {
			s.unavailable.Store(id, result)
		}
	}()
	if state.targetMoved {
		if err := os.Rename(state.subPath, state.subPath+".compacting"); err != nil {
			return fmt.Errorf("move failed target away from %s before restoring backup: %w", state.subPath, err)
		}
		state.targetMoved = false
	}
	if state.sourceMoved {
		if err := os.Rename(state.subPath+".old", state.subPath); err != nil {
			return fmt.Errorf("restore backup %s.old: %w", state.subPath, err)
		}
		state.sourceMoved = false
	}
	if err := syncCompactionPath(filepath.Dir(state.subPath)); err != nil {
		return err
	}
	if exists, err := compactionPathExists(state.subPath); err != nil || !exists {
		return errors.Join(fmt.Errorf("cannot reopen missing original source %s", state.subPath), err)
	}
	original, err := s.createStore(state.subPath, s.maxBlock, s.filesize)
	if err != nil {
		return fmt.Errorf("reopen original source %s: %w", state.subPath, err)
	}
	s.substores.Store(id, original)
	state.sourceClosed = false
	return nil
}

func (s *EnsembleKv) abortCompaction(id int, state *compactionState) (result error) {
	lock, ok := s.substoreLocks.Load(id)
	if !ok {
		return fmt.Errorf("lock for substore %d not found during compaction cleanup", id)
	}
	lock.Lock()
	defer lock.Unlock()
	defer func() {
		result = errors.Join(result, state.keys.close())
		if state.keys == nil || state.keys.db == nil {
			result = errors.Join(result, os.RemoveAll(state.subPath+".compaction-keys"))
		}
	}()
	if state.finished {
		return nil
	}
	state.finished = true
	s.compactingStores.Delete(id)
	if state.sourceClosed {
		if _, failed := s.unavailable.Load(id); !failed {
			s.unavailable.Store(id, fmt.Errorf("compaction of %s ended before its closed source was restored", state.subPath))
		}
	}
	if !state.targetClosed {
		if err := state.target.Close(); err != nil {
			return fmt.Errorf("close aborted compaction target %s: %w", state.subPath, err)
		}
		state.targetClosed = true
	}
	if !state.targetMoved {
		if err := os.RemoveAll(state.subPath + ".compacting"); err != nil {
			return fmt.Errorf("remove aborted compaction target %s: %w", state.subPath, err)
		}
	}
	if !state.sourceClosed {
		return removeCompactionMarker(state.subPath)
	}
	return nil
}

func writeCompactionMarker(primary string) error {
	path := primary + ".compaction"
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("create compaction marker %s: %w", path, err)
	}
	_, writeErr := file.WriteString(compactionMarker)
	if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		return fmt.Errorf("persist compaction marker %s: %w", path, err)
	}
	return syncCompactionPath(filepath.Dir(primary))
}

func readCompactionMarker(primary string) (bool, error) {
	path := primary + ".compaction"
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Size() != int64(len(compactionMarker)) {
		return false, fmt.Errorf("invalid compaction marker %s: expected a regular %d-byte version marker", path, len(compactionMarker))
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	if string(data) != compactionMarker {
		return false, fmt.Errorf("invalid compaction marker %s: received %q, expected %q", path, data, compactionMarker)
	}
	return true, nil
}

func removeCompactionMarker(primary string) error {
	if err := os.Remove(primary + ".compaction"); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove compaction marker for %s: %w", primary, err)
	}
	return syncCompactionPath(filepath.Dir(primary))
}

func compactionPathExists(path string) (bool, error) {
	_, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}

func syncCompactionPath(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open compaction path %s for sync: %w", path, err)
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return fmt.Errorf("sync compaction path %s: %w", path, err)
	}
	return nil
}

func syncCompactionTree(path string) error {
	return filepath.WalkDir(path, func(current string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && !entry.Type().IsRegular() {
			return fmt.Errorf("compaction path %s is neither a regular file nor a directory", current)
		}
		return syncCompactionPath(current)
	})
}

func (s *EnsembleKv) recoverCompaction(primary string) error {
	marked, err := readCompactionMarker(primary)
	if err != nil {
		return err
	}
	old, err := compactionPathExists(primary + ".old")
	if err != nil {
		return err
	}
	retired, err := compactionPathExists(primary + ".retired")
	if err != nil {
		return err
	}
	present, err := compactionPathExists(primary)
	if err != nil {
		return err
	}
	if retired && (old || !present) {
		return fmt.Errorf("invalid compaction recovery at %s: retired backup exists, original backup=%t, primary=%t", primary, old, present)
	}
	if old && present && !marked {
		if s.emptyCompactionBootstrap(primary) {
			if err := os.Remove(primary + ".old"); err != nil {
				return err
			}
			old = false
		} else {
			return fmt.Errorf("ambiguous legacy compaction at %s: primary and .old exist without a version marker; preserving both", primary)
		}
	}
	if marked && !present && !old {
		return fmt.Errorf("compaction marker for %s has no primary or original backup", primary)
	}
	if retired {
		if err := s.validateCompactionRecovery(primary, primary+".compaction-keys"); err != nil {
			return err
		}
	}
	if marked && present && !old && !retired {
		if err := s.validateCompactionRecovery(primary, primary+".compaction-keys"); err != nil {
			return err
		}
	}
	if old {
		if err := s.validateCompactionRecovery(primary+".old", primary+".compaction-keys"); err != nil {
			return err
		}
		if err := os.RemoveAll(primary); err != nil {
			return fmt.Errorf("remove uncommitted primary %s: %w", primary, err)
		}
		if err := os.Rename(primary+".old", primary); err != nil {
			return fmt.Errorf("restore authoritative backup %s.old: %w", primary, err)
		}
		present = true
		if err := syncCompactionPath(filepath.Dir(primary)); err != nil {
			return err
		}
	}
	incomplete, err := compactionPathExists(primary + ".compacting")
	if err != nil {
		return err
	}
	if incomplete && !present {
		return fmt.Errorf("incomplete compaction at %s has no primary or original backup", primary)
	}
	for _, suffix := range []string{".compacting", ".retired", ".compaction-keys"} {
		if err := os.RemoveAll(primary + suffix); err != nil {
			return fmt.Errorf("clean recovered compaction path %s: %w", primary+suffix, err)
		}
	}
	if err := syncCompactionPath(filepath.Dir(primary)); err != nil {
		return err
	}
	return removeCompactionMarker(primary)
}

func (s *EnsembleKv) emptyCompactionBootstrap(primary string) bool {
	if _, err := os.Stat(filepath.Join(s.directory, "metadata.json")); !os.IsNotExist(err) {
		return false
	}
	for _, suffix := range []string{"", ".old", ".compacting"} {
		entries, err := os.ReadDir(primary + suffix)
		if suffix == ".compacting" && os.IsNotExist(err) {
			continue
		}
		if err != nil || len(entries) != 0 {
			return false
		}
	}
	return true
}

func (s *EnsembleKv) validateCompactionRecovery(path, scratchParent string) error {
	inventory, err := inspectCompactionRecovery(path)
	if err != nil {
		return err
	}
	store, err := s.createStore(path, s.maxBlock, s.filesize)
	if err != nil {
		return fmt.Errorf("open authoritative compaction recovery path %s: %w", path, err)
	}
	readErr := validateCompactionRecoveryStore(path, store, inventory)
	if readErr == nil {
		if readErr = os.MkdirAll(scratchParent, 0700); readErr == nil {
			readErr = streamKeyRecordsAt(store, scratchParent, func([]byte, bool) error { return nil })
		}
	}
	if err := errors.Join(readErr, store.Close()); err != nil {
		return fmt.Errorf("validate authoritative compaction recovery path %s: %w", path, err)
	}
	return syncCompactionTree(path)
}
