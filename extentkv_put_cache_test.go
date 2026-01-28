package ensemblekv

import (
	"fmt"
	"testing"
)

func TestExtentPutCachesGrowAndDataIntact(t *testing.T) {
	prevCaching := EnableIndexCaching
	EnableIndexCaching = true
	defer func() { EnableIndexCaching = prevCaching }()

	dir := t.TempDir()
	store, err := NewExtentKeyValueStore(dir, 4096, 10*1024*1024)
	if err != nil {
		fatalf(t, "action=Create store=ExtentKV path=%s err=%v", dir, err)
	}
	defer store.Close()

	if err := store.Put([]byte("seed-key"), []byte("seed-value")); err != nil {
		fatalf(t, "action=Put key=%s err=%v", "seed-key", err)
	}

	if err := store.loadKeyAndOffsetCache(); err != nil {
		fatalf(t, "action=LoadKeyCache err=%v", err)
	}

	prevExists := store.existsCache.Len()
	prevOffsets := store.valueOffsetCache.Len()

	// Put should flush caches to nil so we can verify they reload cleanly.
	key := "cache-put-key"
	value := []byte("cache-put-value")
	if err := store.Put([]byte(key), value); err != nil {
		fatalf(t, "action=Put key=%s value=%s err=%v", key, value, err)
	}

	if state, ok := store.existsCache.Load(key); !ok || !state {
		fatalf(t, "action=ExistsCacheMissing key=%s ok=%v state=%v", key, ok, state)
	}
	if store.existsCache.Len() <= prevExists {
		fatalf(t, "action=ExistsCacheNotGrown prev=%d now=%d", prevExists, store.existsCache.Len())
	}

	offsets, ok := store.valueOffsetCache.Load(key)
	if !ok {
		fatalf(t, "action=ValueOffsetCacheMissing key=%s", key)
	}
	if (offsets[1] - offsets[0]) != int64(len(value)) {
		fatalf(t, "action=OffsetSizeMismatch key=%s got=%d want=%d", key, offsets[1]-offsets[0], len(value))
	}
	if store.valueOffsetCache.Len() <= prevOffsets {
		fatalf(t, "action=OffsetCacheNotGrown prev=%d now=%d", prevOffsets, store.valueOffsetCache.Len())
	}

	got, err := store.Get([]byte(key))
	if err != nil {
		fatalf(t, "action=Get key=%s err=%v", key, err)
	}
	if string(got) != string(value) {
		fatalf(t, "action=GetMismatch key=%s want=%s got=%s", key, value, got)
	}
}

func TestExtentPutCachesAcrossReopens(t *testing.T) {
	prevCaching := EnableIndexCaching
	EnableIndexCaching = true
	defer func() { EnableIndexCaching = prevCaching }()

	dir := t.TempDir()
	expected := make(map[string][]byte)

	openStore := func() *ExtentKeyValStore {
		s, err := NewExtentKeyValueStore(dir, 4096, 10*1024*1024)
		if err != nil {
			fatalf(t, "action=Create store=ExtentKV path=%s err=%v", dir, err)
		}
		return s
	}

	store := openStore()
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("init-key-%d", i)
		val := []byte(fmt.Sprintf("init-val-%d", i))
		if err := store.Put([]byte(key), val); err != nil {
			fatalf(t, "action=Put key=%s value=%s err=%v", key, val, err)
		}
		expected[key] = val
	}

	if err := store.loadKeyAndOffsetCache(); err != nil {
		fatalf(t, "action=LoadKeyCache err=%v", err)
	}

	if err := store.Close(); err != nil {
		fatalf(t, "action=Close err=%v", err)
	}

	for cycle := 0; cycle < 3; cycle++ {
		store = openStore()

		if err := store.loadKeyAndOffsetCache(); err != nil {
			fatalf(t, "action=LoadKeyCache cycle=%d err=%v", cycle, err)
		}

		for k, v := range expected {
			data, err := store.Get([]byte(k))
			if err != nil {
				fatalf(t, "action=Get cycle=%d key=%s err=%v", cycle, k, err)
			}
			if string(data) != string(v) {
				fatalf(t, "action=GetMismatch cycle=%d key=%s want=%s got=%s", cycle, k, v, data)
			}
			if state, ok := store.existsCache.Load(k); !ok || !state {
				fatalf(t, "action=ExistsCacheMissing cycle=%d key=%s ok=%v state=%v", cycle, k, ok, state)
			}
		}

		newKey := fmt.Sprintf("cycle-%d-key", cycle)
		newVal := []byte(fmt.Sprintf("cycle-%d-val", cycle))
		if err := store.Put([]byte(newKey), newVal); err != nil {
			fatalf(t, "action=Put cycle=%d key=%s value=%s err=%v", cycle, newKey, newVal, err)
		}
		expected[newKey] = newVal

		if err := store.Close(); err != nil {
			fatalf(t, "action=Close cycle=%d err=%v", cycle, err)
		}
	}

	store = openStore()
	defer store.Close()

	for k, v := range expected {
		data, err := store.Get([]byte(k))
		if err != nil {
			fatalf(t, "action=Get final key=%s err=%v", k, err)
		}
		if string(data) != string(v) {
			fatalf(t, "action=GetMismatch final key=%s want=%s got=%s", k, v, data)
		}
	}
}
