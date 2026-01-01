package ensemblekv

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func assertSFPut(t *testing.T, store *SingleFileLSM, key, value []byte, ctx string) {
	t.Helper()
	if err := store.Put(key, value); err != nil {
		fatalf(t, "action=PutError key=%s value=%s ctx=%s err=%v", trimTo40(key), trimTo40(value), ctx, err)
	}
}

func assertSFGet(t *testing.T, store *SingleFileLSM, key []byte, ctx string) []byte {
	t.Helper()
	retrieved, err := store.Get(key)
	if err != nil {
		fatalf(t, "action=GetError key=%s ctx=%s err=%v", trimTo40(key), ctx, err)
	}
	return retrieved
}

func assertSFDelete(t *testing.T, store *SingleFileLSM, key []byte, ctx string) {
	t.Helper()
	if err := store.Delete(key); err != nil {
		fatalf(t, "action=DeleteError key=%s ctx=%s err=%v", trimTo40(key), ctx, err)
	}
}

func assertSFExists(t *testing.T, store *SingleFileLSM, key []byte, expected bool, ctx string) {
	t.Helper()
	if exists := store.Exists(key); exists != expected {
		fatalf(t, "action=ExistsMismatch key=%s ctx=%s expected=%v got=%v", trimTo40(key), ctx, expected, exists)
	}
}

func assertSFGetFails(t *testing.T, store *SingleFileLSM, key []byte, ctx string) {
	t.Helper()
	if _, err := store.Get(key); err == nil {
		fatalf(t, "action=GetUnexpectedSuccess key=%s ctx=%s expectedErr=true gotErr=false", trimTo40(key), ctx)
	}
}

func assertSFFlush(t *testing.T, store *SingleFileLSM, ctx string) {
	t.Helper()
	if err := store.Flush(); err != nil {
		fatalf(t, "action=FlushError ctx=%s err=%v", ctx, err)
	}
}

func TestCacheConsistencyPutGetAlternating(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewSingleFileLSM(tmpDir+"/cache_test.lsm", 4096, 1024*1024)
	if err != nil {
		fatalf(t, "action=CreateError path=%s err=%v", tmpDir+"/cache_test.lsm", err)
	}
	defer store.Close()

	key := []byte("test-key")
	for i := 0; i < 100; i++ {
		value := []byte(fmt.Sprintf("value-%d", i))

		assertSFPut(t, store, key, value, fmt.Sprintf("at iteration %d", i))

		retrieved := assertSFGet(t, store, key, fmt.Sprintf("at iteration %d", i))
		if string(retrieved) != string(value) {
			fatalf(t, "action=GetMismatch key=%s iteration=%d expected=%s got=%s", trimTo40(key), i, trimTo40(value), trimTo40(retrieved))
		}

		if !store.Exists(key) {
			fatalf(t, "action=ExistsMismatch key=%s iteration=%d expected=true got=false", trimTo40(key), i)
		}
	}

	t.Log("SUCCESS: Put-Get alternating cache consistency verified")
}

func TestCacheConsistencyPutDeleteAlternating(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewSingleFileLSM(tmpDir+"/cache_test.lsm", 4096, 1024*1024)
	if err != nil {
		fatalf(t, "action=CreateError path=%s err=%v", tmpDir+"/cache_test.lsm", err)
	}
	defer store.Close()

	key := []byte("test-key")
	for i := 0; i < 100; i++ {
		value := []byte(fmt.Sprintf("value-%d", i))

		assertSFPut(t, store, key, value, fmt.Sprintf("at iteration %d", i))

		if !store.Exists(key) {
			fatalf(t, "action=ExistsMismatch key=%s iteration=%d expected=true got=false", trimTo40(key), i)
		}

		retrieved := assertSFGet(t, store, key, fmt.Sprintf("at iteration %d", i))
		if string(retrieved) != string(value) {
			fatalf(t, "action=GetMismatch key=%s iteration=%d expected=%s got=%s", trimTo40(key), i, trimTo40(value), trimTo40(retrieved))
		}

		assertSFDelete(t, store, key, fmt.Sprintf("at iteration %d", i))

		if store.Exists(key) {
			fatalf(t, "action=ExistsMismatch key=%s iteration=%d expected=false got=true", trimTo40(key), i)
		}

		assertSFGetFails(t, store, key, fmt.Sprintf("at iteration %d after Delete", i))
	}

	t.Log("SUCCESS: Put-Delete alternating cache consistency verified")
}

func TestCacheConsistencyMultipleKeys(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewSingleFileLSM(tmpDir+"/cache_test.lsm", 4096, 1024*1024)
	if err != nil {
		fatalf(t, "action=CreateError path=%s err=%v", tmpDir+"/cache_test.lsm", err)
	}
	defer store.Close()

	numKeys := 10
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
	}

	for iteration := 0; iteration < 50; iteration++ {
		for i, key := range keys {
			value := []byte(fmt.Sprintf("value-%d-%d", i, iteration))
			assertSFPut(t, store, key, value, fmt.Sprintf("key %d at iteration %d", i, iteration))
		}

		for i, key := range keys {
			expectedValue := []byte(fmt.Sprintf("value-%d-%d", i, iteration))
			retrieved := assertSFGet(t, store, key, fmt.Sprintf("key %d at iteration %d", i, iteration))
			if string(retrieved) != string(expectedValue) {
				fatalf(t, "action=GetMismatch key=%s iteration=%d expected=%s got=%s",
					trimTo40(key), iteration, trimTo40(expectedValue), trimTo40(retrieved))
			}
		}

		for i := 0; i < numKeys/2; i++ {
			assertSFDelete(t, store, keys[i], fmt.Sprintf("key %d at iteration %d", i, iteration))
		}

		for i := 0; i < numKeys/2; i++ {
			if store.Exists(keys[i]) {
				fatalf(t, "action=ExistsMismatch key=%s iteration=%d expected=false got=true", trimTo40(keys[i]), iteration)
			}
		}

		for i := numKeys / 2; i < numKeys; i++ {
			if !store.Exists(keys[i]) {
				fatalf(t, "action=ExistsMismatch key=%s iteration=%d expected=true got=false", trimTo40(keys[i]), iteration)
			}
		}
	}

	t.Log("SUCCESS: Multiple keys cache consistency verified")
}

func TestCacheConsistencyRandomOperations(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewSingleFileLSM(tmpDir+"/cache_test.lsm", 4096, 1024*1024)
	if err != nil {
		fatalf(t, "action=CreateError path=%s err=%v", tmpDir+"/cache_test.lsm", err)
	}
	defer store.Close()

	seed := time.Now().UnixNano()
	rand.Seed(seed)
	opLog := newOpLog(50)
	failf := func(format string, args ...interface{}) {
		t.Helper()
		args = append([]interface{}{seed}, args...)
		args = append(args, opLog.String())
		fatalf(t, "seed=%d "+format+"\n%s", args...)
	}

	numKeys := 20
	keys := make([][]byte, numKeys)
	expectedState := make(map[string]string)

	for i := 0; i < numKeys; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
	}

	for i := 0; i < 1000; i++ {
		keyIdx := rand.Intn(numKeys)
		key := keys[keyIdx]
		keyStr := string(key)

		switch rand.Intn(4) {
		case 0: // Put
			value := []byte(fmt.Sprintf("value-%d", i))
			opLog.Addf("op=%d action=Put key=%s value=%s", i, trimTo40(key), trimTo40(value))
			if err := store.Put(key, value); err != nil {
				failf("action=PutError key=%s operation=%d err=%v", trimTo40(key), i, err)
			}
			expectedState[keyStr] = string(value)

		case 1: // Get
			expectedVal, exists := expectedState[keyStr]
			expectedLog := "<missing>"
			if exists {
				expectedLog = trimTo40([]byte(expectedVal))
			}
			opLog.Addf("op=%d action=Get key=%s expectedExists=%t expected=%s", i, trimTo40(key), exists, expectedLog)
			retrieved, err := store.Get(key)

			if exists {
				if err != nil {
					failf("action=GetError key=%s operation=%d expectedErr=false gotErr=true err=%v", trimTo40(key), i, err)
				}
				if string(retrieved) != expectedVal {
					failf("action=GetMismatch key=%s operation=%d expected=%s got=%s",
						trimTo40(key), i, trimTo40([]byte(expectedVal)), trimTo40(retrieved))
				}
			} else {
				if err == nil {
					failf("action=GetUnexpectedSuccess key=%s operation=%d expectedErr=true gotErr=false", trimTo40(key), i)
				}
			}

		case 2: // Delete
			_, exists := expectedState[keyStr]
			opLog.Addf("op=%d action=Delete key=%s expectedExists=%t", i, trimTo40(key), exists)
			if err := store.Delete(key); err != nil {
				failf("action=DeleteError key=%s operation=%d err=%v", trimTo40(key), i, err)
			}
			delete(expectedState, keyStr)

		case 3: // Exists
			exists := store.Exists(key)
			_, expected := expectedState[keyStr]
			opLog.Addf("op=%d action=Exists key=%s expected=%t got=%t", i, trimTo40(key), expected, exists)

			if exists != expected {
				failf("action=ExistsMismatch key=%s operation=%d expected=%v got=%v",
					trimTo40(key), i, expected, exists)
			}
		}
	}

	t.Log("SUCCESS: Random operations cache consistency verified")
}

func TestCacheConsistencyOverwrite(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewSingleFileLSM(tmpDir+"/cache_test.lsm", 4096, 1024*1024)
	if err != nil {
		fatalf(t, "action=CreateError path=%s err=%v", tmpDir+"/cache_test.lsm", err)
	}
	defer store.Close()

	key := []byte("test-key")
	for i := 0; i < 200; i++ {
		value := []byte(fmt.Sprintf("value-%d", i))

		assertSFPut(t, store, key, value, fmt.Sprintf("at iteration %d", i))

		retrieved := assertSFGet(t, store, key, fmt.Sprintf("at iteration %d", i))
		if string(retrieved) != string(value) {
			fatalf(t, "action=GetMismatch key=%s iteration=%d expected=%s got=%s",
				trimTo40(key), i, trimTo40(value), trimTo40(retrieved))
		}

		if !store.Exists(key) {
			fatalf(t, "action=ExistsMismatch key=%s iteration=%d expected=true got=false", trimTo40(key), i)
		}
	}

	t.Log("SUCCESS: Overwrite cache consistency verified")
}

func TestCacheConsistencyWithFlush(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewSingleFileLSM(tmpDir+"/cache_test.lsm", 4096, 1024*1024)
	if err != nil {
		fatalf(t, "action=CreateError path=%s err=%v", tmpDir+"/cache_test.lsm", err)
	}
	defer store.Close()

	for round := 0; round < 10; round++ {
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key-%d-%d", round, i))
			value := make([]byte, 100*1024)
			for j := range value {
				value[j] = byte((round + i + j) % 256)
			}
			assertSFPut(t, store, key, value, fmt.Sprintf("round %d key %d", round, i))
		}

		assertSFFlush(t, store, fmt.Sprintf("round %d", round))

		testKey := []byte(fmt.Sprintf("key-%d-50", round))
		testValue := make([]byte, 100*1024)
		for j := range testValue {
			testValue[j] = byte((round + 50 + j) % 256)
		}

		retrieved := assertSFGet(t, store, testKey, fmt.Sprintf("round %d after flush", round))
		if string(retrieved) != string(testValue) {
			fatalf(t, "action=GetMismatch key=%s round=%d expected=%s got=%s", trimTo40(testKey), round, trimTo40(testValue), trimTo40(retrieved))
		}
	}

	t.Log("SUCCESS: Cache consistency with flush verified")
}

func TestCacheConsistencyPutGetDeleteExists(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewSingleFileLSM(tmpDir+"/cache_test.lsm", 4096, 1024*1024)
	if err != nil {
		fatalf(t, "action=CreateError path=%s err=%v", tmpDir+"/cache_test.lsm", err)
	}
	defer store.Close()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))

		assertSFPut(t, store, key, value, fmt.Sprintf("iteration %d", i))

		if !store.Exists(key) {
			fatalf(t, "action=ExistsMismatch key=%s iteration=%d expected=true got=false", trimTo40(key), i)
		}

		retrieved := assertSFGet(t, store, key, fmt.Sprintf("iteration %d", i))
		if string(retrieved) != string(value) {
			fatalf(t, "action=GetMismatch key=%s iteration=%d expected=%s got=%s", trimTo40(key), i, trimTo40(value), trimTo40(retrieved))
		}

		assertSFDelete(t, store, key, fmt.Sprintf("iteration %d", i))

		if store.Exists(key) {
			fatalf(t, "action=ExistsMismatch key=%s iteration=%d expected=false got=true", trimTo40(key), i)
		}

		assertSFGetFails(t, store, key, fmt.Sprintf("iteration %d after Delete", i))
	}

	t.Log("SUCCESS: Put-Get-Delete-Exists sequence cache consistency verified")
}

func TestCacheConsistencyInterleavedOperations(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewSingleFileLSM(tmpDir+"/cache_test.lsm", 4096, 1024*1024)
	if err != nil {
		fatalf(t, "action=CreateError path=%s err=%v", tmpDir+"/cache_test.lsm", err)
	}
	defer store.Close()

	key1 := []byte("key-1")
	key2 := []byte("key-2")
	key3 := []byte("key-3")

	for i := 0; i < 100; i++ {
		val1 := []byte(fmt.Sprintf("value-1-%d", i))
		val2 := []byte(fmt.Sprintf("value-2-%d", i))
		val3 := []byte(fmt.Sprintf("value-3-%d", i))

		assertSFPut(t, store, key1, val1, fmt.Sprintf("iteration %d key1", i))
		assertSFPut(t, store, key2, val2, fmt.Sprintf("iteration %d key2", i))

		if !store.Exists(key1) {
			fatalf(t, "action=ExistsMismatch key=%s iteration=%d expected=true got=false", trimTo40(key1), i)
		}

		assertSFPut(t, store, key3, val3, fmt.Sprintf("iteration %d key3", i))

		v1 := assertSFGet(t, store, key1, fmt.Sprintf("iteration %d", i))
		v2 := assertSFGet(t, store, key2, fmt.Sprintf("iteration %d", i))
		v3 := assertSFGet(t, store, key3, fmt.Sprintf("iteration %d", i))

		if string(v1) != string(val1) {
			fatalf(t, "action=GetMismatch key=%s iteration=%d expected=%s got=%s", trimTo40(key1), i, trimTo40(val1), trimTo40(v1))
		}
		if string(v2) != string(val2) {
			fatalf(t, "action=GetMismatch key=%s iteration=%d expected=%s got=%s", trimTo40(key2), i, trimTo40(val2), trimTo40(v2))
		}
		if string(v3) != string(val3) {
			fatalf(t, "action=GetMismatch key=%s iteration=%d expected=%s got=%s", trimTo40(key3), i, trimTo40(val3), trimTo40(v3))
		}

		assertSFDelete(t, store, key2, fmt.Sprintf("iteration %d key2", i))

		if !store.Exists(key1) {
			fatalf(t, "action=ExistsMismatch key=%s iteration=%d expected=true got=false", trimTo40(key1), i)
		}
		if store.Exists(key2) {
			fatalf(t, "action=ExistsMismatch key=%s iteration=%d expected=false got=true", trimTo40(key2), i)
		}
		if !store.Exists(key3) {
			fatalf(t, "action=ExistsMismatch key=%s iteration=%d expected=true got=false", trimTo40(key3), i)
		}

		val2New := []byte(fmt.Sprintf("value-2-new-%d", i))
		assertSFPut(t, store, key2, val2New, fmt.Sprintf("iteration %d key2 (new)", i))

		v1 = assertSFGet(t, store, key1, fmt.Sprintf("iteration %d (final)", i))
		v2 = assertSFGet(t, store, key2, fmt.Sprintf("iteration %d (final)", i))
		v3 = assertSFGet(t, store, key3, fmt.Sprintf("iteration %d (final)", i))

		if string(v1) != string(val1) {
			fatalf(t, "action=GetMismatch key=%s iteration=%d expected=%s got=%s", trimTo40(key1), i, trimTo40(val1), trimTo40(v1))
		}
		if string(v2) != string(val2New) {
			fatalf(t, "action=GetMismatch key=%s iteration=%d expected=%s got=%s", trimTo40(key2), i, trimTo40(val2New), trimTo40(v2))
		}
		if string(v3) != string(val3) {
			fatalf(t, "action=GetMismatch key=%s iteration=%d expected=%s got=%s", trimTo40(key3), i, trimTo40(val3), trimTo40(v3))
		}

		assertSFDelete(t, store, key1, fmt.Sprintf("iteration %d key1 cleanup", i))
		assertSFDelete(t, store, key2, fmt.Sprintf("iteration %d key2 cleanup", i))
		assertSFDelete(t, store, key3, fmt.Sprintf("iteration %d key3 cleanup", i))
	}

	t.Log("SUCCESS: Interleaved operations cache consistency verified")
}
