package ensemblekv

import (
	"reflect"
	"strings"
	"testing"
)

func requireManagerContents(t *testing.T, store KvLike, expected map[string]string) {
	t.Helper()
	keys := make(map[string]bool)
	for _, key := range store.Keys() {
		if keys[string(key)] {
			t.Fatalf("Keys returned duplicate key %q", key)
		}
		keys[string(key)] = true
	}
	if len(keys) != len(expected) {
		t.Fatalf("Keys returned %d keys, expected %d: %v", len(keys), len(expected), keys)
	}
	for key, expectedValue := range expected {
		value, err := store.Get([]byte(key))
		if err != nil || string(value) != expectedValue || !store.Exists([]byte(key)) || !keys[key] {
			t.Fatalf("key %q: Get=%q, error=%v, Exists=%v, Keys contains key=%v; expected %q", key, value, err, store.Exists([]byte(key)), keys[key], expectedValue)
		}
	}
	values := make(map[string]string)
	_, err := store.MapFunc(func(key, value []byte) error {
		if _, exists := values[string(key)]; exists {
			t.Fatalf("MapFunc returned duplicate key %q", key)
		}
		values[string(key)] = string(value)
		return nil
	})
	if err != nil || !reflect.DeepEqual(values, expected) {
		t.Fatalf("MapFunc=%v, error=%v; expected %v", values, err, expected)
	}
	values = make(map[string]string)
	_, err = store.MapPrefixFunc([]byte("key-"), func(key, value []byte) error {
		if _, exists := values[string(key)]; exists {
			t.Fatalf("MapPrefixFunc returned duplicate key %q", key)
		}
		values[string(key)] = string(value)
		return nil
	})
	prefixExpected := make(map[string]string)
	for key, value := range expected {
		if strings.HasPrefix(key, "key-") {
			prefixExpected[key] = value
		}
	}
	if err != nil || !reflect.DeepEqual(values, prefixExpected) {
		t.Fatalf("MapPrefixFunc=%v, error=%v; expected %v", values, err, prefixExpected)
	}
}
