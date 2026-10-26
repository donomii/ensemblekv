package ensemblekv

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestJsonBinaryKeysSurviveReopen(t *testing.T) {
	directory := t.TempDir()
	store, err := JsonKVCreator(directory, 4096, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	keys := [][]byte{{0xff}, {0xfe}, []byte("\ufffd"), []byte("normal"), {}, {0}}
	for _, key := range keys {
		if err := store.Put(key, append([]byte("value:"), key...)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = JsonKVCreator(directory, 4096, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, key := range keys {
		value, err := store.Get(key)
		want := append([]byte("value:"), key...)
		if err != nil || !bytes.Equal(value, want) {
			t.Fatalf("key=%x reopened value=%x err=%v want=%x", key, value, err, want)
		}
	}
}

func TestJsonLegacyStoreRemainsReadable(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "store.json"), []byte(`{"key":"dmFsdWU="}`), 0600); err != nil {
		t.Fatal(err)
	}
	store, err := JsonKVCreator(directory, 4096, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	value, err := store.Get([]byte("key"))
	if err != nil || string(value) != "value" {
		t.Fatalf("legacy value=%q err=%v", value, err)
	}
}

func TestJsonSavePreservesPrivatePermissions(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "store.json")
	if err := os.WriteFile(path, []byte(`{"key":"dmFsdWU="}`), 0600); err != nil {
		t.Fatal(err)
	}
	store, err := JsonKVCreator(directory, 4096, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("key"), []byte("updated")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("saved store permissions=%o, want 600", info.Mode().Perm())
	}
}
