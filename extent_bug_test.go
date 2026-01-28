package ensemblekv

import (
	"fmt"
	"testing"
)

func TestMinimumExample(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := ExtentCreator(tmpDir, 4096, 10*1024*1024)
	if err != nil {
		fatalf(t, "action=Create store=ExtentKV path=%s err=%v", tmpDir, err)
	}
	defer store.Close()

	key1 := []byte("cache-interleaved-1")
	key2 := []byte("cache-interleaved-2")

	val1 := []byte("value-1-0")
	val2 := []byte("value-2-0")

	fmt.Println("Step 1: Put key1")
	if err := store.Put(key1, val1); err != nil {
		fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(key1), trimTo40(val1), err)
	}

	fmt.Println("Step 2: Put key2")
	if err := store.Put(key2, val2); err != nil {
		fatalf(t, "action=Put key=%s value=%s err=%v", trimTo40(key2), trimTo40(val2), err)
	}

	fmt.Println("Step 3: Check if key1 exists BEFORE delete")
	if store.Exists(key1) {
		fmt.Println("  ✓ key1 exists")
	} else {
		fatalf(t, "action=Exists key=%s expected=true got=false", trimTo40(key1))
	}

	fmt.Println("Step 4: Delete key2")
	if err := store.Delete(key2); err != nil {
		fatalf(t, "action=Delete key=%s err=%v", trimTo40(key2), err)
	}

	fmt.Println("Step 5: Check if key1 exists AFTER deleting key2")
	if store.Exists(key1) {
		fmt.Println("  ✓ key1 exists")
	} else {
		fatalf(t, "action=Exists key=%s expected=true got=false", trimTo40(key1))
	}
}
