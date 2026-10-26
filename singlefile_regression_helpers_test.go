package ensemblekv

import "testing"

func openSingleFileRegressionStore(t *testing.T, path string) *SingleFileLSM {
	t.Helper()
	store, err := NewSingleFileLSM(path, 4096, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close %s: %v", path, err)
		}
	})
	return store
}
