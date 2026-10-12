package ensemblekv

func logTreeSplit(directory string, limit int, key, value []byte) {
	debugf("TreeLSM splitting %s: multiple live keys exceed %d logical bytes after writing key %x with %d value bytes\n", directory, limit, key, len(value))
}
