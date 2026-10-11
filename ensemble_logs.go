package ensemblekv

import "fmt"

func logEnsembleFlushError(id int, err error) {
	fmt.Printf("Ensemble: flushing substore %d failed: %v\n", id, err)
}
