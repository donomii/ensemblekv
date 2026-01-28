package ensemblekv

import (
	"flag"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	flag.Parse()
	_ = flag.Set("test.failfast", "true")
	os.Exit(m.Run())
}
