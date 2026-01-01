package ensemblekv

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func fatalf(tb testing.TB, format string, args ...interface{}) {
	tb.Helper()
	args = append([]interface{}{tb.Name()}, args...)
	tb.Fatalf("test=%s "+format, args...)
}

func runFailfast(t *testing.T, name string, f func(t *testing.T)) {
	fmt.Printf("%v: Running test %v\n", time.Now(), name)
	t.Helper()
	if ok := t.Run(name, f); !ok {
		t.FailNow()
	}
}

type opLog struct {
	entries []string
}

func newOpLog(size int) *opLog {
	if size < 0 {
		size = 0
	}
	return &opLog{entries: make([]string, 0, size)}
}

func (l *opLog) Addf(format string, args ...interface{}) {
	if l == nil {
		return
	}
	l.entries = append(l.entries, fmt.Sprintf(format, args...))
}

func (l *opLog) String() string {
	if l == nil || len(l.entries) == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "ops log (%d entries):\n", len(l.entries))
	for _, entry := range l.entries {
		if entry == "" {
			continue
		}
		b.WriteString(entry)
		b.WriteByte('\n')
	}
	return strings.TrimRight(b.String(), "\n")
}
