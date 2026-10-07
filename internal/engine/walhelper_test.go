package engine

import (
	"path/filepath"
	"testing"

	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/store"
)

func newWALAt(t *testing.T, dir string) *store.WAL {
	t.Helper()
	w, err := store.Open(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatalf("open wal at %s: %v", dir, err)
	}
	return w
}
