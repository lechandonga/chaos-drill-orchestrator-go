// Package store is a tiny JSON-lines write-ahead log. Every engine state
// transition is appended before the side effect is considered durable, so a
// process restart can reconstruct every experiment and detect dangling
// injected faults. No external database is involved.
package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/experiment"
)

// WAL is an append-only log of full experiment snapshots. Full snapshots
// (instead of deltas) make recovery a trivial "last write wins per ID"
// replay, which is easy to reason about after a crash mid-transition.
type WAL struct {
	path string
	f    *os.File
	w    *bufio.Writer
}

func Open(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}
	path := filepath.Join(dir, "experiments.wal")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &WAL{path: path, f: f, w: bufio.NewWriter(f)}, nil
}

// Append writes one snapshot and flushes it to disk.
func (l *WAL) Append(run *experiment.Run) error {
	data, err := json.Marshal(run)
	if err != nil {
		return fmt.Errorf("marshal run: %w", err)
	}
	if _, err := l.w.Write(data); err != nil {
		return err
	}
	if err := l.w.WriteByte('\n'); err != nil {
		return err
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Sync()
}

func (l *WAL) Close() error { return l.f.Close() }

// Replay returns the last known snapshot of every experiment in the log.
// A torn trailing line (process killed while writing) is ignored rather than
// aborting recovery of the intact prefix.
func (l *WAL) Replay() (map[string]*experiment.Run, error) {
	f, err := os.Open(l.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	runs := make(map[string]*experiment.Run)
	dec := json.NewDecoder(bufio.NewReader(f))
	for {
		var run experiment.Run
		if err := dec.Decode(&run); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// Partial write at tail: stop replay but keep intact prefix.
			if errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, fmt.Errorf("replay wal: %w", err)
		}
		cp := run
		runs[run.Spec.ID] = &cp
	}
	return runs, nil
}

// Path exposes the log location for diagnostics.
func (l *WAL) Path() string { return l.path }
