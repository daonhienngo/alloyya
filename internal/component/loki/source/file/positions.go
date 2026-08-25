package file

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// Positions tracks read byte offsets for monitored files in a thread-safe manner.
type Positions interface {
	Get(path string) int64
	Put(path string, offset int64)
	Remove(path string)
	Save() error
	Load() error
}

type positionsTracker struct {
	mu       sync.RWMutex
	filename string
	data     map[string]int64
}

// NewPositions creates a new positions tracker with optional persistence file.
func NewPositions(filename string) Positions {
	p := &positionsTracker{
		filename: filename,
		data:     make(map[string]int64),
	}
	if filename != "" {
		_ = p.Load()
	}
	return p
}

// Get returns the saved offset for a given file path.
func (p *positionsTracker) Get(path string) int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.data[path]
}

// Put updates the saved offset for a given file path.
func (p *positionsTracker) Put(path string, offset int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.data[path] = offset
}

// Remove removes the tracked offset for a given file path.
func (p *positionsTracker) Remove(path string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.data, path)
}

// Load reads positions from disk.
func (p *positionsTracker) Load() error {
	if p.filename == "" {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	f, err := os.Open(p.filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	var diskData map[string]int64
	if err := json.NewDecoder(f).Decode(&diskData); err != nil {
		return err
	}

	for k, v := range diskData {
		p.data[k] = v
	}
	return nil
}

// Save persists the current positions map to disk safely.
func (p *positionsTracker) Save() error {
	if p.filename == "" {
		return nil
	}

	var snapshot map[string]int64
	p.mu.RLock()
	snapshot = make(map[string]int64, len(p.data))
	for k, v := range p.data {
		snapshot[k] = v
	}
	p.mu.RUnlock()

	dir := filepath.Dir(p.filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	tmpFile := p.filename + ".tmp"
	f, err := os.OpenFile(tmpFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	if err := json.NewEncoder(f).Encode(snapshot); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpFile)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpFile)
		return err
	}

	return os.Rename(tmpFile, p.filename)
}
