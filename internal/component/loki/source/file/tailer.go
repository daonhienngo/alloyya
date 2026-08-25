package file

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"time"
)

// Entry represents a single scraped log entry.
type Entry struct {
	Path   string
	Line   string
	Offset int64
	Time   time.Time
}

// Tailer reads and streams new lines from an active file.
type Tailer struct {
	path       string
	positions  Positions
	out        chan<- Entry
	pollPeriod time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewTailer creates a new tailer for the specified file path.
func NewTailer(path string, positions Positions, out chan<- Entry, pollPeriod time.Duration) *Tailer {
	if pollPeriod <= 0 {
		pollPeriod = 10 * time.Millisecond
	}

	ctx, cancel := context.WithCancel(context.Background())

	t := &Tailer{
		path:       path,
		positions:  positions,
		out:        out,
		pollPeriod: pollPeriod,
		ctx:        ctx,
		cancel:     cancel,
	}

	t.wg.Add(1)
	go t.readLoop()

	return t
}

// Stop gracefully stops the tailer and waits for goroutines to exit.
func (t *Tailer) Stop() {
	t.cancel()
	t.wg.Wait()
}

func (t *Tailer) readLoop() {
	defer t.wg.Done()

	var offset int64
	if t.positions != nil {
		offset = t.positions.Get(t.path)
	}

	var file *os.File
	var lastFi os.FileInfo

	defer func() {
		if file != nil {
			_ = file.Close()
		}
	}()

	buf := make([]byte, 64*1024)
	var partial bytes.Buffer
	ticker := time.NewTicker(t.pollPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		default:
		}

		// Open file if not currently open
		if file == nil {
			fi, err := os.Stat(t.path)
			if err != nil {
				select {
				case <-t.ctx.Done():
					return
				case <-ticker.C:
					continue
				}
			}

			f, err := os.Open(t.path)
			if err != nil {
				select {
				case <-t.ctx.Done():
					return
				case <-ticker.C:
					continue
				}
			}

			if offset > fi.Size() {
				offset = 0
			}

			_, err = f.Seek(offset, io.SeekStart)
			if err != nil {
				_ = f.Close()
				select {
				case <-t.ctx.Done():
					return
				case <-ticker.C:
					continue
				}
			}

			file = f
			lastFi = fi
		}

		// Check for truncation or rotation
		fi, err := os.Stat(t.path)
		if err == nil {
			if !os.SameFile(fi, lastFi) {
				// File rotated
				_ = file.Close()
				file = nil
				offset = 0
				partial.Reset()
				continue
			} else if fi.Size() < offset {
				// File truncated
				offset = 0
				partial.Reset()
				_, _ = file.Seek(0, io.SeekStart)
			}
		}

		n, err := file.Read(buf)
		if n > 0 {
			readBytes := buf[:n]
			start := 0
			for i := 0; i < n; i++ {
				if readBytes[i] == '\n' {
					var line string
					if partial.Len() > 0 {
						partial.Write(readBytes[start:i])
						line = partial.String()
						partial.Reset()
					} else {
						line = string(readBytes[start:i])
					}

					// Strip optional trailing '\r'
					if len(line) > 0 && line[len(line)-1] == '\r' {
						line = line[:len(line)-1]
					}

					lineOffset := offset + int64(i) + 1
					entry := Entry{
						Path:   t.path,
						Line:   line,
						Offset: lineOffset,
						Time:   time.Now(),
					}

					select {
					case <-t.ctx.Done():
						return
					case t.out <- entry:
					}

					if t.positions != nil {
						t.positions.Put(t.path, lineOffset)
					}
					start = i + 1
				}
			}

			if start < n {
				partial.Write(readBytes[start:])
			}
			offset += int64(n)
		}

		if err != nil {
			if err == io.EOF {
				select {
				case <-t.ctx.Done():
					return
				case <-ticker.C:
					continue
				}
			} else {
				_ = file.Close()
				file = nil
				select {
				case <-t.ctx.Done():
					return
				case <-ticker.C:
					continue
				}
			}
		}
	}
}
