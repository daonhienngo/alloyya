package file

import (
	"context"
	"sync"
	"time"
)

// Config defines configuration settings for loki.source.file.
type Config struct {
	Targets        []string
	PositionsPath  string
	PollFrequency  time.Duration
	OutputChannel  chan Entry
}

// Component represents the loki.source.file component.
type Component struct {
	mu         sync.Mutex
	config     Config
	positions  Positions
	tailers    map[string]*Tailer
	outChan    chan Entry
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// New creates a new loki.source.file component.
func New(cfg Config) *Component {
	if cfg.PollFrequency <= 0 {
		cfg.PollFrequency = 10 * time.Millisecond
	}

	out := cfg.OutputChannel
	if out == nil {
		out = make(chan Entry, 1000)
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Component{
		config:    cfg,
		positions: NewPositions(cfg.PositionsPath),
		tailers:   make(map[string]*Tailer),
		outChan:   out,
		ctx:       ctx,
		cancel:    cancel,
	}

	c.syncTailers(cfg.Targets)
	return c
}

// Entries returns the channel of scraped log entries.
func (c *Component) Entries() <-chan Entry {
	return c.outChan
}

// Update updates the monitored target files dynamically.
func (c *Component) Update(targets []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config.Targets = targets
	c.syncTailers(targets)
}

func (c *Component) syncTailers(targets []string) {
	targetMap := make(map[string]bool, len(targets))
	for _, t := range targets {
		targetMap[t] = true
		if _, exists := c.tailers[t]; !exists {
			c.tailers[t] = NewTailer(t, c.positions, c.outChan, c.config.PollFrequency)
		}
	}

	for path, tailer := range c.tailers {
		if !targetMap[path] {
			tailer.Stop()
			delete(c.tailers, path)
		}
	}
}

// Run runs the component lifecycle until context cancellation.
func (c *Component) Run(ctx context.Context) error {
	<-ctx.Done()
	c.Stop()
	return nil
}

// Stop gracefully shuts down all active tailers and persists positions.
func (c *Component) Stop() {
	c.cancel()
	c.mu.Lock()
	for path, tailer := range c.tailers {
		tailer.Stop()
		delete(c.tailers, path)
	}
	c.mu.Unlock()

	if c.positions != nil {
		_ = c.positions.Save()
	}
}
