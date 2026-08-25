package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestLokiSourceFile_HighThroughputActiveWrites(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "active.log")

	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("failed to create temp log file: %v", err)
	}
	defer f.Close()

	entryChan := make(chan Entry, 10000)
	comp := New(Config{
		Targets:       []string{logFile},
		PollFrequency: 2 * time.Millisecond,
		OutputChannel: entryChan,
	})
	defer comp.Stop()

	const lineCount = 5000
	var wg sync.WaitGroup
	wg.Add(1)

	// Continuous concurrent high throughput writer
	go func() {
		defer wg.Done()
		for i := 0; i < lineCount; i++ {
			line := fmt.Sprintf("log line item number %d\n", i)
			if _, err := f.WriteString(line); err != nil {
				return
			}
		}
		_ = f.Sync()
	}()

	received := 0
	timeout := time.After(10 * time.Second)

	for received < lineCount {
		select {
		case <-entryChan:
			received++
		case <-timeout:
			t.Fatalf("timed out waiting for lines: received %d/%d", received, lineCount)
		}
	}

	wg.Wait()
	if received != lineCount {
		t.Errorf("expected %d lines, got %d", lineCount, received)
	}
}

func TestLokiSourceFile_CancellationDuringActiveRead(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "cancel.log")

	f, err := os.Create(logFile)
	if err != nil {
		t.Fatalf("failed to create temp log file: %v", err)
	}
	defer f.Close()

	entryChan := make(chan Entry, 10)
	comp := New(Config{
		Targets:       []string{logFile},
		PollFrequency: 5 * time.Millisecond,
		OutputChannel: entryChan,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- comp.Run(ctx)
	}()

	// Write lines continuously in background
	stopWriter := make(chan struct{})
	go func() {
		i := 0
		for {
			select {
			case <-stopWriter:
				return
			default:
				_, _ = f.WriteString(fmt.Sprintf("line %d\n", i))
				i++
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	startCancel := time.Now()
	cancel()

	select {
	case <-done:
		if time.Since(startCancel) > 2*time.Second {
			t.Errorf("cancellation took longer than 2 seconds: %v", time.Since(startCancel))
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Component.Run failed to exit within 2 seconds")
	}

	close(stopWriter)
}

func TestLokiSourceFile_PartialLineBuffering(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "partial.log")

	f, err := os.Create(logFile)
	if err != nil {
		t.Fatalf("failed to create temp log file: %v", err)
	}
	defer f.Close()

	entryChan := make(chan Entry, 10)
	tailer := NewTailer(logFile, NewPositions(""), entryChan, 5*time.Millisecond)
	defer tailer.Stop()

	// Write partial line without newline
	_, _ = f.WriteString("part 1 -")
	_ = f.Sync()

	select {
	case e := <-entryChan:
		t.Fatalf("unexpected entry received before newline: %s", e.Line)
	case <-time.After(50 * time.Millisecond):
	}

	// Complete the line
	_, _ = f.WriteString(" part 2\n")
	_ = f.Sync()

	select {
	case e := <-entryChan:
		if e.Line != "part 1 - part 2" {
			t.Errorf("expected 'part 1 - part 2', got '%s'", e.Line)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for completed line")
	}
}

func TestLokiSourceFile_TruncationAndRotation(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "rotate.log")

	f, err := os.Create(logFile)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	entryChan := make(chan Entry, 100)
	comp := New(Config{
		Targets:       []string{logFile},
		PollFrequency: 5 * time.Millisecond,
		OutputChannel: entryChan,
	})
	defer comp.Stop()

	_, _ = f.WriteString("first line\n")
	_ = f.Sync()

	select {
	case e := <-entryChan:
		if e.Line != "first line" {
			t.Errorf("expected 'first line', got '%s'", e.Line)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for first line")
	}

	_ = f.Close()

	// Rotate by removing and recreating
	_ = os.Remove(logFile)
	f2, err := os.Create(logFile)
	if err != nil {
		t.Fatalf("failed to recreate file: %v", err)
	}
	defer f2.Close()

	_, _ = f2.WriteString("second line\n")
	_ = f2.Sync()

	select {
	case e := <-entryChan:
		if e.Line != "second line" {
			t.Errorf("expected 'second line', got '%s'", e.Line)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for second line after rotation")
	}
}
