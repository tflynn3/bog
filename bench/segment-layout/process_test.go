package main

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestWorkerLifecycle(t *testing.T) {
	for _, mode := range []string{"success", "failure", "canceled", "read failure"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			args := []string{"-test.run=^TestWorkerLifecycleHelper$", "--", mode}
			injected := errors.New("read failure")
			state, err := runWorker(ctx, args, func(r io.Reader) error {
				// Read the marker before canceling so cancellation exercises an
				// already-started subprocess and an active output pipe.
				var marker [5]byte
				if _, err := io.ReadFull(r, marker[:]); err != nil {
					return err
				}
				if string(marker[:]) != "ready" {
					return errors.New("unexpected worker marker")
				}
				switch mode {
				case "read failure":
					return injected
				case "canceled":
					cancel()
				}
				_, err := io.Copy(io.Discard, r)
				return err
			})
			if state == nil || state.Pid() <= 0 {
				t.Fatalf("worker was not reaped: %v, %v", state, err)
			}
			switch mode {
			case "success":
				if err != nil || !state.Success() {
					t.Fatalf("worker success: %v, %v", state, err)
				}
			case "failure":
				if err == nil || state.ExitCode() != 5 || !strings.Contains(err.Error(), "expected stderr") {
					t.Fatalf("worker failure: %v, %v", state, err)
				}
			case "canceled":
				if !errors.Is(err, context.Canceled) || state.Success() {
					t.Fatalf("worker cancellation: %v, %v", state, err)
				}
			case "read failure":
				if !errors.Is(err, injected) || state.Success() {
					t.Fatalf("worker read failure: %v, %v", state, err)
				}
			}
		})
	}
}

func TestWorkerLifecycleHelper(t *testing.T) {
	if len(os.Args) < 4 || os.Args[len(os.Args)-2] != "--" {
		return
	}
	if _, err := io.WriteString(os.Stdout, "ready"); err != nil {
		t.Fatal(err)
	}
	switch os.Args[len(os.Args)-1] {
	case "failure":
		if _, err := io.WriteString(os.Stderr, "expected stderr"); err != nil {
			t.Fatal(err)
		}
		os.Exit(5)
	case "read failure", "canceled":
		time.Sleep(15 * time.Second)
		t.Fatal("parent did not stop worker")
	}
}

func TestWorkerDeadlineUnblocksOutputRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	state, err := runWorker(ctx, []string{"-test.run=^TestWorkerLifecycleHelper$", "--", "canceled"}, func(r io.Reader) error {
		_, err := io.Copy(io.Discard, r)
		return err
	})
	if !errors.Is(err, context.DeadlineExceeded) || state == nil {
		t.Fatalf("worker deadline: %v, %v", state, err)
	}
}
