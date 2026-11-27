package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

type writeDeadliner interface {
	SetWriteDeadline(time.Time) error
}

func prepareOutput(ctx context.Context, out io.Writer) (io.Writer, func() error, error) {
	deadline, _ := ctx.Deadline() // export always supplies an overall deadline.
	var closePipe func() error
	if out == os.Stdout {
		pipe, err := openStdoutPipe(os.Stdout)
		if err != nil {
			return nil, nil, fmt.Errorf("open stdout for deadline-aware writes: %w", err)
		}
		if pipe != nil {
			out = pipe
			closePipe = func() error {
				if err := pipe.Close(); err != nil {
					return fmt.Errorf("close stdout pipe: %w", err)
				}
				return nil
			}
		}
	}
	setter, ok := out.(writeDeadliner)
	if !ok {
		return out, nil, nil
	}
	if err := setter.SetWriteDeadline(deadline); errors.Is(err, os.ErrNoDeadline) {
		return out, closePipe, nil
	} else if err != nil {
		err = fmt.Errorf("set stdout write deadline: %w", err)
		if closePipe != nil {
			err = errors.Join(err, closePipe())
		}
		return nil, nil, err
	}

	// A signal may arrive before the fixed deadline. Wake a blocked write
	// without putting the write itself in a goroutine that could be stranded.
	interrupted := make(chan error, 1)
	stop := context.AfterFunc(ctx, func() {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			interrupted <- nil // The original write deadline already applies.
			return
		}
		err := setter.SetWriteDeadline(time.Now())
		if err != nil {
			err = fmt.Errorf("interrupt stdout write: %w", err)
		}
		interrupted <- err
	})
	return out, func() error {
		var err error
		if !stop() {
			err = <-interrupted
		}
		if closePipe != nil {
			return errors.Join(err, closePipe())
		}
		if resetErr := setter.SetWriteDeadline(time.Time{}); resetErr != nil {
			err = errors.Join(err, fmt.Errorf("clear stdout write deadline: %w", resetErr))
		}
		return err
	}, nil
}
