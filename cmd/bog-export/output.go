package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// recordOutput binds one ordered byte stream to a context. WriteRecord
// returns nil only after the whole record has been written and cancellation
// has been checked. A failed record may leave a prefix in the stream. The
// caller must stop there, without retrying it or writing subsequent records.
// Close releases resources and waits for any output worker to exit.
type recordOutput struct {
	ctx    context.Context
	write  func([]byte) error
	finish func() error
}

func openOutput(ctx context.Context, out io.Writer) (*recordOutput, error) {
	if err := outputCancellation(ctx, nil); err != nil {
		return nil, err
	}
	if out == os.Stdout || out == os.Stderr {
		return openInheritedOutput(ctx, out.(*os.File))
	}

	// In-process callers supply synchronous writers or writers whose
	// SetWriteDeadline interrupts Write. Only the CLI owns inherited output.
	output := &recordOutput{
		ctx:   ctx,
		write: func(record []byte) error { return writeOnce(out, record) },
	}
	if file, ok := out.(interface{ SetWriteDeadline(time.Time) error }); ok {
		finish, err := armOutputDeadline(ctx, file.SetWriteDeadline)
		if err != nil && !errors.Is(err, os.ErrNoDeadline) {
			return nil, fmt.Errorf("set write deadline: %w", err)
		}
		if finish != nil {
			output.finish = func() error {
				return errors.Join(finish(), file.SetWriteDeadline(time.Time{}))
			}
		}
	}
	return output, nil
}

func (out *recordOutput) WriteRecord(record []byte) error {
	if err := outputCancellation(out.ctx, nil); err != nil {
		return err
	}
	return outputCancellation(out.ctx, out.write(record))
}

func (out *recordOutput) Close() error {
	if out.finish == nil {
		return nil
	}
	return out.finish()
}

// The interrupt callback only changes a Go poll deadline. The writer stays
// synchronous, and cleanup joins the callback before closing or resetting
// the underlying descriptor.
func armOutputDeadline(ctx context.Context, set func(time.Time) error) (func() error, error) {
	deadline, _ := ctx.Deadline()
	if err := set(deadline); err != nil {
		return nil, err
	}
	interrupted := make(chan error, 1)
	stop := context.AfterFunc(ctx, func() {
		interrupted <- set(time.Now())
	})
	return func() error {
		if stop() {
			return nil
		}
		return <-interrupted
	}, nil
}

func outputCancellation(ctx context.Context, err error) error {
	contextErr := ctx.Err()
	if contextErr == nil {
		// The Go poller may time out just before the context's timer runs.
		// The overall deadline has still expired.
		if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
			contextErr = context.DeadlineExceeded
		}
	}
	if cause := context.Cause(ctx); cause != nil && !errors.Is(contextErr, cause) {
		err = addOutputError(err, cause)
	}
	return addOutputError(err, contextErr)
}

func addOutputError(err, additional error) error {
	if err == nil {
		return additional
	}
	if additional == nil || errors.Is(err, additional) {
		return err
	}
	return errors.Join(err, additional)
}

func writeOnce(out io.Writer, data []byte) error {
	n, err := out.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}
