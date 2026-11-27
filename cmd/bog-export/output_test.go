package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOutputProtocol(t *testing.T) {
	t.Parallel()
	rejected := errors.New("destination rejected output")
	large := bytes.Repeat([]byte{'x'}, outputChunkSize+5)
	for _, tc := range []struct {
		name        string
		request     []byte
		failWrite   bool
		destination []byte
		reply       []byte
		err         error
	}{
		{name: "order", request: append(outputFrame(large), outputFrame([]byte("next"))...),
			destination: append(bytes.Clone(large), "next"...), reply: []byte{0, 0, 0, 0}},
		{name: "truncated length", request: []byte{1}, err: io.ErrUnexpectedEOF},
		{name: "truncated payload", request: outputFrame(large)[:8+outputChunkSize+1],
			destination: large[:outputChunkSize], err: io.ErrUnexpectedEOF},
		{name: "destination failure", request: append(outputFrame(large), outputFrame([]byte("never"))...),
			failWrite: true, destination: large[:3], reply: append([]byte{0, byte(len(rejected.Error()))}, rejected.Error()...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var destination, reply bytes.Buffer
			out := writerFunc(func(chunk []byte) (int, error) {
				if tc.failWrite {
					n, err := destination.Write(chunk[:3])
					return n, errors.Join(rejected, err)
				}
				return destination.Write(chunk)
			})
			err := serveOutput(io.NopCloser(bytes.NewReader(tc.request)), out, &reply)
			if !errors.Is(err, tc.err) {
				t.Errorf("serve error = %v, want %v", err, tc.err)
			}
			if !bytes.Equal(destination.Bytes(), tc.destination) {
				t.Errorf("destination has %d bytes, want %d exact bytes", destination.Len(), len(tc.destination))
			}
			if !bytes.Equal(reply.Bytes(), tc.reply) {
				t.Errorf("reply = %q, want %q", reply.Bytes(), tc.reply)
			}
		})
	}
}

func outputFrame(payload []byte) []byte {
	return append(binary.BigEndian.AppendUint64(nil, uint64(len(payload))), payload...)
}

func TestRecordOutputReportsCancellationOfFinalRecord(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancelCause(t.Context())
	cause := errors.New("interrupted during final write")
	defer cancel(nil)
	calls := 0
	output, err := openOutput(ctx, writerFunc(func(record []byte) (int, error) {
		calls++
		cancel(cause)
		return len(record), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := output.Close(); err != nil {
			t.Error(err)
		}
	}()
	for range 2 {
		if err := output.WriteRecord([]byte("last")); !errors.Is(err, cause) {
			t.Errorf("write error = %v, want %v", err, cause)
		}
	}
	if calls != 1 {
		t.Errorf("destination writes = %d, want 1", calls)
	}
}

func TestOutputExecutableWithoutProcfs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bog-export")
	if err := os.WriteFile(path, nil, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	noProcfs := func() (string, error) { return "", os.ErrNotExist }
	for _, invocation := range []string{path, "bog-export"} {
		got, err := outputExecutable(noProcfs, invocation)
		if err != nil || got != path {
			t.Errorf("resolve %q: got %q, %v, want %q", invocation, got, err, path)
		}
	}
	if _, err := outputExecutable(noProcfs, "missing-exporter"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("resolve missing exporter: %v", err)
	}
}

func TestOutputWorkerStartFailure(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	f := filepath.Join(t.TempDir(), "missing-exporter")
	if _, err := startOutputWorker(ctx, f, os.Stdout); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("start missing worker: %v", err)
	}
}
