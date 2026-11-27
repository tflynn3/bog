package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"syscall"
)

const (
	outputChunkSize    = 32 << 10
	maxOutputErrorSize = 1024
	outputSyncFrame    = math.MaxUint64
)

// After the startup handshake, a request is an eight-byte length and the
// record, or the reserved sync value with no payload. A two-byte zero reply
// acknowledges the requested operation. A nonzero length introduces a
// bounded error string. There is at most one outstanding request.
func (worker *outputWorker) writeRecord(ctx context.Context, record []byte) error {
	return worker.withDeadline(ctx, func() error {
		return worker.exchange(uint64(len(record)), record)
	})
}

func (worker *outputWorker) sync(ctx context.Context) error {
	return worker.withDeadline(ctx, func() error {
		return worker.exchange(outputSyncFrame, nil)
	})
}

func (worker *outputWorker) exchange(length uint64, record []byte) error {
	var header [8]byte
	binary.BigEndian.PutUint64(header[:], length)
	if err := writeOnce(worker.request, header[:]); err != nil {
		return fmt.Errorf("send output record length: %w", err)
	}
	var sendErr error
	if len(record) != 0 {
		sendErr = writeOnce(worker.request, record)
	}
	// If the destination failed early on a large record, the worker
	// closes the request reader. Read its response even on EPIPE so
	// the destination's error is retained.
	var response [2]byte
	if _, err := io.ReadFull(worker.reply, response[:]); err != nil {
		return errors.Join(sendErr, fmt.Errorf("read output acknowledgment: %w", err))
	}
	size := int(binary.BigEndian.Uint16(response[:]))
	if size == 0 {
		return sendErr
	}
	if size > maxOutputErrorSize {
		return errors.Join(sendErr, errors.New("invalid output worker error length"))
	}
	message := make([]byte, size)
	if _, err := io.ReadFull(worker.reply, message); err != nil {
		return errors.Join(sendErr, fmt.Errorf("read output worker error: %w", err))
	}
	return errors.Join(errors.New(string(message)), sendErr)
}

func runOutputWorker(destination io.Writer) int {
	// The exporter handles group cancellation and owns orderly teardown.
	signal.Ignore(syscall.SIGPIPE, syscall.SIGINT, syscall.SIGTERM)
	reply := os.NewFile(3, "output reply")
	lifeline := os.NewFile(4, "output lifeline")
	// If the exporter disappears before it can reap us, EOF on the
	// lifeline exits this process even during a blocked destination write.
	go func() {
		var probe [1]byte
		_, err := lifeline.Read(probe[:])
		closeErr := lifeline.Close()
		if !errors.Is(err, io.EOF) || closeErr != nil {
			os.Exit(exitIncomplete)
		}
		os.Exit(exitOK)
	}()
	err := writeOnce(reply, []byte(workerHandshake))
	if err == nil {
		err = serveOutput(os.Stdin, destination, reply)
	}
	if err := errors.Join(err, reply.Close()); err != nil {
		return exitIncomplete
	}
	return exitOK
}

func serveOutput(request io.ReadCloser, destination, reply io.Writer) (resultErr error) {
	defer func() {
		if err := request.Close(); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close output reader: %w", err))
		}
	}()
	var header [8]byte
	var buffer [outputChunkSize]byte
	for {
		if _, err := io.ReadFull(request, header[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read output record length: %w", err)
		}
		remaining := binary.BigEndian.Uint64(header[:])
		if remaining == outputSyncFrame {
			syncer, ok := destination.(interface{ Sync() error })
			var err error
			if ok {
				err = syncer.Sync()
			} else {
				err = errors.New("destination does not support sync")
			}
			if replyErr := sendOutputReply(reply, err); err != nil || replyErr != nil {
				return replyErr
			}
			continue
		}
		for remaining > 0 {
			chunk := buffer[:min(remaining, uint64(len(buffer)))]
			if _, err := io.ReadFull(request, chunk); err != nil {
				return fmt.Errorf("read output record: %w", err)
			}
			if err := writeOnce(destination, chunk); err != nil {
				// Return the failure to the exporter. The bounded reply
				// fits independently of any unfinished request payload.
				return sendOutputReply(reply, err)
			}
			remaining -= uint64(len(chunk))
		}
		if err := sendOutputReply(reply, nil); err != nil {
			return err
		}
	}
}

func sendOutputReply(reply io.Writer, writeErr error) error {
	var frame [2 + maxOutputErrorSize]byte
	size := 0
	if writeErr != nil {
		size = copy(frame[2:], writeErr.Error())
		if size == 0 {
			size = copy(frame[2:], "output write failed")
		}
	}
	binary.BigEndian.PutUint16(frame[:2], uint16(size))
	if err := writeOnce(reply, frame[:2+size]); err != nil {
		return fmt.Errorf("send output reply: %w", err)
	}
	return nil
}
