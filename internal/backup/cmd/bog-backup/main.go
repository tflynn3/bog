// Command bog-backup exports, verifies, and restores checkpoint-consistent backups.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"bog/internal/backup"
)

func run(ctx context.Context, args []string, out io.Writer) error {
	var snapshot backup.Snapshot
	var err error
	switch {
	case len(args) == 3 && args[0] == "export":
		snapshot, err = backup.Backup(ctx, args[1], args[2])
	case len(args) == 2 && args[0] == "verify":
		snapshot, err = backup.Verify(ctx, args[1])
	case len(args) == 3 && args[0] == "restore":
		snapshot, err = backup.Restore(ctx, args[1], args[2])
	default:
		return fmt.Errorf("usage: bog-backup export STORE BACKUP | verify BACKUP | restore BACKUP STORE")
	}
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "committed_offset=%d covered_offset=%d rows=%d wal_sha256=%x manifest_sha256=%x\n",
		snapshot.CommittedOffset, snapshot.CoveredOffset, snapshot.Rows, snapshot.WALSHA256, snapshot.ManifestSHA256)
	return err
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout)
	stop()
	if err != nil {
		if _, writeErr := fmt.Fprintln(os.Stderr, err); writeErr != nil {
			os.Exit(2)
		}
		os.Exit(1)
	}
}
