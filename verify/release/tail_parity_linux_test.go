package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"bog/internal/backup"
	querymodel "bog/verify/query-model"
)

func tailOnlyParity(ctx context.Context, binary, root string) (proof tailParityProof, err error) {
	fixtures, err := tailParityFixtures()
	if err != nil {
		return proof, err
	}
	rows := parityRows()
	dir, _, err := makeRestoredFixture(ctx, root, rows)
	if err != nil {
		return proof, fmt.Errorf("prepare v1 tail-only restore: %w", err)
	}
	for _, name := range []string{"bundle", "source"} {
		if err := os.RemoveAll(filepath.Join(root, name)); err != nil {
			return proof, fmt.Errorf("remove initial %s: %w", name, err)
		}
	}
	input, err := readTailParityImage(ctx, dir, "BOGBAK01")
	if err != nil {
		return proof, err
	}
	firstGeneration := input.Generation
	var log querymodel.Log
	log.Append(rows)
	client := &http.Client{
		Transport:     &http.Transport{},
		Timeout:       5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	defer client.CloseIdleConnections()
	var archived [tailParityCycles]string
	for i, fixture := range fixtures {
		if i > 0 {
			fixture.input = proof.Cycles[i-1].Export
		}
		if err := checkTailParityPoint(input, fixture.input); err != nil {
			return proof, fmt.Errorf("cycle %d input: %w", i+1, err)
		}
		exported := filepath.Join(root, fmt.Sprintf("export-%d", i+1))
		restored := filepath.Join(root, fmt.Sprintf("restore-%d", i+1))
		archived[i] = filepath.Join(root, fmt.Sprintf("archived-%d", i+1))
		cycle, err := runTailParityCycle(ctx, binary, dir, exported, archived[i], restored, client, &log, fixture, input)
		proof.Cycles[i] = cycle
		if err != nil {
			return proof, fmt.Errorf("cycle %d: %w", i+1, err)
		}
		if err := validateTailParityCycle(cycle, fixture, firstGeneration); err != nil {
			return proof, fmt.Errorf("cycle %d: %w", i+1, err)
		}
		dir, input = restored, cycle.Export
	}
	for i, path := range archived {
		got, err := readTailParityImage(ctx, path, "BOGBAK02")
		if err != nil {
			return proof, fmt.Errorf("reverify archived export %d: %w", i+1, err)
		}
		if err := checkTailParityPoint(got, proof.Cycles[i].Export); err != nil {
			return proof, fmt.Errorf("archived export %d changed: %w", i+1, err)
		}
	}
	return proof, nil
}

func runTailParityCycle(ctx context.Context, binary, dir, exported, archived, restored string, client *http.Client,
	log *querymodel.Log, fixture tailCycleFixture, input tailParityPoint,
) (cycle tailParityCycle, err error) {
	cycle.Input = input
	flags := []string{fmt.Sprintf("-query-max-response-bytes=%d", restoredPageCap)}
	data, err := os.OpenRoot(dir)
	if err != nil {
		return cycle, err
	}
	defer func() { err = errors.Join(err, data.Close()) }()
	if err := requireTailOnlyDirectory(data); err != nil {
		return cycle, err
	}
	err = serveDirectoryWithStop(ctx, binary, dir, client, flags, (*candidateProcess).crashStop, func(ctx context.Context, base string) error {
		if _, err := requireTailParityStatus(ctx, client, base, input.NextOrdinal, input.CommittedOrdinal); err != nil {
			return fmt.Errorf("writable restore status: %w", err)
		}
		pager, err := newReleasePager(base, client, restoredPageCap)
		if err != nil {
			return err
		}
		walk := restoredWalk(pager, "", []int{1, 3})
		held, err := querymodel.VerifyWireWalk(ctx, log, walk, parityFetch(pager), func(ctx context.Context, _ string) error {
			for i, batch := range fixture.batches {
				status, appendErr := appendTailParityRows(ctx, client, base, log, batch)
				cycle.AppendStatuses[i] = status
				if appendErr != nil {
					return fmt.Errorf("append batch %d: %w", i+1, appendErr)
				}
			}
			previous, err := requireTailParityStatus(ctx, client, base, input.NextOrdinal, fixture.export.CommittedOrdinal)
			if err != nil {
				return err
			}
			_, err = publishReleaseGeneration(ctx, client, base, fixture.export.NextOrdinal, fixture.export.CommittedOrdinal, &previous)
			return err
		})
		if err != nil {
			return fmt.Errorf("held walk across append and compaction: %w", err)
		}
		cycle.HeldRows = held.Rows
		cycle.FreshRows, err = walkTailParity(ctx, pager, log)
		if err != nil {
			return fmt.Errorf("fresh walk after compaction: %w", err)
		}
		return nil
	})
	if err != nil {
		return cycle, fmt.Errorf("serve writable tail and compact: %w", err)
	}

	tail, err := exportTailParityCycle(ctx, data, exported, fixture, &cycle)
	if err != nil {
		return cycle, err
	}
	defer func() { err = errors.Join(err, tail.file.Close()) }()
	if err := backup.Restore(ctx, exported, restored); err != nil {
		return cycle, fmt.Errorf("restore v2 export: %w", err)
	}
	got, err := readTailParityImage(ctx, restored, "BOGBAK02")
	if err != nil {
		return cycle, fmt.Errorf("verify restored export: %w", err)
	}
	if err := checkTailParityPoint(got, cycle.Export); err != nil {
		return cycle, fmt.Errorf("restore differs from export: %w", err)
	}
	// Move the verified bundle before either startup and keep its bytes
	// available for independent reverification after the next cycle.
	if err := os.Rename(exported, archived); err != nil {
		return cycle, fmt.Errorf("archive source export: %w", err)
	}
	restoredRoot, err := os.OpenRoot(restored)
	if err != nil {
		return cycle, err
	}
	if err := errors.Join(requireTailOnlyDirectory(restoredRoot), restoredRoot.Close()); err != nil {
		return cycle, err
	}

	// The restarted live directory must extend the exported tail at its
	// recovered ordinal. A separate oracle keeps this probe append out of
	// the history consumed by the next writable restore.
	var restartedLog querymodel.Log
	restartRows, err := log.Match(log.HighWater(), querymodel.Filter{Start: fixtureTime(0), End: fixtureTime(3)})
	if err != nil {
		return cycle, err
	}
	if len(restartRows) != log.HighWater() {
		return cycle, errors.New("restart oracle must cover every appended row")
	}
	// Match returns query order. Rebuild append order using the oracle's
	// append ordinals before adding another timestamp tie.
	slices.SortFunc(restartRows, func(a, b querymodel.Entry) int { return cmp.Compare(a.Ordinal, b.Ordinal) })
	for _, r := range restartRows {
		restartedLog.Append([]querymodel.Row{r.Row})
	}
	probe := []row{fixtureWireRow(1, fmt.Sprintf("restart after ordinal %d [match]", cycle.Export.CommittedOrdinal))}
	err = serveDirectoryWithStop(ctx, binary, dir, client, flags, (*candidateProcess).crashStop, func(ctx context.Context, base string) error {
		pager, err := newReleasePager(base, client, restoredPageCap)
		if err != nil {
			return err
		}
		p := cycle.Export
		if _, err := requireTailParityStatus(ctx, client, base, p.NextOrdinal, p.CommittedOrdinal); err != nil {
			return err
		}
		cycle.RestartRows, err = walkTailParity(ctx, pager, log)
		if err != nil {
			return err
		}
		cycle.RestartAppendStatus, err = appendTailParityRows(ctx, client, base, &restartedLog, probe)
		if err != nil {
			return err
		}
		status, err := requireTailParityStatus(ctx, client, base, p.NextOrdinal, p.CommittedOrdinal+len(probe))
		if err != nil {
			return err
		}
		cycle.RestartNextOrdinal = status.committed
		_, err = walkTailParity(ctx, pager, &restartedLog)
		if err != nil {
			return fmt.Errorf("walk after restarted append: %w", err)
		}
		return nil
	})
	if err != nil {
		return cycle, fmt.Errorf("restart advanced directory and append: %w", err)
	}
	probeFrame, err := fixtureFrame(probe)
	if err != nil {
		return cycle, err
	}
	if err := verifyParityRestartAppend(ctx, data, tail, probeFrame, len(probe)); err != nil {
		return cycle, fmt.Errorf("verify selected tail after restart append: %w", err)
	}
	err = serveDirectory(ctx, binary, restored, client, flags, func(ctx context.Context, base string) error {
		p := cycle.Export
		if _, err := requireTailParityStatus(ctx, client, base, p.NextOrdinal, p.CommittedOrdinal); err != nil {
			return err
		}
		pager, err := newReleasePager(base, client, restoredPageCap)
		if err != nil {
			return err
		}
		cycle.RestoredRows, err = walkTailParity(ctx, pager, log)
		return err
	})
	if err != nil {
		return cycle, fmt.Errorf("start separate v2 restore after source relocation: %w", err)
	}
	return cycle, nil
}

func appendTailParityRows(ctx context.Context, client *http.Client, base string, log *querymodel.Log, batch []row) (int, error) {
	modelRows := make([]querymodel.Row, 0, len(batch))
	for _, r := range batch {
		ts, err := time.Parse(time.RFC3339Nano, r.TS)
		if err != nil {
			return 0, err
		}
		modelRows = append(modelRows, querymodel.Row{Time: ts, Line: r.Line})
	}
	status, err := ingest(ctx, client, base, batch)
	if err == nil {
		log.Append(modelRows)
	}
	return status, err
}

func walkTailParity(ctx context.Context, pager *releasePager, log *querymodel.Log) (int, error) {
	walk := restoredWalk(pager, "", []int{3, 1, 10})
	stats, err := querymodel.VerifyWireWalk(ctx, log, walk, parityFetch(pager), nil)
	return stats.Rows, err
}

func requireTailParityStatus(ctx context.Context, client *http.Client, base string, covered, committed int) (backfillCompaction, error) {
	s, err := readBackfillCompaction(ctx, client, base, http.MethodGet, "status", 0)
	if err == nil && (s.state != "complete" || s.covered != covered || s.committed != committed) {
		err = fmt.Errorf("compaction status %+v, want complete with covered=%d and committed=%d", s, covered, committed)
	}
	return s, err
}

func TestTailParityV1Fixture(t *testing.T) {
	dir, _, err := makeRestoredFixture(t.Context(), t.TempDir(), parityRows())
	if err != nil {
		t.Fatal(err)
	}
	point, err := readTailParityImage(t.Context(), dir, "BOGBAK01")
	if err != nil {
		t.Fatal(err)
	}
	fixtures, err := tailParityFixtures()
	if err != nil {
		t.Fatal(err)
	}
	if err := checkTailParityPoint(point, fixtures[0].input); err != nil {
		t.Fatal(err)
	}
}
