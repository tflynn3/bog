package checkpoint

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"bog/internal/store"
)

func checkpointNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func requirePublicationOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDefinitivePublicationFailureCleansOnlyAttempt(t *testing.T) {
	dir, _, _ := fixture(t)
	l, err := OpenLegacy(dir)
	requirePublicationOK(t, err)
	closeOnCleanup(t, l)
	old, err := l.Publish()
	requirePublicationOK(t, err)
	unknown := segmentPrefix + "11111111111111111111111111111111"
	requirePublicationOK(t, os.WriteFile(filepath.Join(dir, unknown), []byte("unknown ownership"), 0600))
	want := checkpointNames(t, dir)
	l.segmentDirSync = func() error { return syscall.EIO }
	for range 3 {
		if m, err := l.Publish(); m != nil || !errors.Is(err, syscall.EIO) || errors.Is(err, ErrPublicationUncertain) {
			t.Fatalf("definitive failure returned manifest %v and error %v", m, err)
		}
		if got := checkpointNames(t, dir); !slices.Equal(got, want) {
			t.Fatalf("directory after failed publication: got %v, want %v", got, want)
		}
		p, err := l.OpenPublished()
		requirePublicationOK(t, err)
		if !slices.Equal(p.Manifest().Segments, old.Segments) {
			t.Fatal("failed publication changed authoritative segments")
		}
	}
	l.segmentDirSync = nil
	next, err := l.Publish()
	requirePublicationOK(t, err)
	if slices.Equal(old.Segments, next.Segments) {
		t.Fatal("retry did not install a successor")
	}
	if got := readFixture(t, dir, unknown); string(got) != "unknown ownership" {
		t.Fatal("unowned file changed")
	}
}

func TestRecoveryPreservesCollisionAndRemovesInstalledName(t *testing.T) {
	dir, _, _ := fixture(t)
	l, err := OpenLegacy(dir)
	requirePublicationOK(t, err)
	closeOnCleanup(t, l)
	_, err = l.Publish()
	requirePublicationOK(t, err)
	requirePublicationOK(t, l.root.Mkdir(stageDir, 0700))
	a := publicationAttempt{pub: l.publication(), ctx: t.Context()}
	first := segmentPrefix + "22222222222222222222222222222222"
	collision := segmentPrefix + "33333333333333333333333333333333"
	_, blocks := blockSeeds(t)
	requirePublicationOK(t, a.stageSegment(first, blocks[0], digest(blocks[0])))
	requirePublicationOK(t, a.stageSegment(collision, blocks[1], digest(blocks[1])))
	requirePublicationOK(t, os.WriteFile(filepath.Join(dir, collision), []byte("preexisting file"), 0600))
	if err := a.install(); !errors.Is(err, os.ErrExist) {
		t.Fatalf("expected final name collision: %v", err)
	}
	// Bypass the normal failure defer to model interruption after a partial install.
	requirePublicationOK(t, l.Close())
	l, err = OpenLegacy(dir)
	requirePublicationOK(t, err)
	closeOnCleanup(t, l)
	_, err = l.OpenPublished()
	requirePublicationOK(t, err)
	if got := readFixture(t, dir, collision); string(got) != "preexisting file" {
		t.Fatal("recovery removed or changed a name collision")
	}
	for _, name := range []string{first, stageDir} {
		if _, err := os.Lstat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("owned path %q remains after recovery: %v", name, err)
		}
	}
}

func TestUncertainPublicationWithUnreadableAuthorityLeavesAllFiles(t *testing.T) {
	dir, _, _ := fixture(t)
	l, err := OpenLegacy(dir)
	requirePublicationOK(t, err)
	closeOnCleanup(t, l)
	_, err = l.Publish()
	requirePublicationOK(t, err)
	good := readFixture(t, dir, manifestName)
	l.manifestRename = func(string, string) error { return syscall.EIO }
	if _, err := l.Publish(); !errors.Is(err, ErrPublicationUncertain) {
		t.Fatalf("expected uncertainty: %v", err)
	}
	requirePublicationOK(t, l.Close())
	requirePublicationOK(t, os.WriteFile(filepath.Join(dir, manifestName), []byte("damaged"), 0600))
	before := checkpointNames(t, dir)
	l, err = OpenLegacy(dir)
	requirePublicationOK(t, err)
	closeOnCleanup(t, l)
	if _, err := l.OpenPublished(); err == nil {
		t.Fatal("corrupt authority accepted")
	}
	requirePublicationOK(t, os.Remove(filepath.Join(dir, manifestName)))
	if _, err := l.OpenPublished(); !errors.Is(err, ErrIntegrity) || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing previous manifest: %v", err)
	}
	requirePublicationOK(t, os.WriteFile(filepath.Join(dir, manifestName), []byte("damaged"), 0600))
	if got := checkpointNames(t, dir); !slices.Equal(got, before) {
		t.Fatalf("recovery mutated ambiguous files: %v, want %v", got, before)
	}
	requirePublicationOK(t, os.WriteFile(filepath.Join(dir, manifestName), good, 0600))
	recovered, err := l.Publish()
	requirePublicationOK(t, err)
	if recovered == nil || len(recovered.Segments) != 1 {
		t.Fatalf("retry did not publish: %+v", recovered)
	}
	if len(checkpointNames(t, dir)) != len(before)-1 {
		t.Fatalf("retry retained losing attempt files: %v", checkpointNames(t, dir))
	}
}

func TestRecoverBeforeManifestAttempt(t *testing.T) {
	for _, phase := range []string{"staged only", "intent durable", "final and staging links", "final link only"} {
		t.Run(phase, func(t *testing.T) {
			dir, _, _ := fixture(t)
			l, err := OpenLegacy(dir)
			requirePublicationOK(t, err)
			closeOnCleanup(t, l)

			name := segmentPrefix + "44444444444444444444444444444444"
			staged := stageDir + "/" + name
			requirePublicationOK(t, l.root.Mkdir(stageDir, 0700))
			a := publicationAttempt{pub: l.publication(), ctx: t.Context()}
			_, blocks := blockSeeds(t)
			requirePublicationOK(t, a.stageSegment(name, blocks[0], digest(blocks[0])))
			if phase != "staged only" {
				requirePublicationOK(t, a.recordIntent())
			}
			if phase == "final and staging links" || phase == "final link only" {
				requirePublicationOK(t, l.root.Link(staged, name))
			}
			if phase == "final link only" {
				requirePublicationOK(t, l.root.Remove(staged))
			}
			requirePublicationOK(t, l.Close())

			l, err = OpenLegacy(dir)
			requirePublicationOK(t, err)
			closeOnCleanup(t, l)
			if _, err := l.OpenPublished(); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("first publication had no authority: %v", err)
			}
			for _, path := range []string{stageDir, name} {
				if _, err := os.Lstat(filepath.Join(dir, path)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("orphan %q survived recovery: %v", path, err)
				}
			}
			if _, err := l.Publish(); err != nil {
				t.Fatalf("retry after recovery: %v", err)
			}
		})
	}
}

func TestRecoverInterruptedStagedReplacement(t *testing.T) {
	for _, tc := range []struct {
		step  stagingStep
		files int
	}{
		{writeSuccessor, 2},
		{closeSuccessor, 2},
		{unlinkPrior, 2},
		{syncRetirement, 1},
	} {
		t.Run(string(tc.step), func(t *testing.T) {
			path, l, wal := capturedPublicationFixture(t)
			old, err := l.Publish()
			requirePublicationOK(t, err)
			const unrelated = segmentPrefix + "66666666666666666666666666666666"
			requirePublicationOK(t, l.publication().writeNew(unrelated, []byte("unrelated")))
			before := checkpointNames(t, path)
			authority := readFixture(t, path, manifestName)
			original := readFixture(t, path, old.Segments[0].Name)

			ctx := context.WithValue(t.Context(), stagingFaultKey{}, func(step stagingStep) error {
				if step == tc.step {
					return syscall.EIO
				}
				return nil
			})
			rows := []BlockRow{{Row: store.Row{TS: time.Unix(20, 0).UTC(), Line: "tie"}}}
			successor := append(slices.Clone(rows), BlockRow{Row: rows[0].Row, Ordinal: 1})
			// Leave the stage exactly as the interrupted operation left it.
			err = withTestPublication(l, func(p *publication) error {
				if err := p.root.Mkdir(stageDir, 0700); err != nil {
					return err
				}
				a := publicationAttempt{pub: p, ctx: ctx}
				if err := a.stageBlock(rows); err != nil {
					return err
				}
				return a.replaceBlock(0, successor)
			})
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("expected interruption at %s: %v", tc.step, err)
			}
			if got := checkpointNames(t, filepath.Join(path, stageDir)); len(got) != tc.files {
				t.Fatalf("staged entries = %v, want %d", got, tc.files)
			}
			requirePublicationOK(t, l.Close())
			l, err = OpenLegacy(path)
			requirePublicationOK(t, err)
			closeOnCleanup(t, l)
			publisher := NewPublisher(l.root, l.dir, NewBudget(Limits{StagingBytes: MaxBlockSize}))
			for range 2 {
				requirePublicationOK(t, publisher.RecoverCaptured(func() (*Manifest, error) {
					return old, l.publication().checkPrevious(t.Context(), old)
				}))
				if !slices.Equal(before, checkpointNames(t, path)) ||
					!bytes.Equal(authority, readFixture(t, path, manifestName)) ||
					!bytes.Equal(original, readFixture(t, path, old.Segments[0].Name)) ||
					string(readFixture(t, path, unrelated)) != "unrelated" {
					t.Fatal("recovery changed a file outside its abandoned stage")
				}
			}
			requirePublicationOK(t, CheckPublication(l.dir))
			m, err := publisher.PublishCaptured(t.Context(), bytes.NewReader(wal), int64(len(wal)), old, old.NextOrdinal, digest(wal))
			requirePublicationOK(t, err)
			if m == nil || !slices.Equal(m.Segments, old.Segments) {
				t.Fatal("retry did not preserve the previous checkpoint")
			}
		})
	}
}

func capturedPublicationFixture(t *testing.T) (string, *Legacy, []byte) {
	t.Helper()
	dir, wal, _ := fixture(t)
	l, err := OpenLegacy(dir)
	requirePublicationOK(t, err)
	closeOnCleanup(t, l)
	return dir, l, wal[:l.CommittedOffset()]
}

func recoverCaptured(l *Legacy) error {
	return RecoverCaptured(l.root, l.dir, func() (*Manifest, error) {
		p, err := l.readPublished(false)
		if err != nil {
			return nil, err
		}
		m := p.Manifest()
		return &m, nil
	})
}

func withTestPublication(l *Legacy, run func(*publication) error) (err error) {
	lock, err := LockPublication(l.dir, true)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	return run(l.publication())
}

type capturedSuffix struct {
	io.ReaderAt
	start  int64
	before func()
}

func (r capturedSuffix) ReadAt(b []byte, offset int64) (int, error) {
	if offset < r.start {
		return 0, errors.New("omitted restore prefix was read")
	}
	r.before()
	return r.ReaderAt.ReadAt(b, offset)
}

func TestPublishCapturedExtendsPinnedGenerationAndSerializesPublishers(t *testing.T) {
	dir, l, wal := capturedPublicationFixture(t)
	ctx := t.Context()
	start := int64(len(wal))
	old, err := PublishCaptured(ctx, l.root, l.dir, bytes.NewReader(wal), start, nil, 2, digest(wal))
	requirePublicationOK(t, err)
	oldAuthority := readFixture(t, dir, manifestName)
	full := append(bytes.Clone(wal), wal[len("BOGWAL01"):]...)
	end, sum := int64(len(full)), digest(full)

	scanning := make(chan struct{})
	resume := make(chan struct{})
	result := make(chan error, 1)
	var once sync.Once
	input := capturedSuffix{
		ReaderAt: bytes.NewReader(full),
		start:    start,
		before:   func() { once.Do(func() { close(scanning); <-resume }) },
	}
	go func() {
		_, err := PublishCaptured(ctx, l.root, l.dir, input, end, old, 4, sum)
		result <- err
	}()
	var contention, pending error
	var authority []byte
	select {
	case <-scanning:
		_, contention = PublishCaptured(ctx, l.root, l.dir, bytes.NewReader(full), end, old, 4, sum)
		pending = CheckPublication(l.dir)
		authority, err = l.root.ReadFile(manifestName)
		close(resume)
		requirePublicationOK(t, <-result)
	case err := <-result:
		t.Fatalf("publisher returned before scanning: %v", err)
	}
	requirePublicationOK(t, err)
	if !errors.Is(contention, syscall.EWOULDBLOCK) || !errors.Is(pending, ErrPublicationUncertain) {
		t.Fatalf("competing publish: %v, pending check: %v", contention, pending)
	}
	if !bytes.Equal(authority, oldAuthority) {
		t.Fatal("authority changed before scan finished")
	}
	requirePublicationOK(t, CheckPublication(l.dir))

	// Reopen against the full committed prefix to check v1 compatibility.
	requirePublicationOK(t, l.Close())
	requirePublicationOK(t, os.WriteFile(filepath.Join(dir, "bog.wal"), full, 0600))
	marker := make([]byte, 24)
	copy(marker, "BOGCMT01")
	binary.LittleEndian.PutUint64(marker[8:16], uint64(end))
	binary.LittleEndian.PutUint64(marker[16:24], ^uint64(end))
	requirePublicationOK(t, os.WriteFile(filepath.Join(dir, "bog.commit"), marker, 0600))
	l, err = OpenLegacy(dir)
	requirePublicationOK(t, err)
	closeOnCleanup(t, l)
	p, err := l.OpenPublished()
	requirePublicationOK(t, err)
	m := p.Manifest()
	if m.CoveredOffset != end || m.SourceSHA256 != sum || m.NextOrdinal != 4 ||
		len(m.Segments) != 2 || m.Segments[0] != old.Segments[0] {
		t.Fatalf("unexpected successor: %+v", m)
	}
	var rows []BlockRow
	requirePublicationOK(t, p.Scan(func(row BlockRow) error {
		rows = append(rows, row)
		return nil
	}))
	if len(rows) != 4 {
		t.Fatalf("wrong row count: %d", len(rows))
	}
	for i, row := range rows {
		if row.Ordinal != uint64(i) || row.Row != rows[0].Row {
			t.Fatalf("wrong rows or ordinals: %+v", rows)
		}
	}
}

func TestPublishCapturedRejectsInvalidPrefix(t *testing.T) {
	dir, l, wal := capturedPublicationFixture(t)
	old, err := l.Publish()
	requirePublicationOK(t, err)
	before := checkpointNames(t, dir)
	original := readFixture(t, dir, manifestName)
	full := append(bytes.Clone(wal), wal[len("BOGWAL01"):]...)
	start, end := len(wal), int64(len(full))
	badHeader, badChecksum := bytes.Clone(full), bytes.Clone(full)
	badHeader[start] ^= 1
	badChecksum[start+16] ^= 1
	for _, tt := range []struct {
		name  string
		input []byte
		end   int64
		next  uint64
		sum   string
		want  error
	}{
		{"malformed digest", full, end, 4, "bad", ErrIntegrity},
		{"short frame header", full, int64(start + 15), 4, digest(full), ErrIntegrity},
		{"short frame payload", full, end - 1, 4, digest(full), ErrIntegrity},
		{"header checksum", badHeader, end, 4, digest(full), ErrIntegrity},
		{"payload checksum", badChecksum, end, 4, digest(full), ErrIntegrity},
		{"wrong ordinal", full, end, 3, digest(full), ErrIntegrity},
		{"truncated reader", full[:len(full)-1], end, 4, digest(full), io.ErrUnexpectedEOF},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m, err := PublishCaptured(t.Context(), l.root, l.dir, bytes.NewReader(tt.input), tt.end, old, tt.next, tt.sum)
			if m != nil || !errors.Is(err, tt.want) {
				t.Fatalf("publish = %v, %v, want %v", m, err, tt.want)
			}
			if got := checkpointNames(t, dir); !slices.Equal(got, before) {
				t.Fatalf("failed scan changed directory: got %v, want %v", got, before)
			}
			if !bytes.Equal(readFixture(t, dir, manifestName), original) {
				t.Fatal("failed scan changed authoritative manifest")
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := PublishCaptured(ctx, l.root, l.dir, bytes.NewReader(full), end, old, 4, digest(full)); !errors.Is(err, context.Canceled) {
		t.Fatalf("publish with cancelled context: %v", err)
	}
}

func TestRecoverCapturedRejectsMissingReferencedSegment(t *testing.T) {
	dir, l, wal := capturedPublicationFixture(t)
	m, err := l.Publish()
	requirePublicationOK(t, err)
	original := readFixture(t, dir, manifestName)
	segment := m.Segments[0].Name
	requirePublicationOK(t, l.root.Remove(segment))
	requirePublicationOK(t, l.root.Mkdir(stageDir, 0700))
	if err := recoverCaptured(l); !errors.Is(err, ErrIntegrity) || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recover missing referenced segment: %v", err)
	}
	// A wrapped ENOENT from the authority callback cannot be mistaken for an
	// absent manifest when the manifest itself still exists.
	err = RecoverCaptured(l.root, l.dir, func() (*Manifest, error) {
		_, err := l.root.Lstat(segment)
		return nil, err
	})
	if !errors.Is(err, ErrIntegrity) || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recover missing referenced segment reported as absent manifest: %v", err)
	}
	if _, err := l.root.Lstat(stageDir); err != nil {
		t.Fatalf("failed recovery discarded staging: %v", err)
	}
	requirePublicationOK(t, l.root.Remove(stageDir))
	if _, err := PublishCaptured(t.Context(), l.root, l.dir, bytes.NewReader(wal), int64(len(wal)), m, 2, digest(wal)); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("publish over missing referenced segment: %v", err)
	}
	if got := readFixture(t, dir, manifestName); !bytes.Equal(got, original) {
		t.Fatal("failed recovery changed manifest")
	}
}
