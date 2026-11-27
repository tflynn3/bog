package streampolicy

import (
	"bytes"
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
)

// syntheticTarget models a disposable target over serialized committed frames.
// Query state is rebuilt on restart instead of preserving its in-memory index.
type syntheticTarget struct {
	fixture   Fixture
	appendMu  sync.Mutex
	masking   map[Scope]MaskingChange
	redaction map[Scope]RedactionChange
	committed []byte
	frames    []syntheticFrame
	cursors   map[string]syntheticCursor
	artifacts map[string][]byte
	next      int
}

type syntheticFrame struct {
	Scope         Scope
	Principal     string
	Key           string
	Fingerprint   []byte
	PolicyVersion string
	Status        int
	Rows          []Row
}

type syntheticCursor struct {
	request  QueryRequest
	scope    Scope
	revision string
	rows     []Row
}

func newSynthetic() *syntheticTarget {
	return &syntheticTarget{
		fixture: ReferenceFixture(), cursors: make(map[string]syntheticCursor),
		artifacts: make(map[string][]byte), masking: make(map[Scope]MaskingChange), redaction: make(map[Scope]RedactionChange),
	}
}

func (s *syntheticTarget) adapters() Adapters { return Adapters{s, s, s, s} }

func (s *syntheticTarget) authorize(target Target, permission func(Grant) bool) (Stream, bool) {
	for _, grant := range s.fixture.Grants {
		if grant.Target == target && permission(grant) {
			for _, stream := range s.fixture.Streams {
				if stream.Selector == target.Selector {
					return stream, true
				}
			}
		}
	}
	return Stream{}, false
}

func response(status int, body string) Response {
	return Response{Complete: true, Status: status, Body: []byte(body)}
}

func forbiddenResponse() Response { return response(http.StatusForbidden, "forbidden\n") }

func (s *syntheticTarget) Ingest(_ context.Context, request IngestRequest) (Response, error) {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()
	return s.submit(request, s.publish)
}

func (s *syntheticTarget) submit(request IngestRequest, onPrepared func(syntheticFrame, []byte)) (Response, error) {
	stream, ok := s.authorize(request.Target, func(g Grant) bool { return g.Ingest })
	if !ok {
		return forbiddenResponse(), nil
	}
	mac := hmac.New(sha256.New, []byte("disposable test fixture signing key"))
	if _, err := io.WriteString(mac, request.Body); err != nil {
		return Response{}, err
	}
	digest := mac.Sum(nil)
	for _, frame := range s.frames {
		if frame.Scope == stream.Scope && frame.Principal == request.Principal && frame.Key == request.Key {
			if hmac.Equal(frame.Fingerprint, digest) {
				return response(frame.Status, ""), nil
			}
			return response(http.StatusConflict, "retry conflict\n"), nil
		}
	}

	policy, found := s.redaction[stream.Scope]
	if !found {
		policy = RedactionChange{stream.Scope, stream.PolicyVersion, s.fixture.Replacements}
	}
	frame := syntheticFrame{Scope: stream.Scope, Principal: request.Principal, Key: request.Key,
		Fingerprint: digest, PolicyVersion: policy.Version, Status: http.StatusNoContent}
	if err := decodeFixture(strings.NewReader(request.Body), func(row Row) error {
		if strings.Contains(row.Line, s.fixture.FailOn) {
			return errRedactorUnavailable
		}
		for _, rule := range policy.Replacements {
			row.Line = strings.ReplaceAll(row.Line, rule.Forbidden, rule.Replacement)
		}
		frame.Rows = append(frame.Rows, row)
		return nil
	}); err != nil {
		if errors.Is(err, errRedactorUnavailable) {
			return response(http.StatusServiceUnavailable, "redaction unavailable\n"), nil
		}
		return Response{}, err
	}
	var encoded bytes.Buffer
	if err := json.NewEncoder(&encoded).Encode(frame); err != nil {
		return Response{}, err
	}
	onPrepared(frame, encoded.Bytes())
	return response(frame.Status, ""), nil
}

func (s *syntheticTarget) publish(frame syntheticFrame, encoded []byte) {
	s.committed = append(s.committed, encoded...)
	s.frames = append(s.frames, frame)
}

var errRedactorUnavailable = errors.New("fixture redactor unavailable")

func decodeFixture[T any](input io.Reader, accept func(T) error) error {
	decoder := json.NewDecoder(input)
	for {
		var value T
		err := decoder.Decode(&value)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := accept(value); err != nil {
			return err
		}
	}
}

func (s *syntheticTarget) records() []Record {
	var records []Record
	for _, frame := range s.frames {
		for _, row := range frame.Rows {
			records = append(records, Record{frame.Scope, row})
		}
	}
	return records
}

func (s *syntheticTarget) Query(_ context.Context, request QueryRequest) (Page, error) {
	stream, ok := s.authorize(request.Target, func(g Grant) bool { return g.Query })
	if !ok {
		return Page{Response: forbiddenResponse()}, nil
	}
	var rows []Row
	mask := s.masking[stream.Scope]
	if request.Cursor == "" {
		for _, record := range s.records() {
			if record.Scope != stream.Scope || record.Row.Timestamp < request.Start || request.End <= record.Row.Timestamp {
				continue
			}
			row := record.Row
			if mask.Replacement.Forbidden != "" {
				row.Line = strings.ReplaceAll(row.Line, mask.Replacement.Forbidden, mask.Replacement.Replacement)
			}
			if strings.Contains(row.Line, request.Contains) {
				rows = append(rows, row)
			}
		}
		slices.SortStableFunc(rows, func(a, b Row) int { return cmp.Compare(a.Timestamp, b.Timestamp) })
	} else {
		cursor, ok := s.cursors[request.Cursor]
		if !ok || cursor.revision != mask.Revision || cursor.scope != stream.Scope || cursor.request.Principal != request.Principal ||
			cursor.request.Contains != request.Contains || cursor.request.Start != request.Start || cursor.request.End != request.End {
			return Page{Response: response(http.StatusBadRequest, "invalid cursor\n")}, nil
		}
		rows = cursor.rows
	}

	result := Page{Response: response(http.StatusOK, "")}
	if len(rows) > request.Limit {
		s.next++
		result.NextCursor = fmt.Sprintf("synthetic-cursor-%04d", s.next)
		s.cursors[result.NextCursor] = syntheticCursor{request, stream.Scope, mask.Revision, rows[request.Limit:]}
		result.Headers = map[string][]string{"Bog-Next-Cursor": {result.NextCursor}}
		rows = rows[:request.Limit]
	}
	var encoded bytes.Buffer
	for _, row := range rows {
		if err := json.NewEncoder(&encoded).Encode(row); err != nil {
			return Page{}, err
		}
	}
	result.Body, result.Rows = encoded.Bytes(), slices.Clone(rows)
	return result, nil
}

func (s *syntheticTarget) RestartAndReplay(context.Context) (Committed, error) {
	var recovered []syntheticFrame
	err := decodeFixture(bytes.NewReader(s.committed), func(frame syntheticFrame) error {
		recovered = append(recovered, frame)
		return nil
	})
	if err != nil {
		return Committed{}, err
	}
	s.frames = recovered
	clear(s.cursors)
	return Committed{Complete: true, Bytes: bytes.Clone(s.committed), Records: s.records()}, nil
}

type syntheticImage struct {
	Manifest Manifest
	Records  []Record
}

func (s *syntheticTarget) CreateExport(_ context.Context, target Target) (ExportStart, error) {
	stream, ok := s.authorize(target, func(g Grant) bool { return g.Export })
	if !ok {
		return ExportStart{Response: forbiddenResponse()}, nil
	}
	image := syntheticImage{Manifest: Manifest{Scope: stream.Scope, Version: "fixture-v1"}}
	for _, frame := range s.frames {
		if frame.Scope == stream.Scope && !slices.Contains(image.Manifest.PolicyVersions, frame.PolicyVersion) {
			image.Manifest.PolicyVersions = append(image.Manifest.PolicyVersions, frame.PolicyVersion)
		}
	}
	for _, record := range s.records() {
		if record.Scope == stream.Scope {
			image.Records = append(image.Records, record)
		}
	}
	data, err := json.Marshal(image)
	if err != nil {
		return ExportStart{}, err
	}
	s.next++
	id := fmt.Sprintf("synthetic-artifact-%04d", s.next)
	tag, err := fixtureTag(data)
	if err != nil {
		return ExportStart{}, err
	}
	s.artifacts[id] = append(data, tag...)
	return ExportStart{Response: response(http.StatusOK, ""), ArtifactID: id}, nil
}

func (s *syntheticTarget) FetchExport(_ context.Context, target Target, id string) (Artifact, error) {
	stream, ok := s.authorize(target, func(g Grant) bool { return g.Export })
	data, found := s.artifacts[id]
	if !ok || !found {
		return Artifact{Response: forbiddenResponse()}, nil
	}
	if len(data) < sha256.Size {
		return Artifact{}, errors.New("truncated fixture artifact")
	}
	payload, tag := data[:len(data)-sha256.Size], data[len(data)-sha256.Size:]
	wantTag, err := fixtureTag(payload)
	if err != nil {
		return Artifact{}, err
	}
	if !hmac.Equal(wantTag, tag) {
		return Artifact{}, errors.New("invalid fixture artifact authentication")
	}
	var image syntheticImage
	if err := json.Unmarshal(payload, &image); err != nil {
		return Artifact{}, err
	}
	if image.Manifest.Scope != stream.Scope {
		return Artifact{Response: forbiddenResponse()}, nil
	}
	return Artifact{
		Response: response(http.StatusOK, ""), Verified: true, Bytes: bytes.Clone(data),
		Records: image.Records, Manifest: image.Manifest,
	}, nil
}

func fixtureTag(data []byte) ([]byte, error) {
	mac := hmac.New(sha256.New, []byte("synthetic export fixture authentication key"))
	if _, err := mac.Write(data); err != nil {
		return nil, err
	}
	return mac.Sum(nil), nil
}
