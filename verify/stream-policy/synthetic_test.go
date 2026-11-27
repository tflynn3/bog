package streampolicy

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sort"
	"strings"
)

// syntheticTarget models a disposable target over serialized committed frames.
// Query state is rebuilt on restart instead of preserving its in-memory index.
type syntheticTarget struct {
	fixture   Fixture
	committed []byte
	frames    []syntheticFrame
	cursors   map[string]syntheticCursor
	artifacts map[string][]byte
	next      int
	revision  uint64
	policies  map[Scope]StreamPolicy
}

type syntheticFrame struct {
	Scope            Scope
	Principal        string
	Key              string
	Fingerprint      []byte
	RedactionVersion string
	Outcome          Response
	Rows             []Row
}

type syntheticCursor struct {
	request        QueryRequest
	scope          Scope
	maskingVersion string
	rows           []Row
}

func newSynthetic() *syntheticTarget {
	return &syntheticTarget{
		fixture: ReferenceFixture(), cursors: make(map[string]syntheticCursor),
		artifacts: make(map[string][]byte),
	}
}

func (s *syntheticTarget) adapters() Adapters { return Adapters{s, s, s, s} }

func (s *syntheticTarget) revisionAdapters() RevisionAdapters {
	return RevisionAdapters{s.adapters(), s, s, s}
}

func (s *syntheticTarget) ApplyPolicy(ctx context.Context, revision PolicyRevision) (PolicyAck, error) {
	if err := ctx.Err(); err != nil {
		return PolicyAck{}, err
	}
	if revision.Number != s.revision+1 || len(revision.Streams) != len(s.fixture.Streams) {
		return PolicyAck{}, errors.New("invalid fixture policy revision")
	}
	s.revision = revision.Number
	s.fixture.Grants = revision.Grants
	s.policies = make(map[Scope]StreamPolicy, len(revision.Streams))
	for _, policy := range revision.Streams {
		s.policies[policy.Scope] = policy
	}
	return PolicyAck{Complete: true, Number: s.revision}, nil
}

func (s *syntheticTarget) streamPolicy(stream Stream) StreamPolicy {
	if policy, ok := s.policies[stream.Scope]; ok {
		return policy
	}
	return StreamPolicy{RedactionVersion: stream.PolicyVersion, Redactions: s.fixture.Replacements, FailOn: s.fixture.FailOn}
}

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
				return frame.Outcome, nil
			}
			return response(http.StatusConflict, "retry conflict\n"), nil
		}
	}

	policy := s.streamPolicy(stream)
	outcome := response(http.StatusNoContent, "")
	outcome.Headers = map[string][]string{"Bog-Disposition": {fmt.Sprintf("synthetic-commit-%04d", len(s.frames)+1)}}
	frame := syntheticFrame{Scope: stream.Scope, Principal: request.Principal, Key: request.Key, Fingerprint: digest,
		RedactionVersion: policy.RedactionVersion, Outcome: outcome}
	if err := decodeFixture(strings.NewReader(request.Body), func(row Row) error {
		if strings.Contains(row.Line, policy.FailOn) {
			return errRedactorUnavailable
		}
		for _, rule := range policy.Redactions {
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
	s.committed = append(s.committed, encoded.Bytes()...)
	s.frames = append(s.frames, frame)
	return outcome, nil
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
	policy := s.streamPolicy(stream)
	var rows []Row
	if request.Cursor == "" {
		for _, record := range s.records() {
			if record.Scope != stream.Scope || request.Start > record.Row.Timestamp || record.Row.Timestamp >= request.End {
				continue
			}
			row := record.Row
			for _, rule := range policy.Masks {
				row.Line = strings.ReplaceAll(row.Line, rule.Forbidden, rule.Replacement)
			}
			if strings.Contains(row.Line, request.Contains) {
				rows = append(rows, row)
			}
		}
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].Timestamp < rows[j].Timestamp })
	} else {
		cursor, ok := s.cursors[request.Cursor]
		if !ok || cursor.scope != stream.Scope || cursor.request.Principal != request.Principal ||
			cursor.request.Contains != request.Contains || cursor.request.Start != request.Start || cursor.request.End != request.End ||
			cursor.maskingVersion != policy.MaskingVersion {
			return Page{Response: response(http.StatusBadRequest, "invalid cursor\n")}, nil
		}
		rows = cursor.rows
	}

	result := Page{Response: response(http.StatusOK, "")}
	if len(rows) > request.Limit {
		s.next++
		result.NextCursor = fmt.Sprintf("synthetic-cursor-%04d", s.next)
		s.cursors[result.NextCursor] = syntheticCursor{request, stream.Scope, policy.MaskingVersion, rows[request.Limit:]}
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

func (s *syntheticTarget) ObserveRetryRecovery(ctx context.Context) (RetryRecovery, error) {
	if err := ctx.Err(); err != nil {
		return RetryRecovery{}, err
	}
	result := RetryRecovery{Complete: true, Commits: []RetryCommit{}}
	err := decodeFixture(bytes.NewReader(s.committed), func(frame syntheticFrame) error {
		unit := RetryCommit{
			Scope: frame.Scope, Principal: frame.Principal, Key: frame.Key,
			RedactionVersion: frame.RedactionVersion, Outcome: frame.Outcome,
		}
		for _, row := range frame.Rows {
			unit.Records = append(unit.Records, Record{frame.Scope, row})
		}
		result.Commits = append(result.Commits, unit)
		return nil
	})
	return result, err
}

type syntheticImage struct {
	Manifest Manifest
	Records  []Record
}

func (s *syntheticTarget) CreateExport(ctx context.Context, target Target) (ExportStart, error) {
	return s.createExport(ctx, target, nil)
}

func (s *syntheticTarget) ObserveExportPublication(ctx context.Context, target Target, beforePublish func(context.Context) error) (ExportPublication, error) {
	existing := make(map[string]bool, len(s.artifacts))
	for id := range s.artifacts {
		existing[id] = true
	}
	result, err := s.createExport(ctx, target, beforePublish)
	if err != nil {
		return ExportPublication{}, err
	}
	publication := ExportPublication{Complete: true, ExportStart: result}
	for id := range s.artifacts {
		if !existing[id] {
			publication.Published = append(publication.Published, id)
		}
	}
	slices.Sort(publication.Published)
	return publication, nil
}

func (s *syntheticTarget) createExport(ctx context.Context, target Target, beforePublish func(context.Context) error) (ExportStart, error) {
	stream, ok := s.authorize(target, func(g Grant) bool { return g.Export })
	if !ok {
		return ExportStart{Response: forbiddenResponse()}, nil
	}
	image := syntheticImage{Manifest: Manifest{stream.Scope, "fixture-v1", []string{stream.PolicyVersion}}}
	for _, record := range s.records() {
		if record.Scope == stream.Scope {
			image.Records = append(image.Records, record)
		}
	}
	data, err := json.Marshal(image)
	if err != nil {
		return ExportStart{}, err
	}
	if beforePublish != nil {
		if err := beforePublish(ctx); err != nil {
			return ExportStart{}, err
		}
		if _, ok := s.authorize(target, func(g Grant) bool { return g.Export }); !ok {
			return ExportStart{Response: forbiddenResponse()}, nil
		}
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
