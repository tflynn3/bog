# ADR 0002 — Bounded history generation pins

Status: Accepted for staged checkpoint history

This decision supersedes the per-segment pinning described in the history README.

## Catalog and reader lifetime

A Catalog owns a reference to the current validated generation. Replace opens
the authoritative checkpoint relative to the pinned data directory under the
shared publication lock. It validates every referenced segment and registers the
generation pin before making the generation current. A failed validation, pin
registration, or cancellation before the swap leaves the prior generation
available. An error releasing resources after the swap accompanies the installed
generation.

AcquireCurrent and Acquire take a Reader reference to the current generation.
Acquire of an unavailable ID returns a typed expiry. A Reader can start scans
after replacement or Catalog.Close. An accepted scan takes its own reference,
which survives Reader.Close. Close stops accepting new operations on that
handle. The generation's Drained channel closes after retirement and release of
all Reader and scan references, including release of the underlying pin.

## Cross-process reclamation

Each activated generation keeps a directory descriptor and one shared flock on
a uniquely named record in that directory. The record has an eight-byte
`BOGPIN01` magic and version, fixed-width v1 segment basenames, and a trailing
SHA-256 of the preceding bytes. It has at most 1024 names. The Catalog
exclusively creates a staging name, sets its mode, locks and writes the file,
then renames it to the active name under the shared publication lock before
activation. Replace requires permission to create these records in the
directory. The prefixes `.bog-history-pin-stage-` and `.bog-history-pin-`,
each followed by 32 lowercase hex digits, reserve the staging and active
names. The records are transient coordination state on the local filesystem.
The validated manifest remains the recovery authority. The kernel
releases the flock on process exit, so an interrupted record can be discarded
once the exclusive publication lock and an exclusive lock on that record can
both be obtained. Staging names can be removed under the exclusive publication
lock because publication lock holders finish registration before activation.
Graceful release removes the record under the publication
lock when available. An exclusive publication operation in progress defers
record removal to reclamation.

A reclaimer holds the exclusive publication lock from authority validation
through directory sync. It protects the union of the authoritative manifest and
all segment names listed by live records. It validates live records before
deleting any segment and fails closed on unreadable or malformed live records.
It considers only previously observed retired files, checks their recorded file
identity, and also takes an exclusive segment lock to honor existing readers
that pin individual segment inodes. Shared names remain protected until every
referencing generation drains and the authoritative manifest has moved on.
All directory operations are relative to the pinned directory. Segment names
and lock records must remain immutable while they are in use, and processes
using reclamation must follow this protocol. A per-segment reclaimer from an
earlier revision must be stopped before enabling generation pins in a directory.

Publication still syncs staged segments, installs their names, syncs the
directory, then syncs and atomically publishes the manifest and syncs the
directory again. History checks publication state before loading or reclaiming.
An uncertain attempt requires publisher recovery. Publisher cleanup is limited
to files owned by an unfinished attempt. History can acquire the generation
only after that attempt has been resolved under the publication lock.
Unknown files are preserved.
Reclamation checks file identity again on retry and syncs directory changes.

A generation retains two descriptors regardless of the segment count. Each
active scan or validation pass opens at most one segment at a time. Each
maintenance operation also uses a constant number of coordination descriptors.
The descriptor bound is therefore two per pinned generation plus a constant
per concurrent operation.

With at most 1024 segments per generation and a maximum encoded block size
of 4 MiB plus 68 bytes, validation reads a manifest capped at 256 KiB and
reuses one encoded block buffer. It uses a bitmap of one bit per minimum-size
row slot in that block and, for shuffled ordinal membership, a 2 MiB ordinal
window that is reused across the generation. Each validated generation retains
file metadata and at most 128 sparse seek points per segment. A scan uses one
cursor and at most one 4 KiB page per overlapping segment, one reusable line
buffer capped by the maximum block, and a separate copy of the delivered row.
The caller budgets any retained results. Reclamation enumerates 64 directory
names at a time and reuses a single pin record buffer capped at 45,096 bytes.
It tracks protected names only for its retired candidates. The retired
candidate table persists in the Catalog until reclamation processes it.

## Live recovery and query cutover

Before making a checkpoint-backed store ready, the storage integration must
capture a validated history generation and a pinned WAL identity with a
validated committed end under the writer's recovery boundary. The checkpoint's
prefix digest and covered offset must match the pinned WAL through a whole
committed frame boundary, or the magic for empty history. The prefix digest
identifies bytes. A WAL generation identity is also required to distinguish
different files or writer epochs with identical prefixes.

Replay only complete committed frames in the half-open interval
`[CoveredOffset, committedEnd)`, starting at `NextOrdinal`. Preserve whole
batches and duplicate rows, and assign append ordinals globally across history
and the WAL suffix. Missing referenced files, committed corruption, or an
unresolved publication stop recovery. Legacy conversion and tail replay must
budget a largest accepted encoded frame and its decoded rows separately from
history's validation, metadata, and scan working sets.

Query admission must capture the history and WAL view together. A continuation
must bind the query and its exclusive last `(timestamp, append ordinal)` key
to that same view, enforcing the half-open time range and exact literal match.
Retaining snapshots, scanning the merged history and tail, and encoding results
must fit the live query work and memory budgets. If the view is unavailable,
expire the continuation. Cutover and live reclamation require agreement on this
recovery point with the WAL owner in #6. This ADR records the handoff for #6.
