# ADR 0016 — Bounded checkpoint staging reuse

**Status** — Proposed for retained-history publication.

## Context

At the v1 segment ceiling, two originals with 30% payload each can be
coalesced to make room for an 80% suffix flush. A following 25% flush
fits in the coalesced output. A packer that considers only originals
will refuse the second flush if the other segments are full.

## Decision

Keep the existing original-segment packing paths, including the
four-input repack window. If those paths cannot place a flush, plan
placement of its rows across gaps in unused originals and eligible
outputs from the current publication attempt. In this fallback, sort
rows by descending encoded size and open another gap only when the
currently selected gaps cannot hold the next row. Read one selected
segment at a time. Sort each finished block by timestamp and append
ordinal, preserving duplicate rows and the order of timestamp ties.

The publication attempt owns the staged files, their reservations, and
their stable slots. The packer can read a staged block or ask the
attempt to replace it. A staged read checks file identity, exact size,
SHA-256, v1 block checksums, and row order and ordinals. Originals
remain immutable for pinned readers. A slot can be rewritten once,
so an attempt with at most 1,024 live staged blocks writes at most
1,024 replacements. The original repack window holds at most four
decoded inputs plus the current suffix run. Slack placement keeps
metadata for at most 1,024 eligible gaps and decodes one input at a
time, reusing a single encoded input buffer.

Charge the full replacement before encoding or writing it. The old
and new files overlap for one replacement at a time, before any
manifest or intent is staged. Sync and close the replacement, unlink
the superseded file, and sync staging before releasing its charge.
Admission can fail even when the final files would fit the budget.
The local packing heuristic and rewrite limit can also refuse a
layout with enough aggregate free bytes.

## Publication and recovery

Keep the v1 manifest, segment, and intent formats. The intent names
the live staged outputs after all rewrites. A crash before intent
creation can leave the overlap file in the private staging directory,
where the existing bounded cleanup reclaims it. Preserve the order of
durable intent, segment installation and sync, manifest rename and
directory sync. On an uncertain rename or sync, hold the remaining
charges until recovery validates and syncs the authoritative generation
and reclaims losing files.

The existing Publisher API remains the boundary for the #53 retained
runtime. The WAL owner supplies the pinned directory and committed
cutoff, and history manages retired generation lifetimes. [Retained-history
runtime cutover](0015-retained-history-runtime-cutover.md)
