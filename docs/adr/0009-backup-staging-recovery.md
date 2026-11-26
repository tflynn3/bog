# ADR 0009 — Destination-scoped backup staging recovery

**Status** — Accepted for issue #64.

## Context

Export and Restore use a shared publisher on a trusted, service-owned local
filesystem. An interrupted writer can leave a complete or partial image in a
sibling directory. A staging prefix alone carries no evidence about the
destination, directory identity, or liveness of the writer. Restore and
export must preserve the committed v1 image layout and the file, directory,
and publication sync order.

## Decision

Scope a claim to the identity of the opened parent directory and the exact
destination basename. The claim names use the SHA-256 of that basename in
the reserved `.bog-backup-v1-` namespace. All coordination and mutation are
relative to the pinned parent. Symlink aliases for the parent share the same
lock domain. The caller keeps unrelated writers out of the destination and
internal staging namespace.

The publisher holds an exclusive advisory lock on the parent during claim
inspection, provisioning, recovery, and final publication. It releases
that guard during image I/O and holds an exclusive lock on the private
stage directory throughout the attempt. Another destination can copy its
image concurrently. A competing writer for the same destination waits
according to its context. Process exit releases the locks, including on
SIGKILL. Lock ownership uses open filesystem handles and needs no PID,
timestamp, or timeout to classify an abandoned attempt.

1. Atomically create an intent symlink containing a random 128-bit nonce in
   canonical hexadecimal and sync the parent. The destination hash and nonce
   uniquely name the fresh private stage. Check that the name is absent before
   claiming it.
2. Create the stage and atomically record its identity in a second symlink.
   Its fixed-size payload encodes the device and inode as little-endian
   64-bit integers and a little-endian CRC32C over the stage basename and
   identity bytes. The publisher reads both symlinks as metadata. It acquires
   the stage lock and syncs the parent before copying any image data.
3. On retry, validate both records and compare the no-follow directory identity
   to the recorded device and inode. Check the private mode, effective owner,
   and parent filesystem. Reclaim an identified stage only with the stage
   lock held. A valid intent without an identity record represents bootstrap
   and allows removal of the reserved directory only if it is empty.
   Malformed records, conflicting entries, and identity mismatches fail
   closed. No prefix scan or cleanup of legacy staging is performed.
4. Sync and close staged image files and sync and close the staging root
   before publication. Under the parent guard, recheck cancellation,
   destination eligibility, and stage identity. Linux atomically renames
   the verified directory over the absent or empty destination using the
   pinned parent descriptor. The portable fallback retains the existing
   empty-placeholder removal immediately before rename.
5. Sync the parent after publication. Make the absence of the stage name
   durable before removing its records, then unlink and sync the identity
   record before unlinking and syncing the intent. The same order applies to
   rollback and recovery. The metadata lives outside the image throughout
   publication.

An I/O error with an uncertain rename outcome, or a post-rename durability
error, requires reopening and verifying the destination. Preserve recovery
records across an uncertain rename or publication sync. A later publisher
can settle those records under the parent guard and rejects a populated
destination. No retry deletes or overwrites a completed image. A crash
during cleanup can leave a valid identity, an intent alone, or no records.

The protocol assumes cooperating Bog publishers and a service-owned,
case-sensitive local filesystem for the Linux deployment. A checksum
detects malformed metadata and does not authenticate records from a writer
with access to the parent. Older prefix-only stages require operator
inspection. Successful publication removes the staging entries owned by the attempt.

## Verification

Portable tests exercise ownership validation, cancellation while a publisher
holds staging, concurrent Export and Restore, and injected rename and sync
errors. Linux subprocess tests kill each operation after staging and retry
the same destination repeatedly. A separate interruption after rename
exercises cleanup beside an intact image. The Linux placeholder test observes
one rename event and verifies the installed image at the parent sync boundary.
