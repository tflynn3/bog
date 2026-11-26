# HA operator status

The HA operator and replication proxy are not implemented. Do not deploy multiple
independent Bog instances behind a write service as an HA cluster: their WALs
are independent, and a local 204 does not establish a durable quorum commit.

A production implementation needs a persistent, fenced consensus log and a
recovery protocol that applies committed entries in the same order at each
member before acknowledging them. It must account for unknown outcomes when a
local Bog ingest reply is lost: the current Bog API has no idempotency key or
committed-log index, so a proxy cannot safely determine whether replaying an
entry would duplicate it. Lease-only leadership or quorum fan-out does not solve
this problem. Changes to the server or a verified integration with its WAL
publication boundary are required before claiming acknowledged-write failover.
