# Issue 108 local Linux evidence

The shared Linux amd64 runner built clean qualification revision
`663aef01bc712961a871e9e556f0b1d7ced00234` from the composed working tree.
The candidate binary SHA-256 was
`c4bd5eb3643c4a88d7996c8501267d00258fca4d9728f06fa595879ee8f228d0`.
Both starts used that binary with the default CLI settings, the same
data directory, and a fresh Bog-only cgroup configured before exec with
`memory.max=268435456` and `memory.swap.max=0`.

| Measurement | Initial start | Restart |
| --- | ---: | ---: |
| Health ready | 0.024 s | 0.706 s |
| Peak resident bytes (`VmHWM`) | 88,395,776 | 42,950,656 |
| Increase in `oom` and `oom_kill` | 0 and 0 | 0 and 0 |
| SIGTERM exit status | 0 | 0 |
| SIGTERM to reap | 0.007 s | 0.001 s |

All 128 batches returned 204 within 11.43 seconds. The query after batch
32 and all three final and restarted queries returned the exact ordered
NDJSON rows, using two-second request deadlines. Both lifetimes and
teardown completed in 12.19 seconds. The black-box ENOSPC regression
passed with the same production source.

The shared runner lacks delegated I/O accounting for the separate
128 MiB release capacity fixture. Its workloads reached measurement
validation at both scales. The recorded cgroup peaks were 134,316,032
and 134,352,896 bytes, above that fixture's strict 134,217,728-byte
ceiling. Native Linux arm64 execution was unavailable.
