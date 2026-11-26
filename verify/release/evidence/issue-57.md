# Delegated cgroup v2 verification for #57

The amd64 run used Ubuntu 24.04.4, Linux 6.8.0-101-generic and Go 1.27.0.
Both kernel regression tests passed in three consecutive runs under a
systemd service with `Delegate=memory io` and `DelegateSubgroup=runner`.
`TestDelegatedSubtreeTeardown` verified that the candidate had exited while
a process remained in a nested cgroup and the parent's `cgroup.procs` was
empty. Teardown killed the descendant and removed the subtree.
`TestDelegatedFixtureCancellation` canceled a running fixture during startup
and verified successful cleanup with a failed workload. Both tests also
passed three consecutive times with the race detector on Linux arm64.

The [amd64 base capacity report](issue-57-base-capacity.json) was produced
using a clean build of `efb5f059f5ad7ab04e3321f33c4c7a4b2f7ea605` with
`GOAMD64=v1`, `CGO_ENABLED=0`, `-trimpath`, and `-buildvcs=true`.
Both live fixture attempts reached readiness, received HTTP 404 from the
compaction control endpoint, and proved candidate reaping, subtree
emptiness, and subtree removal. The producer exited 1 and a final scan
of the delegation found zero disposable capacity cgroups.

The same binary completed both legacy fixtures. The restart HTTP walks
matched all 10,240 and 34,816 acknowledged rows respectively, and both
fixtures proved cleanup. The live qualification case was skipped in this
mode. [amd64 legacy baseline report](issue-57-base-legacy.json)

The full release gate is pending a revision with #53 composed. The base
report records cleanup on the capability failure path.
