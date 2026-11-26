//go:build !linux

package main

import "context"

func measure(_ context.Context, _ options, _, _ string, f fixture) result {
	return result{Fixture: f, Phase: "provision", Status: "fail", Workload: "fail",
		Detail: "capacity measurement requires Linux and cgroup v2", Cleanup: cleanupProof{Status: "not_started"}}
}
