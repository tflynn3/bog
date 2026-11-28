#!/usr/bin/env python3
"""Qualify the frozen bog/0.2/O1/J1 journey on one native Linux runner."""

import argparse
import concurrent.futures
import datetime
import hashlib
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.parse

CAP = 256 * 1024**2
EPOCH = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
PADDING = "x" * 10240
CHECKS = (0, 16384, 32764)


def stamp(i):
    return (EPOCH + datetime.timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%SZ")


def row(i):
    tag = "NEEDLE" if i % 4 == 1 else "needle"
    return {"ts": stamp(i), "line": f"{tag}-{i}-{PADDING}"}


def request(port, method, target, deadline, body=None):
    start = time.monotonic()
    timeout = min(2.0, deadline - start)
    if timeout <= 0:
        raise TimeoutError(f"{method} {target} started after the phase deadline")
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        headers = {"Content-Type": "application/x-ndjson"} if body is not None else {}
        conn.request(method, target, body, headers)
        with conn.getresponse() as response:
            status = response.status
            media = response.headers.get_content_type()
            data = response.read(64 << 10)
    finally:
        conn.close()
    elapsed = time.monotonic() - start
    if elapsed > timeout:
        raise TimeoutError(f"{method} {target} took {elapsed:.3f}s")
    return status, media, data, elapsed


def check_query(port, k, deadline, started=None):
    params = urllib.parse.urlencode({
        "start": stamp(k), "end": stamp(k + 3), "contains": "needle", "limit": 10,
    })
    if started is not None:
        started.set()
    status, media, data, elapsed = request(port, "GET", "/query?" + params, deadline)
    if status != 200 or media != "application/x-ndjson":
        raise ValueError(f"query {k} returned HTTP {status}, {media}: {data[:200]!r}")
    if [json.loads(line) for line in data.splitlines()] != [row(k), row(k + 2)]:
        raise ValueError(f"query {k} returned incorrect NDJSON rows")
    return {"start_row": k, "seconds": elapsed}


def events(group):
    counters = dict(line.split() for line in (group / "memory.events").read_text().splitlines())
    return {key: int(counters[key]) for key in ("oom", "oom_kill")}


def peak_rss(pid):
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmHWM:"):
            _, n, unit = line.split()
            if unit == "kB":
                return int(n) * 1024
    raise ValueError(f"missing VmHWM for Bog PID {pid}")


def stop(proc):
    before = time.monotonic()
    try:
        os.kill(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    killed = False
    deadline = before + 4
    while True:
        pid, status, usage = os.wait4(proc.pid, os.WNOHANG)
        if pid:
            proc.returncode = os.waitstatus_to_exitcode(status)
            return {
                "exit": proc.returncode, "seconds": time.monotonic() - before,
                "sigkill": killed, "peak_rss_bytes": usage.ru_maxrss * 1024,
            }
        if time.monotonic() >= deadline:
            if killed:
                raise TimeoutError(f"failed to reap Bog PID {proc.pid} after SIGKILL")
            os.kill(proc.pid, signal.SIGKILL)
            killed, deadline = True, time.monotonic() + 2
        time.sleep(0.01)


def run_start(binary, directory, parent, deadline, ingest):
    with socket.socket() as reserve_port:
        reserve_port.bind(("127.0.0.1", 0))
        port = reserve_port.getsockname()[1]
    group = None
    report = {"phase": "ingest" if ingest else "restart", "queries": [], "acknowledgements": 0}
    try:
        cmd = [str(binary), "-data-dir", str(directory), "-addr", f"127.0.0.1:{port}"]
        if parent is not None:
            group = Path(tempfile.mkdtemp(dir=parent, prefix="o1-"))
            for name, value in {"memory.max": str(CAP), "memory.swap.max": "0"}.items():
                (group / name).write_text(value)
                if (group / name).read_text().strip() != value:
                    raise ValueError(f"{name} readback did not match {value}")
            initial_events = events(group)
            cmd = ["/bin/sh", "-ec", 'printf "%s\n" "$$" > "$1/cgroup.procs"; shift; exec "$@"',
                   "cgroup-launch", str(group), *cmd]
        env = {k: v for k, v in os.environ.items()
               if k not in ("BOG_CAPACITY_CONTROL", "GOMEMLIMIT", "GOGC", "GOMAXPROCS")}
        startup = time.monotonic()
        proc = subprocess.Popen(cmd, env=env)
        try:
            health_deadline = min(deadline, startup + 15)
            while time.monotonic() < health_deadline:
                try:
                    status, _, _, elapsed = request(port, "GET", "/healthz", health_deadline)
                    if status == 200:
                        break
                except ConnectionRefusedError:
                    time.sleep(0.01)
            else:
                raise TimeoutError(f"Bog PID {proc.pid} did not reach /healthz within 15s")
            report["health_seconds"] = time.monotonic() - startup
            report["max_request_seconds"] = elapsed
            if group is not None and (group / "cgroup.procs").read_text().split() != [str(proc.pid)]:
                raise ValueError("only Bog must run in its cgroup")
            if ingest:
                start = time.monotonic()
                writes = min(deadline, start + 60)
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as worker:
                    for batch in range(1, 129):
                        offset = (batch - 1) * 256
                        body = "".join(json.dumps(row(i)) + "\n" for i in range(offset, offset + 256)).encode()
                        status, _, response, elapsed = request(port, "POST", "/ingest", writes, body)
                        if status != 204:
                            raise ValueError(f"batch {batch} returned HTTP {status}: {response[:200]!r}")
                        report["acknowledgements"] += 1
                        report["max_request_seconds"] = max(report["max_request_seconds"], elapsed)
                        if batch == 32:
                            started = threading.Event()
                            concurrent_query = worker.submit(check_query, port, 0, deadline, started)
                            if not started.wait(timeout=2):
                                raise TimeoutError("concurrent query did not start after batch 32")
                        if peak_rss(proc.pid) > CAP:
                            raise ValueError(f"Bog peak RSS exceeded 256 MiB during batch {batch}")
                    report["ingest_seconds"] = time.monotonic() - start
                    report["concurrent_query"] = concurrent_query.result(timeout=2)
            for k in CHECKS:
                report["queries"].append(check_query(port, k, deadline))
            timings = [q["seconds"] for q in report["queries"]]
            if ingest:
                timings.append(report["concurrent_query"]["seconds"])
            report["max_request_seconds"] = max(report["max_request_seconds"], *timings)
            report["vmhwm_bytes"] = peak_rss(proc.pid)
        finally:
            report["shutdown"] = stop(proc)
        shutdown = report["shutdown"]
        if shutdown["exit"] != 0 or shutdown["sigkill"] or shutdown["seconds"] > 4:
            raise ValueError(f"Bog did not shut down cleanly: {shutdown}")
        if max(report["vmhwm_bytes"], shutdown["peak_rss_bytes"]) > CAP:
            raise ValueError("Bog peak resident memory exceeded 256 MiB")
        if group is not None:
            final_events = events(group)
            report["oom_deltas"] = {key: final_events[key] - initial_events[key] for key in final_events}
            if any(report["oom_deltas"].values()):
                raise ValueError(f"cgroup OOM counters advanced: {report['oom_deltas']}")
        return report
    finally:
        if group is not None:
            group.rmdir()


def main():
    started = time.monotonic()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--rss-only", action="store_true", help="local smoke test without delegated cgroups")
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)

    def check_binary():
        with binary.open("rb") as candidate:
            if hashlib.file_digest(candidate, "sha256").hexdigest() != args.sha256:
                raise ValueError("candidate checksum mismatch")

    check_binary()
    parent = None
    if not args.rss_only:
        membership = Path("/proc/self/cgroup").read_text().strip()
        if not membership.startswith("0::/"):
            raise ValueError("this journey requires a delegated cgroup v2 service")
        parent = Path("/sys/fs/cgroup") / membership[4:]
        if parent.name == "runner":
            parent = parent.parent
        (parent / "cgroup.subtree_control").write_text("+memory")
    with tempfile.TemporaryDirectory(dir=args.work_dir, prefix="o1-data-") as data:
        starts = [run_start(binary, Path(data), parent, started + 110, ingest) for ingest in (True, False)]
    check_binary()
    elapsed = time.monotonic() - started
    if elapsed > 120:
        raise TimeoutError(f"O1/J1 took {elapsed:.3f}s including cleanup")
    print(json.dumps({
        "journey": "bog/0.2/O1/J1", "revision": args.revision, "binary_sha256": args.sha256,
        "machine": os.uname().machine, "memory_max": CAP if parent else None,
        "memory_swap_max": 0 if parent else None, "starts": starts, "elapsed_seconds": elapsed,
    }, indent=2))


if __name__ == "__main__":
    main()
