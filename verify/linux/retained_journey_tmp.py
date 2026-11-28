import concurrent.futures
import datetime as dt
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse

budget = 268435456
origin = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
started = time.monotonic()

def ts(i):
    return (origin + dt.timedelta(minutes=i)).isoformat().replace("+00:00", "Z")

def row(i):
    return {"ts": ts(i), "line": ("NEEDLE" if i % 4 == 1 else "needle") + "-" + str(i) + "-" + "x"*10240}

def events(path):
    return {key: int(value) for key, value in (line.split() for line in path.read_text().splitlines())}

def hwm(pid):
    return int(next(line.split()[1] for line in Path(f"/proc/{pid}/status").read_text().splitlines() if line.startswith("VmHWM:"))) * 1024

def request(conn, method, path, body=None):
    at = time.monotonic()
    conn.request(method, path, body)
    response = conn.getresponse()
    b = response.read()
    assert time.monotonic() - at <= 2, (method, path, time.monotonic()-at)
    return response.status, response.getheader("Content-Type"), b

with tempfile.TemporaryDirectory(prefix="retained-journey-", dir=".") as work:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    work = Path(work)
    data = work / "data"
    data.mkdir(mode=0o700)
    def new_conn():
        return http.client.HTTPConnection("127.0.0.1", port, timeout=2)
    def query(k):
        q = urllib.parse.urlencode(dict(start=ts(k), end=ts(k+3), contains="needle",limit=10))
        conn = new_conn()
        try:
            code, media, body = request(conn,"GET","/query?"+q)
        finally:
            conn.close()
        assert code == 200 and media == "application/x-ndjson", (code,media,body[:200])
        assert [json.loads(line) for line in body.splitlines()] == [row(k), row(k+2)], k

    for cycle in (1,2):
        cgroup = Path(sys.argv[2]) / (f"retained-journey-{os.getpid()}-{cycle}")
        cgroup.mkdir()
        process = None
        try:
            (cgroup/"memory.max").write_text(str(budget))
            (cgroup/"memory.swap.max").write_text("0")
            before = events(cgroup/"memory.events")
            with (work / f"log-{cycle}").open("wb") as log:
                at = time.monotonic()
                process = subprocess.Popen(["bash", "-c", 'echo $$ > "$1/cgroup.procs" || exit; shift; exec "$@"', "_", str(cgroup), sys.argv[1], "-data-dir", str(data), "-addr", f"127.0.0.1:{port}"], stdout=log, stderr=log)
            while True:
                conn = new_conn()
                try:
                    if request(conn,"GET","/healthz")[0] == 200:
                        break
                except OSError:
                    pass
                finally:
                    conn.close()
                assert process.poll() is None, (process.returncode,(work/f"log-{cycle}").read_text())
                assert time.monotonic() - at < 15
                time.sleep(.02)
            health = time.monotonic() - at
            assert health <= 15
            assert (cgroup/"cgroup.procs").read_text().split() == [str(process.pid)]
            peak = hwm(process.pid)
            if cycle == 1:
                ingest_at = time.monotonic()
                writer = new_conn()
                try:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as concurrent_reader:
                        pending = None
                        for j in range(128):
                            body = b"".join(json.dumps(row(i),separators=(",", ":")).encode()+b"\n" for i in range(j*256,(j+1)*256))
                            try:
                                result = request(writer,"POST","/ingest",body)
                            except Exception:
                                print(j+1, peak, (cgroup/"memory.current").read_text(), events(cgroup/"memory.events"), (work/f"log-{cycle}").read_text(), flush=True)
                                if process.poll() is None:
                                    process.send_signal(signal.SIGQUIT)
                                    process.wait(timeout=2)
                                    print((work/f"log-{cycle}").read_text()[:13000], flush=True)
                                raise
                            assert result[0] == 204, (j+1, result, (work/f"log-{cycle}").read_text())
                            if j == 31:
                                pending = concurrent_reader.submit(query,0)
                            peak = max(peak,hwm(process.pid))
                            assert peak <= budget, (j+1,peak)
                            assert time.monotonic()-ingest_at <= 60
                        pending.result()
                    print(f"all 128 batches acknowledged in {time.monotonic()-ingest_at:.3f}s", flush=True)
                finally:
                    writer.close()
            for k in (0,16384,32764):
                query(k)
            peak = max(peak,hwm(process.pid))
            assert peak <= budget and time.monotonic()-started <= 110
            shutdown_at = time.monotonic()
            process.send_signal(signal.SIGTERM)
            assert process.wait(timeout=4) == 0, (work/f"log-{cycle}").read_text()
            shutdown = time.monotonic()-shutdown_at
            after = events(cgroup/"memory.events")
            oom = after["oom"]-before["oom"]
            killed = after["oom_kill"]-before["oom_kill"]
            assert oom == killed == 0
            print(f"start {cycle} health={health:.3f}s VmHWM={peak} bytes oom={oom} oom_kill={killed} shutdown={shutdown:.3f}s", flush=True)
        finally:
            if process is not None and process.poll() is None:
                process.kill()
                process.wait(timeout=2)
            cgroup.rmdir()
assert time.monotonic()-started <= 120
print(f"O1/J1 passed in {time.monotonic()-started:.3f}s", flush=True)
