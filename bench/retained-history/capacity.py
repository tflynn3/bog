#!/usr/bin/env python3
"""Pinned, single-host Linux capacity verification for bog."""
import argparse
import concurrent.futures
import hashlib
import json
import os
import pathlib
import platform
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import uuid
import time
import urllib.error
import urllib.parse
import urllib.request

START = '2020-01-01T00:00:00Z'
END = '2030-01-01T00:00:00Z'


def fixture_row(i):
    # Repeated timestamps and a deterministic permutation create ties and backfill.
    second = (i // 7 * 7919) % 86400
    return {'ts': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(1577836800 + second)),
            'line': 'capacity-%012d' % i}


def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def request(url, data=None):
    req = urllib.request.Request(url, data=data, method='POST' if data is not None else 'GET')
    with urllib.request.urlopen(req, timeout=60) as response:
        return response.read(), response.headers


def walk(base, page_size, after_first=None):
    params = {'start': START, 'end': END, 'limit': str(page_size), 'page': '1'}
    result = []
    seen = set()
    while True:
        body, headers = request(base + '/query?' + urllib.parse.urlencode(params))
        result.extend(json.loads(line) for line in body.splitlines())
        cursor = headers.get('Bog-Next-Cursor')
        if after_first is not None:
            after_first(result[:])
            after_first = None
        if not cursor:
            return result
        if cursor in seen:
            raise RuntimeError('server repeated a cursor')
        seen.add(cursor)
        params.pop('page', None)
        params['cursor'] = cursor


def cgroup_memory():
    """Return current cgroup and visible mount root (cgroup v2)."""
    entry = next((line.split(':', 2)[2].strip() for line in open('/proc/self/cgroup') if line.startswith('0::')), None)
    if entry is None:
        raise RuntimeError('cgroup v2 is required')
    for line in open('/proc/self/mountinfo'):
        left, sep, right = line.partition(' - ')
        if sep and right.split()[0] == 'cgroup2':
            fields = left.split()
            root, mount = (v.replace('\\040', ' ').replace('\\134', '\\') for v in fields[3:5])
            if entry == root or entry.startswith(root.rstrip('/') + '/'):
                return pathlib.Path(mount) / entry[len(root):].lstrip('/'), pathlib.Path(mount)
    raise RuntimeError('cannot resolve cgroup v2 mount')


def effective_limit(cg, mount):
    limits = []
    while True:
        value = (cg / 'memory.max').read_text().strip()
        if value != 'max':
            limits.append(int(value))
        if cg == mount:
            break
        if mount not in cg.parents:
            raise RuntimeError('cgroup outside mount')
        cg = cg.parent
    if not limits or min(limits) <= 0:
        raise RuntimeError('a finite effective cgroup memory limit is required')
    return min(limits)


def device_io(cg, device):
    for line in (cg / 'io.stat').read_text().splitlines():
        fields = line.split()
        if fields[0] == device:
            values = dict(field.split('=', 1) for field in fields[1:])
            if 'rbytes' not in values or 'wbytes' not in values:
                break
            return {key: int(values[key]) for key in ('rbytes', 'wbytes')}
    raise RuntimeError('cgroup io.stat lacks read/write counters for data device ' + device)


def io_delta(before, after):
    return {key: after[key] - before[key] for key in before}


def backfill_for(first_page, rows):
    boundary = first_page[-1]['ts']
    timestamps = sorted({fixture_row(i)['ts'] for i in range(rows - 1)})
    candidates = [ts for ts in timestamps if boundary < ts < timestamps[-1]]
    if not candidates:
        raise RuntimeError('fixture cannot place backfill after the first cursor and before the latest row')
    return {'ts': candidates[len(candidates) // 2], 'line': 'capacity-%012d' % (rows - 1)}


def percentile(values, p):
    if not values:
        raise RuntimeError('no ingest measurements')
    values = sorted(values)
    return values[max(0, (len(values) * p + 99) // 100 - 1)]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--binary', required=True)
    ap.add_argument('--commit', required=True, help='full git commit embedded in Go binary')
    ap.add_argument('--sha256', required=True, help='expected SHA-256 of binary')
    ap.add_argument('--data-dir', required=True, help='empty directory on target device')
    ap.add_argument('--report', required=True)
    ap.add_argument('--rows', type=int, default=1000000)
    ap.add_argument('--batch-size', type=int, default=500)
    ap.add_argument('--writers', type=int, default=4)
    ap.add_argument('--page-size', type=int, default=500)
    args = ap.parse_args()
    report = {'schema': 'bog.retained-history.capacity.v1', 'status': 'fail', 'errors': []}
    process = None
    log = None
    benchmark_cg = None
    try:
        if platform.system() != 'Linux':
            raise RuntimeError('Linux is required')
        if args.rows < args.page_size + 8 or args.batch_size < 1 or args.writers < 1 or not 2 <= args.page_size <= 1000 or args.page_size % 7 == 0:
            raise RuntimeError('invalid fixture or concurrency parameters')
        binary = os.path.realpath(args.binary)
        digest = sha256(binary)
        if digest != args.sha256.lower() or len(args.sha256) != 64:
            raise RuntimeError('binary SHA-256 mismatch')
        build = subprocess.check_output(['go', 'version', '-m', binary], text=True)
        if len(args.commit) != 40 or ('vcs.revision=' + args.commit) not in build or 'vcs.modified=false' not in build:
            raise RuntimeError('binary must embed the requested clean full revision')
        data = pathlib.Path(args.data_dir).resolve()
        if not data.is_dir() or any(data.iterdir()):
            raise RuntimeError('data directory must exist and be empty')
        cg, mount = cgroup_memory()
        limit = effective_limit(cg, mount)
        benchmark_cg = cg / ('bog-capacity-' + uuid.uuid4().hex)
        benchmark_cg.mkdir()
        device = '%d:%d' % (os.major(data.stat().st_dev), os.minor(data.stat().st_dev))
        if not (benchmark_cg / 'memory.peak').exists() or not (benchmark_cg / 'io.stat').exists():
            raise RuntimeError('delegated cgroup with memory.peak and io.stat is required')
        report.update(commit=args.commit, binary={'path': binary, 'sha256': digest},
                      fixture={'name': 'capacity-v1', 'rows': args.rows, 'batch_size': args.batch_size,
                               'writers': args.writers, 'page_size': args.page_size,
                               'timestamp_permutation': '(i//7*7919)%86400', 'line': 'capacity-%012d'},
                      cgroup={'path': str(benchmark_cg), 'effective_memory_max_bytes': limit},
                      device={'major_minor': device},
                      host={'machine': platform.machine(), 'kernel': platform.release()}, data_dir=str(data))
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]
        base = 'http://127.0.0.1:%d' % port
        log = tempfile.TemporaryFile()
        def start():
            nonlocal process
            before = time.monotonic()
            def enter_cgroup():
                (benchmark_cg / 'cgroup.procs').write_text(str(os.getpid()))
            process = subprocess.Popen([binary, '-data-dir', str(data), '-addr', '127.0.0.1:%d' % port], stdout=log, stderr=log, preexec_fn=enter_cgroup)
            while time.monotonic() - before < 120:
                if process.poll() is not None:
                    raise RuntimeError('server exited before ready: %s' % process.returncode)
                try:
                    request(base + '/healthz')
                    return (time.monotonic() - before) * 1000
                except (OSError, urllib.error.URLError):
                    time.sleep(.02)
            raise RuntimeError('server readiness timeout')
        def stop():
            process.send_signal(signal.SIGTERM)
            if process.wait(timeout=60) != 0:
                raise RuntimeError('server did not shut down cleanly')
        start()
        latencies = []
        latency_lock = threading.Lock()
        stop_queries = threading.Event()
        query_errors = []
        query_count = [0]
        def query_loop():
            while not stop_queries.is_set():
                try:
                    request(base + '/query?' + urllib.parse.urlencode({'start': START, 'end': END, 'limit': 10}))
                    query_count[0] += 1
                except Exception as e:
                    query_errors.append(str(e))
                    return
        def ingest(indices, override=None):
            payload = b''.join((json.dumps(override if override is not None else fixture_row(i), separators=(',', ':')) + '\n').encode() for i in indices)
            t = time.monotonic()
            request(base + '/ingest', payload)
            with latency_lock:
                latencies.append((time.monotonic() - t) * 1000)
        thread = threading.Thread(target=query_loop)
        thread.start()
        try:
            # Reserve the last row as a post-snapshot backfill.
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.writers) as pool:
                futures = [pool.submit(ingest, range(i, min(i + args.batch_size, args.rows - 1)))
                           for i in range(0, args.rows - 1, args.batch_size)]
                for future in futures:
                    future.result()
        finally:
            stop_queries.set()
            thread.join()
        if query_errors or not query_count[0]:
            raise RuntimeError('concurrent query failed or did not run: %s' % query_errors)
        backfill = []
        def insert_backfill(first_page):
            backfill.append(backfill_for(first_page, args.rows))
            ingest([args.rows - 1], backfill[0])
        rows = walk(base, args.page_size, insert_backfill)
        expected = [fixture_row(i) for i in range(args.rows - 1)]
        # Ties from concurrent commits have nondeterministic ordering; compare exact multisets
        # and require timestamp order, then ensure the snapshot excludes the backfill.
        if len(rows) != len(expected) or sorted((r['ts'], r['line']) for r in rows) != sorted((r['ts'], r['line']) for r in expected):
            raise RuntimeError('snapshot walk does not reconcile with fixture')
        if any(rows[i]['ts'] > rows[i+1]['ts'] for i in range(len(rows)-1)):
            raise RuntimeError('snapshot walk is not timestamp ordered')
        full = walk(base, args.page_size)
        if [r for r in full if r['line'] != backfill[0]['line']] != rows:
            raise RuntimeError('tie order changed after backfill')
        index = next(i for i, row in enumerate(full) if row['line'] == backfill[0]['line'])
        expected_index = sum(row['ts'] <= backfill[0]['ts'] for row in rows)
        if full[index] != backfill[0] or index != expected_index:
            raise RuntimeError('backfill is not at its expected position')
        if sorted((r['ts'], r['line']) for r in full) != sorted((r['ts'], r['line']) for r in expected + backfill):
            raise RuntimeError('post-backfill walk does not reconcile')
        io_before_restart = device_io(benchmark_cg, device)
        disk = os.statvfs(data)
        stop()
        restart_ms = start()
        replay = walk(base, args.page_size)
        if replay != full:
            raise RuntimeError('restart walk differs from pre-restart walk')
        io_restart = device_io(benchmark_cg, device)
        peak = int((benchmark_cg / 'memory.peak').read_text())
        stop()
        report.update(status='pass', reconciliation={'snapshot_rows': len(rows), 'post_backfill_rows': len(full), 'restart_rows': len(replay)},
                      measurements={'peak_cgroup_memory_bytes': peak, 'restart_to_ready_ms': restart_ms,
                                    'device_io_bytes': {'live': io_before_restart, 'restart': io_delta(io_before_restart, io_restart)},
                                    'ingest_p99_ms': percentile(latencies, 99), 'ingest_requests': len(latencies),
                                    'concurrent_queries': query_count[0], 'disk_available_bytes': disk.f_bavail * disk.f_frsize,
                                    'disk_total_bytes': disk.f_blocks * disk.f_frsize})
    except Exception as e:
        report['errors'].append(str(e))
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()
        if log is not None:
            log.close()
        if benchmark_cg is not None and benchmark_cg.exists():
            try:
                benchmark_cg.rmdir()
            except OSError as e:
                report['status'] = 'fail'
                report['errors'].append('cannot remove benchmark cgroup: %s' % e)
        with open(args.report, 'w') as f:
            json.dump(report, f, indent=2, sort_keys=True)
            f.write('\n')
    return 0 if report['status'] == 'pass' else 1


if __name__ == '__main__':
    sys.exit(main())
