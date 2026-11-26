#!/usr/bin/env python3
"""Black-box, bounded retained-history experiment. See README.md for contracts."""
import argparse
import hashlib
import json
import math
import os
import pathlib
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

START = '2025-01-01T00:00:00Z'
END = '2025-01-02T00:00:00Z'
BOUNDS = {'start': START, 'end': END, 'limit': '1000'}


def command(*args, **kw):
    return subprocess.check_output(args, text=True, **kw).strip()


def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def marker(data):
    raw = (data / 'bog.commit').read_bytes()
    if len(raw) != 24 or raw[:8] != b'BOGCMT01':
        raise ValueError('invalid commit marker')
    end, complement = struct.unpack('<QQ', raw[8:])
    if end ^ complement != (1 << 64) - 1 or end < 8:
        raise ValueError('invalid committed offset')
    wal = data / 'bog.wal'
    if wal.stat().st_size < end or wal.open('rb').read(8) != b'BOGWAL01':
        raise ValueError('committed offset outside WAL or bad WAL magic')
    return end


def fixture(n):
    # Batches alternate forward and reverse timestamp ranges. Equal timestamps
    # and repeated identical rows exercise tie order and multiplicity.
    rows = []
    for i in range(n):
        batch, within = divmod(i, 100)
        group = batch // 2
        tick = group * 100 + (within if batch % 2 == 0 else 99 - within)
        tick //= 2
        rows.append({'ts': '2025-01-01T00:%02d:%02dZ' % divmod(tick % 3600, 60),
                     'line': 'duplicate' if within % 7 < 2 else 'row-%08d' % i})
    return rows


def ndjson(rows):
    return b''.join((json.dumps(r, separators=(',', ':')) + '\n').encode() for r in rows)


def request(base, path, data=None, timeout=5):
    req = urllib.request.Request(base + path, data=data)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.headers, resp.read()


def io_stats(path):
    result = {}
    for line in path.read_text().splitlines():
        fields = line.split()
        result[fields[0]] = {k: int(v) for k, v in (x.split('=') for x in fields[1:])}
    return result


def io_delta(before, after):
    return {dev: {key: value - before.get(dev, {}).get(key, 0)
                  for key, value in values.items()}
            for dev, values in after.items()}


def events(path):
    return {k: int(v) for k, v in (line.split() for line in path.read_text().splitlines())}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--checkout', type=pathlib.Path, required=True)
    p.add_argument('--revision', required=True, help='full 40-character commit ID')
    p.add_argument('--work-dir', type=pathlib.Path, required=True, help='local filesystem for copied data and binary')
    p.add_argument('--cgroup-parent', type=pathlib.Path, default=pathlib.Path('/sys/fs/cgroup'))
    p.add_argument('--memory-max', type=int, default=536870912)
    p.add_argument('--rows', type=int, default=2000)
    p.add_argument('--seconds', type=int, default=5)
    p.add_argument('--timeout', type=int, default=120, help='overall seconds')
    p.add_argument('--fixture', type=pathlib.Path, help='prebuilt data directory, copied before use')
    p.add_argument('--expected', type=pathlib.Path, help='complete ordered snapshot as NDJSON')
    p.add_argument('--covered-offset', type=int)
    p.add_argument('--checkpoint', help='relative checkpoint filename inside fixture')
    p.add_argument('--offset-at', type=int, help='byte position of little-endian uint64 covered offset in checkpoint')
    a = p.parse_args()
    result = {'status': 'error', 'errors': [], 'timeouts': [], 'revision': a.revision}
    proc = None
    cg = None
    log = None
    temp = None
    deadline = time.monotonic() + a.timeout
    stop = threading.Event()
    threads = []
    latencies, query_latencies, workload_errors = [], [], []
    oom_before, io_before = {}, {}
    try:
        if sys.platform != 'linux':
            raise ValueError('Linux with writable cgroup v2 is required')
        if len(a.revision) != 40 or any(c not in '0123456789abcdef' for c in a.revision):
            raise ValueError('revision must be a full lowercase commit ID')
        if not 1 <= a.rows <= 1000000 or not 1 <= a.seconds <= 300 or not 10 <= a.timeout <= 3600 or a.memory_max < 16777216:
            raise ValueError('rows, seconds, timeout or memory limit outside allowed bounds')
        checkout = a.checkout.resolve()
        if command('git', '-C', str(checkout), 'rev-parse', 'HEAD') != a.revision:
            raise ValueError('checkout HEAD differs from requested revision')
        if command('git', '-C', str(checkout), 'status', '--porcelain', '--untracked-files=all'):
            raise ValueError('checkout must be clean, including untracked files')
        a.work_dir.mkdir(parents=True, exist_ok=True)
        fs_type = command('stat', '-f', '-c', '%T', str(a.work_dir))
        if fs_type in {'nfs', 'nfs4', 'cifs', 'smb2', 'smb3', '9p', 'fuseblk'} or fs_type.startswith('fuse.'):
            raise ValueError('work directory must be on a local filesystem: ' + fs_type)
        result['filesystem_type'] = fs_type
        temp = tempfile.TemporaryDirectory(prefix='bog-baseline-', dir=a.work_dir)
        root = pathlib.Path(temp.name)
        binary = root / 'bog'
        subprocess.run(['go', 'build', '-buildvcs=true', '-o', str(binary), './cmd/bog'], cwd=checkout, check=True, timeout=min(300, a.timeout))
        version = command('go', 'version', '-m', str(binary))
        settings = dict(line.strip().removeprefix('build\t').split('=', 1) for line in version.splitlines() if line.strip().startswith('build\t') and '=' in line)
        if settings.get('vcs.revision') != a.revision or settings.get('vcs.modified') != 'false':
            raise ValueError('binary VCS metadata does not match clean revision')
        result['binary_sha256'] = digest(binary)
        data = root / 'data'
        if a.fixture:
            if not all(x is not None for x in (a.expected, a.covered_offset, a.checkpoint, a.offset_at)):
                raise ValueError('scale mode requires expected, covered-offset, checkpoint and offset-at')
            if a.covered_offset < 8 or a.offset_at < 0:
                raise ValueError('invalid checkpoint offset')
            cp = pathlib.PurePosixPath(a.checkpoint)
            if cp.is_absolute() or '..' in cp.parts:
                raise ValueError('checkpoint must be a relative path')
            if sum(f.stat().st_size for f in a.fixture.rglob('*') if f.is_file()) > 100 * 1024**3:
                raise ValueError('fixture exceeds 100 GiB limit')
            shutil.copytree(a.fixture, data, symlinks=False)
            checkpoint = data.joinpath(*cp.parts)
            with checkpoint.open('rb') as f:
                f.seek(a.offset_at)
                raw = f.read(8)
            if len(raw) != 8 or struct.unpack('<Q', raw)[0] != a.covered_offset:
                raise ValueError('checkpoint embedded covered offset does not match supplied value')
            committed = marker(data)
            if a.covered_offset > committed:
                raise ValueError('checkpoint covers beyond committed WAL')
            result['checkpoint'] = {'file': a.checkpoint, 'sha256': digest(checkpoint), 'offset_at': a.offset_at,
                                    'covered_offset': a.covered_offset, 'committed_offset': committed,
                                    'committed_uncovered_wal_bytes': committed - a.covered_offset}
            expected = [json.loads(line) for line in a.expected.read_text().splitlines() if line.strip()]
            if len(expected) > 1000000:
                raise ValueError('expected snapshot exceeds 1M rows')
        else:
            if any(x is not None for x in (a.expected, a.covered_offset, a.checkpoint, a.offset_at)):
                raise ValueError('checkpoint arguments require --fixture')
            data.mkdir(mode=0o700)
            expected = fixture(a.rows)
        result['fixture'] = {'rows': len(expected), 'input_bytes': len(ndjson(expected)),
                             'mode': 'prebuilt' if a.fixture else 'seeded'}
        disk = shutil.disk_usage(data)
        result['disk_headroom_bytes_before'] = disk.free
        cg = a.cgroup_parent / ('bog-baseline-%d' % os.getpid())
        cg.mkdir()
        (cg / 'memory.max').write_text(str(a.memory_max))
        result['memory_max'] = (cg / 'memory.max').read_text().strip()
        oom_before = events(cg / 'memory.events')
        io_before = io_stats(cg / 'io.stat')
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]
        base = 'http://127.0.0.1:%d' % port
        log = open(root / 'server.log', 'w+')

        def remaining():
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError('overall deadline')
            return left

        def start():
            nonlocal proc
            started = time.monotonic()
            def join_cgroup():
                (cg / 'cgroup.procs').write_text(str(os.getpid()))
            proc = subprocess.Popen([str(binary), '-data-dir', str(data), '-addr', '127.0.0.1:%d' % port],
                                    stdout=log, stderr=log, preexec_fn=join_cgroup, start_new_session=True)
            while True:
                remaining()
                if proc.poll() is not None:
                    raise RuntimeError('server exited during startup: %s' % proc.returncode)
                try:
                    if request(base, '/healthz', timeout=min(1, remaining()))[0] == 200:
                        return time.monotonic() - started
                except (OSError, urllib.error.URLError):
                    time.sleep(0.05)

        def shutdown():
            nonlocal proc
            if proc and proc.poll() is None:
                os.killpg(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait(timeout=3)
            proc = None

        start()
        if not a.fixture:
            for i in range(0, len(expected), 100):
                remaining()
                status, _, _ = request(base, '/ingest', ndjson(expected[i:i+100]), min(5, remaining()))
                if status != 204:
                    raise RuntimeError('seed ingest status %s' % status)
        shutdown()
        result['fixture']['disk_bytes'] = sum(f.stat().st_size for f in data.rglob('*') if f.is_file())
        result['restart_to_ready_seconds'] = start()
        expected.sort(key=lambda row: row['ts'])  # Python sort preserves append order for ties.
        params = dict(BOUNDS, page='1')
        actual = []
        seen = set()
        while True:
            remaining()
            _, headers, body = request(base, '/query?' + urllib.parse.urlencode(params), timeout=min(5, remaining()))
            actual.extend(json.loads(line) for line in body.splitlines())
            if len(actual) > len(expected):
                raise ValueError('cursor walk returned excess rows')
            token = headers.get('Bog-Next-Cursor')
            if not token:
                break
            if token in seen:
                raise ValueError('cursor repeated')
            seen.add(token)
            params.pop('page', None)
            params['cursor'] = token
        result['snapshot'] = {'expected_rows': len(expected), 'actual_rows': len(actual), 'matches': actual == expected,
                              'pages': len(seen) + 1}
        if actual != expected:
            raise ValueError('cursor walk differs from expected ordered rows')

        def worker(kind):
            while not stop.is_set() and time.monotonic() < deadline:
                t = time.monotonic()
                try:
                    if kind == 'ingest':
                        status, _, _ = request(base, '/ingest', ndjson([{'ts': START, 'line': 'workload'}] * 10), timeout=2)
                        if status != 204:
                            raise ValueError('ingest status %s' % status)
                        latencies.append(time.monotonic() - t)
                    else:
                        request(base, '/query?' + urllib.parse.urlencode(BOUNDS), timeout=2)
                        query_latencies.append(time.monotonic() - t)
                except Exception as e:
                    workload_errors.append('%s: %s' % (kind, e))
                    stop.set()
        threads = [threading.Thread(target=worker, args=(kind,)) for kind in ('ingest', 'ingest', 'query', 'query')]
        for thread in threads:
            thread.start()
        stop.wait(min(a.seconds, remaining()))
        stop.set()
        for thread in threads:
            thread.join(timeout=4)
        remaining()
        if workload_errors:
            raise RuntimeError('workload failed: ' + workload_errors[0])
        if not latencies or not query_latencies:
            raise RuntimeError('no durable ingest or broad query completed')
        latencies.sort()
        query_latencies.sort()
        result['workload'] = {'ingest_requests': len(latencies), 'ingest_p99_seconds': latencies[math.ceil(len(latencies) * .99) - 1],
                              'query_requests': len(query_latencies),
                              'query_p99_seconds': query_latencies[math.ceil(len(query_latencies) * .99) - 1],
                              'duration_seconds': a.seconds, 'writers': 2, 'query_workers': 2}
        result['status'] = 'ok'
    except (Exception, KeyboardInterrupt) as e:
        result['errors'].append('%s: %s' % (type(e).__name__, e))
        if isinstance(e, (TimeoutError, subprocess.TimeoutExpired)):
            result['timeouts'].append(str(e))
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=4)
        if proc and proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
        if log:
            log.flush()
            log.seek(0)
            result['server_log_tail'] = log.read()[-4096:]
            log.close()
        if cg and cg.exists():
            try:
                result['memory_peak_bytes'] = int((cg / 'memory.peak').read_text())
                result['memory_events_delta'] = {k: v - oom_before.get(k, 0) for k, v in events(cg / 'memory.events').items()}
                result['io_device_deltas'] = io_delta(io_before, io_stats(cg / 'io.stat'))
                if result['memory_events_delta'].get('oom_kill', 0):
                    result['status'] = 'error'
                    result['errors'].append('cgroup OOM kill')
            except Exception as e:
                result['errors'].append('cgroup statistics: %s' % e)
                result['status'] = 'error'
            try:
                cg.rmdir()
            except OSError as e:
                result['errors'].append('cgroup cleanup: %s' % e)
                result['status'] = 'error'
        if temp:
            result['disk_headroom_bytes_after'] = shutil.disk_usage(temp.name).free
            temp.cleanup()
        print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result['status'] == 'ok' else 1


if __name__ == '__main__':
    sys.exit(main())
