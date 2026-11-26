#!/usr/bin/env python3
"""Verify recovery and lifecycle through a binary built from a pinned checkout."""
import argparse
import collections
import errno
import os
import platform
import random
import http.client
import json
import pathlib
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse

START, END = '2025-01-01T00:00:00Z', '2025-01-02T00:00:00Z'
TIMEOUT = 15


def check(value, message):
    if not value:
        raise AssertionError(message)


def run(args, cwd, env=None):
    return subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=True,
                          check=True, timeout=120).stdout.strip()


def row(line, ts='2025-01-01T12:00:00Z'):
    return {'ts': ts, 'line': line}


def encode(rows):
    return b''.join((json.dumps(r) + '\n').encode() for r in rows)


def listener():
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    sock.listen()
    return sock


def request(port, method, path, body=None):
    conn = http.client.HTTPConnection('127.0.0.1', port, timeout=TIMEOUT)
    try:
        conn.request(method, path, body)
        response = conn.getresponse()
        return response.status, dict(response.getheaders()), response.read()
    finally:
        conn.close()


class Server:
    def __init__(self, binary, directory, port, children):
        self.port = port
        self.log = tempfile.TemporaryFile(mode='w+')
        self.process = subprocess.Popen([str(binary), '-data-dir', str(directory),
                                         '-addr', f'127.0.0.1:{port}'],
                                        stdout=self.log, stderr=subprocess.STDOUT)
        children.append(self)

    def output(self):
        self.log.flush()
        self.log.seek(0)
        return self.log.read()

    def wait(self):
        try:
            return self.process.wait(timeout=TIMEOUT)
        except subprocess.TimeoutExpired:
            raise AssertionError('process did not exit: ' + self.output())

    def stop(self, sig=signal.SIGTERM):
        if self.process.poll() is None:
            self.process.send_signal(sig)
        return self.wait()

    def ready(self):
        deadline = time.monotonic() + TIMEOUT
        while time.monotonic() < deadline:
            check(self.process.poll() is None, 'server exited: ' + self.output())
            try:
                if request(self.port, 'GET', '/healthz')[0] == 200:
                    return
            except (OSError, http.client.HTTPException):
                pass
            time.sleep(.05)
        raise AssertionError('health timeout: ' + self.output())

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
        try:
            self.process.wait(timeout=TIMEOUT)
        finally:
            self.log.close()


def start(binary, directory, children):
    sock = listener()
    port = sock.getsockname()[1]
    sock.close()
    server = Server(binary, directory, port, children)
    server.ready()
    return server


def ingest(server, rows):
    status, _, body = request(server.port, 'POST', '/ingest', encode(rows))
    check(status == 204, f'ingest returned {status}: {body!r}')


def query(server, **params):
    values = {'start': START, 'end': END}
    values.update(params)
    status, headers, body = request(server.port, 'GET', '/query?' + urllib.parse.urlencode(values))
    rows = [json.loads(line) for line in body.splitlines()] if status == 200 else []
    return status, {k.lower(): v for k, v in headers.items()}, rows


def all_rows(server, **params):
    rows, seen, cursor = [], set(), None
    while True:
        options = dict(params, limit=37)
        options.update({'cursor': cursor} if cursor else {'page': 1})
        status, headers, page = query(server, **options)
        check(status == 200, f'query returned {status}')
        rows.extend(page)
        cursor = headers.get('bog-next-cursor')
        if not cursor:
            return rows
        check(page and cursor not in seen and len(seen) < 100000,
              'pagination failed to progress')
        seen.add(cursor)


def same_rows(actual, expected, label):
    def counts(rows):
        return collections.Counter((r['ts'], r['line']) for r in rows)
    check(counts(actual) == counts(expected), f'{label}: {actual!r} != {expected!r}')


def crash(binary, directory, children, rounds, seed):
    rng = random.Random(seed)
    server = start(binary, directory, children)
    expected = []
    for n in range(rounds):
        acknowledged = [row(f'ack-{n}-{i}-{rng.getrandbits(64):016x}')
                        for i in range(rng.randint(1, 5))]
        ingest(server, acknowledged)
        expected.extend(acknowledged)
        status, _, _ = request(server.port, 'POST', '/ingest',
                               encode([row(f'rejected-{n}')]) + b'{invalid}\n')
        check(status == 400, f'rejected batch returned {status}')
        unknown = [row(f'unread-{n}-{i}-{rng.getrandbits(64):016x}')
                   for i in range(rng.randint(1, 5))]
        body = encode(unknown)
        with socket.create_connection(('127.0.0.1', server.port), timeout=TIMEOUT) as sock:
            sock.sendall(b'POST /ingest HTTP/1.1\r\nHost: localhost\r\nContent-Length: ' +
                         str(len(body)).encode() + b'\r\nConnection: close\r\n\r\n' + body)
            # Deliberately do not read the response before killing the process.
        check(server.stop(signal.SIGKILL) == -signal.SIGKILL, 'SIGKILL failed')
        server = start(binary, directory, children)
        recovered = all_rows(server)
        observed = collections.Counter(r['line'] for r in recovered
                                       if r['line'].startswith(f'unread-{n}-'))
        check(not observed or observed == collections.Counter(r['line'] for r in unknown),
              f'partial unread batch: {observed}')
        if observed:
            expected.extend(unknown)
        same_rows(recovered, expected, f'round {n}')
    return server


def pagination(server, binary, directory, children):
    fixture = [row('walk-duplicate'), row('walk-duplicate'), row('walk-third'),
               row('walk-later', '2025-01-01T13:00:00Z')]
    ingest(server, fixture)
    check(server.stop() == 0, 'shutdown failed')
    server = start(binary, directory, children)
    status, headers, walked = query(server, contains='walk-', page=1, limit=1)
    cursor = headers.get('bog-next-cursor')
    check(status == 200 and len(walked) == 1 and cursor, 'missing first cursor')
    stale = cursor
    backfill = row('walk-backfill')
    ingest(server, [backfill])
    seen = {cursor}
    while cursor:
        status, headers, page = query(server, contains='walk-', cursor=cursor, limit=1)
        check(status == 200 and page, 'continuation failed')
        walked.extend(page)
        cursor = headers.get('bog-next-cursor')
        check(not cursor or cursor not in seen, 'repeated cursor')
        if cursor:
            seen.add(cursor)
        check(len(walked) <= len(fixture), 'pagination did not terminate')
    check(walked == fixture, f'snapshot order mismatch: {walked!r}')
    status, _, fresh = query(server, contains='walk-', limit=1000)
    check(status == 200 and fresh == fixture[:3] + [backfill] + fixture[3:],
          f'fresh query mismatch: {fresh!r}')
    check(server.stop() == 0, 'shutdown failed')
    server = start(binary, directory, children)
    check(query(server, contains='walk-', cursor=stale, limit=1)[0] == 400,
          'stale cursor was accepted')
    status, _, recovered = query(server, contains='walk-', limit=1000)
    check(status == 200 and recovered == fresh, 'fixture changed after restart')
    check(server.stop() == 0, 'shutdown failed')


def bind_failure(binary, directory, children):
    with listener() as sock:
        server = Server(binary, directory, sock.getsockname()[1], children)
        check(server.wait() != 0, 'occupied address succeeded')
        check('address already in use' in server.output().lower(), 'missing bind failure diagnostic')
        reopened = start(binary, directory, children)
        ingest(reopened, [row('bind-reopen')])
        check(reopened.stop() == 0, 'reopened store shutdown failed')


def read_headers(sock):
    data = b''
    while not data.endswith(b'\r\n\r\n'):
        chunk = sock.recv(1)
        check(chunk, f'connection closed while reading headers: {data!r}')
        data += chunk
        check(len(data) < 65536, 'response headers too large')
    return data


def shutdown(binary, directory, children):
    server = start(binary, directory, children)
    rows = [row('shutdown-1'), row('shutdown-2')]
    body = encode(rows)
    with socket.create_connection(('127.0.0.1', server.port), timeout=TIMEOUT) as sock:
        sock.settimeout(TIMEOUT)
        sock.sendall(b'POST /ingest HTTP/1.1\r\nHost: localhost\r\nExpect: 100-continue\r\n'
                     b'Connection: close\r\nContent-Length: ' + str(len(body)).encode() + b'\r\n\r\n')
        interim = read_headers(sock)
        check(interim.startswith(b'HTTP/1.1 100 '), f'no admission: {interim!r}')
        server.process.send_signal(signal.SIGTERM)
        deadline = time.monotonic() + TIMEOUT
        while True:
            check(server.process.poll() is None, 'server exited before body completed')
            try:
                with socket.create_connection(('127.0.0.1', server.port), timeout=.2):
                    pass
            except OSError:
                break
            check(time.monotonic() < deadline, 'server kept accepting connections')
            time.sleep(.05)
        sock.sendall(body)
        response = http.client.HTTPResponse(sock)
        response.begin()
        response.read()
        check(response.status == 204, f'admitted request returned {response.status}')
        check(server.wait() == 0, 'SIGTERM shutdown failed: ' + server.output())
    restarted = start(binary, directory, children)
    same_rows(all_rows(restarted), rows, 'shutdown recovery')
    check(restarted.stop() == 0, 'recovery server shutdown failed')


def mutations(binary, root, children):
    for name in ('torn', 'corrupt'):
        directory = root / name
        directory.mkdir(mode=0o700)
        server = start(binary, directory, children)
        expected = [row(name + '-committed')]
        ingest(server, expected)
        check(server.stop() == 0, 'mutation fixture shutdown failed')
        wal = directory / 'bog.wal'
        original = wal.read_bytes()
        if name == 'torn':
            with wal.open('ab') as f:
                f.write(b'\x10\x00\x00')
                f.flush()
                os.fsync(f.fileno())
            server = start(binary, directory, children)
            same_rows(all_rows(server), expected, 'torn suffix')
            check(wal.read_bytes() == original, 'torn suffix not discarded')
            check(server.stop() == 0, 'torn shutdown failed')
        else:
            damaged = bytearray(original)
            damaged[-1] ^= 1
            wal.write_bytes(damaged)
            sock = listener()
            port = sock.getsockname()[1]
            sock.close()
            server = Server(binary, directory, port, children)
            check(server.wait() == 1, 'committed corruption did not cause startup exit 1')
            check('invalid WAL checksum at ' in server.output(),
                  'missing WAL checksum diagnostic: ' + server.output())
            check(wal.read_bytes() == damaged, 'corrupt WAL was changed')


def disk_full(binary, directory, children):
    server = start(binary, directory, children)
    before = [row('disk-before')]
    rejected = [row('disk-rejected-' + 'x' * 65536)]
    after = [row('disk-after')]
    ingest(server, before)
    filler = directory / 'filler'
    exhausted = False
    try:
        with filler.open('wb', buffering=0) as f:
            chunk = b'x' * (1024 * 1024)
            for _ in range(2049):
                try:
                    f.write(chunk)
                except OSError as exc:
                    if exc.errno != errno.ENOSPC:
                        raise
                    exhausted = True
                    break
        check(exhausted, 'filesystem did not report ENOSPC within 2 GiB')
        status, _, body = request(server.port, 'POST', '/ingest', encode(rejected))
        check(status == 503 and body == b'WAL unavailable\n',
              f'full filesystem ingest returned {status}: {body!r}')
        check('WAL rollback confirmed operation=wal_write category=enospc' in server.output(),
              'server did not report a WAL write ENOSPC: ' + server.output())
    finally:
        filler.unlink(missing_ok=True)
    ingest(server, after)
    same_rows(all_rows(server), before + after, 'disk full live')
    check(server.stop() == 0, 'disk full shutdown failed')
    server = start(binary, directory, children)
    same_rows(all_rows(server), before + after, 'disk full recovery')
    check(server.stop() == 0, 'disk recovery shutdown failed')


def percentile(samples, percent):
    ordered = sorted(samples)
    return ordered[max(0, (len(ordered) * percent + 99) // 100 - 1)] * 1000


def rss(server):
    for line in pathlib.Path(f'/proc/{server.process.pid}/status').read_text().splitlines():
        if line.startswith('VmHWM:'):
            return int(line.split()[1])
    raise AssertionError('VmHWM unavailable')


def performance(binary, directory, children, batches, batch_size):
    server = start(binary, directory, children)
    timings, queries, expected = [], [], []
    begin = time.monotonic()
    for n in range(batches):
        batch = [row(f'perf-{n:06d}-{i:04d}') for i in range(batch_size)]
        t = time.monotonic()
        ingest(server, batch)
        timings.append(time.monotonic() - t)
        expected.extend(batch)
        t = time.monotonic()
        status, _, _ = query(server, contains='perf-', limit=10)
        queries.append(time.monotonic() - t)
        check(status == 200, f'performance query returned {status}')
    elapsed = time.monotonic() - begin
    peak = rss(server)
    check(server.stop() == 0, 'performance shutdown failed')
    t = time.monotonic()
    server = start(binary, directory, children)
    restart = time.monotonic() - t
    peak = max(peak, rss(server))
    same_rows(all_rows(server), expected, 'performance recovery')
    peak = max(peak, rss(server))
    check(server.stop() == 0, 'performance recovery shutdown failed')
    return {'rows_per_second': round(len(expected) / elapsed, 2),
            'ingest_p99_ms': round(percentile(timings, 99), 3),
            'query_p95_ms': round(percentile(queries, 95), 3),
            'restart_health_ms': round(restart * 1000, 3), 'peak_rss_kib': peak}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('checkout', type=pathlib.Path, help='clean pinned Linux checkout')
    parser.add_argument('--revision', required=True, help='full 40-character pinned commit ID')
    parser.add_argument('--filesystem', required=True, type=pathlib.Path,
                        help='dedicated capacity-limited filesystem mount')
    parser.add_argument('--seed', type=int, default=314159, help='reproducible crash oracle seed')
    parser.add_argument('--rounds', type=int, default=5)
    parser.add_argument('--batches', type=int, default=100)
    parser.add_argument('--batch-size', type=int, default=100)
    args = parser.parse_args()
    check(sys.platform.startswith('linux'), 'must run on Linux')
    check(len(args.revision) == 40 and all(c in '0123456789abcdef' for c in args.revision),
          'revision must be a full lowercase commit ID')
    check(2 <= args.rounds <= 100, 'rounds must be between 2 and 100')
    check(20 <= args.batches <= 10000 and 1 <= args.batch_size <= 1000,
          'invalid fixture settings')
    checkout = args.checkout.resolve()
    mount = args.filesystem.resolve()
    verifier_root = pathlib.Path(run(['git', 'rev-parse', '--show-toplevel'],
                                     pathlib.Path(__file__).resolve().parent)).resolve()
    check(checkout != verifier_root, 'checkout must be separate from the verifier checkout')
    check(mount.is_mount(), 'filesystem must be a dedicated mount point')
    stats = os.statvfs(mount)
    check(stats.f_blocks * stats.f_frsize <= 2 * 1024**3, 'filesystem exceeds 2 GiB')
    check(stats.f_bavail * stats.f_frsize >= 8 * 1024**2, 'filesystem needs 8 MiB free')
    check(run(['git', 'rev-parse', '--show-toplevel'], checkout) == str(checkout), 'not repository root')
    check(run(['git', 'rev-parse', 'HEAD'], checkout) == args.revision, 'wrong revision')
    check(not run(['git', 'status', '--porcelain', '--untracked-files=all'], checkout), 'checkout is dirty')
    with tempfile.TemporaryDirectory(prefix='bog-linux-verify-') as tmp:
        root = pathlib.Path(tmp)
        binary = root / 'bog'
        build_tree = root / 'source'
        run(['git', 'worktree', 'add', '--detach', str(build_tree), args.revision], checkout)
        try:
            build_env = os.environ.copy()
            build_env.update({'GOWORK': 'off', 'GOFLAGS': '', 'GOENV': 'off'})
            run(['go', 'build', '-buildvcs=true', '-o', str(binary), './cmd/bog'],
                build_tree, env=build_env)
            info = run(['go', 'version', '-m', str(binary)], build_tree, env=build_env).split()
        finally:
            run(['git', 'worktree', 'remove', '--force', str(build_tree)], checkout)
        check('vcs.revision=' + args.revision in info and 'vcs.modified=false' in info,
              'binary revision or clean build metadata incorrect')
        children = []
        try:
            for name in ('crash', 'bind', 'shutdown', 'perf1', 'perf2'):
                (root / name).mkdir(mode=0o700)
            server = crash(binary, root / 'crash', children, args.rounds, args.seed)
            pagination(server, binary, root / 'crash', children)
            bind_failure(binary, root / 'bind', children)
            shutdown(binary, root / 'shutdown', children)
            mutations(binary, root, children)
            with tempfile.TemporaryDirectory(prefix='bog-enospc-', dir=mount) as disk:
                disk_full(binary, pathlib.Path(disk), children)
            results = [performance(binary, root / f'perf{i}', children, args.batches,
                                   args.batch_size) for i in (1, 2)]
            print(json.dumps({'revision': args.revision, 'host': platform.uname()._asdict(),
                              'enospc_filesystem': run(['findmnt', '-T', str(mount), '-no',
                                                        'SOURCE,FSTYPE,OPTIONS'], checkout),
                              'work_filesystem': run(['findmnt', '-T', str(root), '-no',
                                                      'SOURCE,FSTYPE,OPTIONS'], checkout),
                              'seed': args.seed, 'rounds': args.rounds, 'batches': args.batches,
                              'batch_size': args.batch_size, 'runs': results}, indent=2))
        finally:
            for child in reversed(children):
                child.close()
    print('PASS: all durability, recovery, lifecycle and ENOSPC cases')


if __name__ == '__main__':
    try:
        main()
    except (AssertionError, OSError, subprocess.SubprocessError, http.client.HTTPException) as exc:
        print(json.dumps({'status': 'FAIL', 'error': str(exc)}), file=sys.stderr)
        sys.exit(1)
