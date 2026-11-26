#!/usr/bin/env python3
"""Verify recovery and lifecycle through a binary built from a pinned checkout."""
import argparse
import collections
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

REV = 'f727b8f676845a8058d1ecf114442d8963cbbbae'
START, END = '2025-01-01T00:00:00Z', '2025-01-02T00:00:00Z'
TIMEOUT = 15


def check(value, message):
    if not value:
        raise AssertionError(message)


def run(args, cwd):
    return subprocess.run(args, cwd=cwd, text=True, capture_output=True,
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


def all_rows(server):
    status, _, rows = query(server, limit=1000)
    check(status == 200, f'query returned {status}')
    return rows


def same_rows(actual, expected, label):
    def counts(rows):
        return collections.Counter((r['ts'], r['line']) for r in rows)
    check(counts(actual) == counts(expected), f'{label}: {actual!r} != {expected!r}')


def crash(binary, directory, children, rounds):
    server = start(binary, directory, children)
    expected = []
    for n in range(rounds):
        acknowledged = [row(f'ack-{n}-{i}') for i in range(3)]
        ingest(server, acknowledged)
        expected.extend(acknowledged)
        status, _, _ = request(server.port, 'POST', '/ingest',
                               encode([row(f'rejected-{n}')]) + b'{invalid}\n')
        check(status == 400, f'rejected batch returned {status}')
        unknown = [row(f'unread-{n}-{i}') for i in range(4)]
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
    status, headers, walked = query(server, contains='walk-', page=1, limit=1)
    cursor = headers.get('bog-next-cursor')
    check(status == 200 and len(walked) == 1 and cursor, 'missing first cursor')
    stale = cursor
    ingest(server, [row('walk-backfill', '2025-01-01T11:00:00Z')])
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
    check(status == 200 and fresh == [row('walk-backfill', '2025-01-01T11:00:00Z')] + fixture,
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('checkout', type=pathlib.Path, help='clean pinned Linux checkout')
    parser.add_argument('--rounds', type=int, default=5)
    args = parser.parse_args()
    check(sys.platform.startswith('linux'), 'must run on Linux')
    check(2 <= args.rounds <= 100, 'rounds must be between 2 and 100')
    checkout = args.checkout.resolve()
    check(run(['git', 'rev-parse', '--show-toplevel'], checkout) == str(checkout), 'not repository root')
    check(run(['git', 'rev-parse', 'HEAD'], checkout) == REV, 'wrong revision')
    check(not run(['git', 'status', '--porcelain', '--untracked-files=all'], checkout), 'checkout is dirty')
    with tempfile.TemporaryDirectory(prefix='bog-linux-verify-') as tmp:
        root = pathlib.Path(tmp)
        binary = root / 'bog'
        run(['go', 'build', '-buildvcs=true', '-o', str(binary), './cmd/bog'], checkout)
        info = run(['go', 'version', '-m', str(binary)], checkout).split()
        check('vcs.revision=' + REV in info and 'vcs.modified=false' in info,
              'binary revision or clean build metadata incorrect')
        children = []
        try:
            for name in ('crash', 'bind', 'shutdown'):
                (root / name).mkdir(mode=0o700)
            server = crash(binary, root / 'crash', children, args.rounds)
            pagination(server, binary, root / 'crash', children)
            bind_failure(binary, root / 'bind', children)
            shutdown(binary, root / 'shutdown', children)
        finally:
            for child in reversed(children):
                child.close()
    print(f'PASS: {args.rounds} crash rounds, pagination, bind failure and SIGTERM ({REV})')


if __name__ == '__main__':
    try:
        main()
    except (AssertionError, OSError, subprocess.SubprocessError, http.client.HTTPException) as exc:
        print(f'FAIL: {exc}', file=sys.stderr)
        sys.exit(1)
