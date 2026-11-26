#!/usr/bin/env python3
"""Black-box durability verifier; see README.md for the execution contract."""
import argparse
import hashlib
import json
import os
import pathlib
import platform
import re
import shutil
import signal
import socket
import struct
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

START = '2025-01-01T00:00:00Z'
END = '2025-01-02T00:00:00Z'


def require(value, message):
    if not value:
        raise AssertionError(message)


def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


class Server:
    def __init__(self, binary, directory):
        self.binary, self.directory, self.proc = binary, directory, None
        self.log = tempfile.TemporaryFile()
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            self.port = s.getsockname()[1]

    def start(self):
        self.log.seek(0, os.SEEK_END)
        self.log_start = self.log.tell()
        self.proc = subprocess.Popen([self.binary, '-data-dir', str(self.directory), '-addr', '127.0.0.1:%d' % self.port], stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError('server exited during startup: ' + self.logs())
            try:
                if self.request('/healthz')[0] == 200:
                    return
            except (OSError, urllib.error.URLError):
                time.sleep(.05)
        raise RuntimeError('health timeout: ' + self.logs())

    def logs(self):
        self.log.flush()
        self.log.seek(self.log_start)
        return self.log.read().decode('utf-8', 'replace')[-4000:]

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)

    def request(self, path, data=None):
        req = urllib.request.Request('http://127.0.0.1:%d%s' % (self.port, path), data=data)
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status, r.headers, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.headers, e.read()

    def ingest(self, rows):
        return self.request('/ingest', b''.join((json.dumps(row) + '\n').encode() for row in rows))[0]

    def page(self, cursor=None):
        params = {'start': START, 'end': END, 'limit': '1'}
        params.update({'cursor': cursor} if cursor else {'page': '1'})
        return self.request('/query?' + urllib.parse.urlencode(params))

    def walk(self):
        rows, cursor, seen = [], None, set()
        while True:
            status, headers, body = self.page(cursor)
            require(status == 200, 'query returned %s: %r' % (status, body))
            rows.extend(json.loads(line) for line in body.splitlines())
            cursor = headers.get('Bog-Next-Cursor')
            if not cursor:
                return rows
            require(cursor not in seen, 'repeated cursor')
            seen.add(cursor)


def row(name, second=10):
    return {'ts': '2025-01-01T00:00:%02dZ' % second, 'line': name}


def check_rows(server, expected):
    actual = server.walk()
    require(actual == expected, 'rows differ: expected %r, got %r' % (expected, actual))


def token(server):
    status, headers, _ = server.page()
    require(status == 200 and headers.get('Bog-Next-Cursor'), 'could not obtain cursor')
    return headers['Bog-Next-Cursor']


def restart(server, old):
    server.stop()
    server.start()
    require(server.page(old)[0] == 400, 'pre-restart cursor accepted')


def recovery(binary, directory):
    s = Server(binary, directory)
    try:
        s.start()
        expected = [row('a', 10), row('b', 10), row('c', 20)]
        require(s.ingest(expected) == 204, 'initial ingest failed')
        require(s.request('/ingest', b'{bad json}\n')[0] == 400, 'invalid batch accepted')
        old = token(s)
        restart(s, old)
        check_rows(s, expected)
        # Backfill ties both the old timestamp and the old commit order.
        backfill = [row('backfill', 10), row('earlier', 5)]
        require(s.ingest(backfill) == 204, 'backfill failed')
        expected = [backfill[1], expected[0], expected[1], backfill[0], expected[2]]
        check_rows(s, expected)
        old = token(s)
        s.stop()
        wal = directory / 'bog.wal'
        original_length = wal.stat().st_size
        with wal.open('ab') as f:
            f.write(b'\x10\x00\x00')
            f.flush()
            os.fsync(f.fileno())
        s.start()
        require(wal.stat().st_size == original_length, 'torn WAL suffix was not truncated')
        require(s.page(old)[0] == 400, 'cursor survived torn-tail restart')
        check_rows(s, expected)
        appended = [row('after-torn-tail', 25)]
        require(s.ingest(appended) == 204, 'append after torn-tail recovery failed')
        expected += appended
        old = token(s)
        restart(s, old)
        check_rows(s, expected)
        s.stop()
        # Corrupt a byte inside the committed frame; startup must not repair it.
        data = bytearray(wal.read_bytes())
        require(len(data) > 25, 'WAL unexpectedly short')
        data[24] ^= 1
        wal.write_bytes(data)
        before = {p.name: p.read_bytes() for p in (wal, directory / 'bog.commit')}
        try:
            s.start()
        except RuntimeError:
            require(s.proc.poll() not in (None, 0), 'corruption did not cause a nonzero startup exit')
            require(re.search(r'invalid WAL (?:header )?checksum', s.logs(), re.I),
                    'startup did not report WAL checksum corruption: ' + s.logs())
        else:
            raise AssertionError('committed corruption accepted')
        after = {p.name: p.read_bytes() for p in (wal, directory / 'bog.commit')}
        require(before == after, 'committed files changed after corruption')
    finally:
        s.stop()
        s.log.close()


def kill_case(binary, directory):
    s = Server(binary, directory)
    client = None
    try:
        s.start()
        acknowledged = [row('ack-1'), row('ack-2')]
        require(s.ingest(acknowledged) == 204, 'acknowledged batch failed')
        old = token(s)
        batch = [row('unknown-%d' % i) for i in range(100)]
        body = b''.join((json.dumps(item) + '\n').encode() for item in batch)
        client = socket.create_connection(('127.0.0.1', s.port), timeout=10)
        client.settimeout(10)
        client_port = client.getsockname()[1]
        headers = ('POST /ingest HTTP/1.1\r\nHost: localhost\r\nContent-Length: %d\r\nConnection: close\r\n\r\n' % len(body)).encode()
        client.sendall(headers + body[:1])
        # The accepted socket must belong to Bog and its receive queue must be
        # drained after the partial request was sent. This establishes that Bog
        # read the request before the kill, not merely that connect() succeeded.
        deadline = time.monotonic() + 5
        reached = False
        while time.monotonic() < deadline and s.proc.poll() is None:
            inodes = set()
            for fd in pathlib.Path('/proc/%d/fd' % s.proc.pid).iterdir():
                try:
                    match = re.fullmatch(r'socket:\[(\d+)\]', os.readlink(fd))
                    if match:
                        inodes.add(match.group(1))
                except FileNotFoundError:
                    pass
            for line in pathlib.Path('/proc/net/tcp').read_text().splitlines()[1:]:
                fields = line.split()
                local, remote = fields[1], fields[2]
                if (int(local.split(':')[1], 16) == s.port and
                        int(remote.split(':')[1], 16) == client_port and
                        fields[3] == '01' and fields[9] in inodes and
                        int(fields[4].split(':')[1], 16) == 0):
                    reached = True
                    break
            # TCP_INFO.tcpi_unacked is at offset 24 on Linux. Waiting for
            # acknowledgements avoids mistaking an empty queue before arrival
            # for consumption by the server.
            unacked = struct.unpack_from('=I', client.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, 104), 24)[0]
            if reached and unacked == 0:
                break
            reached = False
            time.sleep(.01)
        require(reached, 'inconclusive SIGKILL: Bog did not consume the in-flight ingest')
        outcome = []
        def send():
            try:
                client.sendall(body[1:])
                response = client.makefile('rb').readline().split()
                outcome.append(int(response[1]) if len(response) >= 2 else None)
            except (OSError, ValueError):
                outcome.append(None)
        t = threading.Thread(target=send)
        t.start()
        s.stop()
        t.join(timeout=15)
        client.close()
        require(not t.is_alive(), 'interrupted request did not finish')
        s.start()
        require(s.page(old)[0] == 400, 'cursor survived SIGKILL')
        actual = s.walk()
        require(actual in (acknowledged, acknowledged + batch), 'interrupted batch partially recovered')
        if outcome == [204]:
            require(actual == acknowledged + batch, 'acknowledged interrupted batch lost')
        elif outcome and outcome[0] is not None:
            require(actual == acknowledged, 'rejected interrupted batch published')
    finally:
        if client:
            client.close()
        s.stop()
        s.log.close()


def disk_full(binary, mount):
    # Only operate on a dedicated tmpfs mount with no existing entries.
    mount = pathlib.Path(mount).resolve()
    require(os.path.ismount(mount), 'ENOSPC path must be a dedicated mount point')
    entries = [line.split() for line in pathlib.Path('/proc/mounts').read_text().splitlines()]
    require(any(e[1].replace('\\040', ' ') == str(mount) and e[2] == 'tmpfs' for e in entries), 'ENOSPC mount must be tmpfs')
    require(not list(mount.iterdir()), 'ENOSPC mount must be empty')
    directory = mount / 'data'
    directory.mkdir(mode=0o700)
    s = Server(binary, directory)
    filler = mount / 'filler'
    try:
        s.start()
        acknowledged = [row('disk-ack-1'), row('disk-ack-2')]
        require(s.ingest(acknowledged) == 204, 'disk initial ingest failed')
        old = token(s)
        # Actual writes, not sparse truncation, must exhaust available blocks.
        with filler.open('wb', buffering=0) as f:
            block = b'x' * (1024 * 1024)
            try:
                while True:
                    f.write(block)
            except OSError as e:
                import errno
                require(e.errno == errno.ENOSPC, 'filler failed without ENOSPC: %s' % e)
        rejected = [row('disk-rejected-1'), row('disk-rejected-2')]
        require(s.ingest(rejected) == 503, 'full-disk batch was not explicitly rejected')
        check_rows(s, acknowledged)
        filler.unlink()
        resumed = [row('disk-resumed')]
        require(s.ingest(resumed) == 204, 'ingest did not resume')
        restart(s, old)
        check_rows(s, acknowledged + resumed)
    finally:
        s.stop()
        s.log.close()
        if filler.exists():
            filler.unlink()
        shutil.rmtree(directory)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, help='prebuilt Linux executable')
    parser.add_argument('--revision', required=True, help='full source commit SHA used to build binary')
    parser.add_argument('--sha256', required=True, help='expected binary SHA-256')
    parser.add_argument('--report', required=True, help='JSON report path')
    parser.add_argument('--enospc-mount', required=True, help='empty dedicated tmpfs mount')
    args = parser.parse_args()
    report = {'revision': args.revision, 'binary': os.path.abspath(args.binary), 'binary_sha256': None,
              'host': platform.platform(), 'cases': {}, 'status': 'fail'}
    try:
        require(platform.system() == 'Linux', 'Linux is required')
        require(len(args.revision) == 40 and all(c in '0123456789abcdef' for c in args.revision), 'revision must be a full lowercase commit SHA')
        binary = os.path.abspath(args.binary)
        with open(binary, 'rb') as f:
            require(f.read(4) == b'\x7fELF', 'binary must be a Linux ELF executable')
        report['binary_sha256'] = digest(binary)
        require(report['binary_sha256'] == args.sha256.lower(), 'binary SHA-256 mismatch')
        report['filesystem'] = subprocess.check_output(['stat', '-f', '-c', '%T', args.enospc_mount], text=True).strip()
        with tempfile.TemporaryDirectory(prefix='bog-verify-') as temp:
            for name, case in [('sigkill', kill_case), ('recovery', recovery)]:
                path = pathlib.Path(temp) / name
                path.mkdir(mode=0o700)
                try:
                    case(binary, path)
                    report['cases'][name] = {'status': 'pass'}
                except Exception as e:
                    report['cases'][name] = {'status': 'fail', 'error': str(e)}
            try:
                disk_full(binary, args.enospc_mount)
                report['cases']['enospc'] = {'status': 'pass'}
            except Exception as e:
                report['cases']['enospc'] = {'status': 'fail', 'error': str(e)}
        require(digest(binary) == report['binary_sha256'], 'binary changed during verification')
        if all(c['status'] == 'pass' for c in report['cases'].values()):
            report['status'] = 'pass'
    except Exception as e:
        report['error'] = str(e)
    finally:
        pathlib.Path(args.report).write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(report, indent=2))
    return 0 if report['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
