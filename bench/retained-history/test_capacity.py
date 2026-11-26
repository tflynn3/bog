import importlib.util
import pathlib
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('capacity', pathlib.Path(__file__).with_name('capacity.py'))
capacity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capacity)


class CapacityTest(unittest.TestCase):
    def test_fixture_ties_and_backfill(self):
        rows = [capacity.fixture_row(i) for i in range(15)]
        self.assertEqual(len({r['ts'] for r in rows[:7]}), 1)
        self.assertEqual(len({r['line'] for r in rows}), 15)
        self.assertLess(capacity.fixture_row(77)['ts'], capacity.fixture_row(70)['ts'])

    def test_walk_follows_cursor_and_inserts_after_first_page(self):
        calls = []
        def request(url):
            calls.append(url)
            if len(calls) == 1:
                return b'{"ts":"a","line":"one"}\n', {'Bog-Next-Cursor': 'a+b'}
            return b'{"ts":"b","line":"two"}\n', {}
        inserted = []
        with patch.object(capacity, 'request', request):
            rows = capacity.walk('http://localhost', 2, lambda page: inserted.append(page))
        self.assertEqual(len(rows), 2)
        self.assertEqual(inserted, [[{'ts': 'a', 'line': 'one'}]])
        self.assertIn('cursor=a%2Bb', calls[1])
        self.assertNotIn('page=1', calls[1])

    def test_backfill_is_after_cursor_and_before_latest_for_regression(self):
        fixture = sorted((capacity.fixture_row(i) for i in range(77)), key=lambda r: r['ts'])
        row = capacity.backfill_for(fixture[:8], 78)
        self.assertGreater(row['ts'], fixture[7]['ts'])
        self.assertLess(row['ts'], fixture[-1]['ts'])

    def test_effective_limit_uses_all_visible_ancestors(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            child = root / 'child'
            child.mkdir()
            (root / 'memory.max').write_text('100')
            (child / 'memory.max').write_text('max')
            self.assertEqual(capacity.effective_limit(child, root), 100)
            (child / 'memory.max').write_text('50')
            self.assertEqual(capacity.effective_limit(child, root), 50)

    def test_device_io_selects_device_and_requires_counters(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            cg = pathlib.Path(tmp)
            (cg / 'io.stat').write_text('8:0 rbytes=12 wbytes=34 rios=1\n8:1 rbytes=5 wbytes=6\n')
            self.assertEqual(capacity.device_io(cg, '8:1'), {'rbytes': 5, 'wbytes': 6})
            with self.assertRaises(RuntimeError):
                capacity.device_io(cg, '9:0')

    def test_percentile_nearest_rank(self):
        self.assertEqual(capacity.percentile(list(range(100)), 99), 98)

if __name__ == '__main__':
    unittest.main()
