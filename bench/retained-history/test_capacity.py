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
            rows = capacity.walk('http://localhost', 2, lambda: inserted.append(True))
        self.assertEqual(len(rows), 2)
        self.assertEqual(inserted, [True])
        self.assertIn('cursor=a%2Bb', calls[1])
        self.assertNotIn('page=1', calls[1])

    def test_percentile_nearest_rank(self):
        self.assertEqual(capacity.percentile(list(range(100)), 99), 98)

if __name__ == '__main__':
    unittest.main()
