import unittest
from collections import Counter

from streaming_algorithms import (
    BloomFilter,
    canonical_key,
    dgim_expire,
    dgim_query,
    dgim_update,
    screen_batch_keys,
)


class BloomFilterTests(unittest.TestCase):
    def test_existing_keys_are_candidates_and_new_keys_are_retained(self):
        records = [
            {
                "latitude": float(index),
                "longitude": -120.0,
                "acq_ts_utc": f"2020-08-15 00:{index:02d}:00",
                "satellite": "N",
            }
            for index in range(20)
        ]
        existing = [canonical_key(record, "firms") for record in records[:10]]
        new_rows, bloom_hits = screen_batch_keys(records, "firms", existing)
        self.assertEqual(len(new_rows) + len(bloom_hits), len(records))
        hit_keys = {canonical_key(row, "firms") for row in bloom_hits}
        self.assertTrue(set(existing).issubset(hit_keys))

    def test_inserted_values_are_never_false_negatives(self):
        bloom = BloomFilter(100, 0.01)
        values = [f"key-{index}" for index in range(100)]
        for value in values:
            bloom.add(value)
        self.assertTrue(all(value in bloom for value in values))


class DGIMTests(unittest.TestCase):
    def test_oldest_in_window_bucket_is_halved(self):
        self.assertEqual(dgim_query([(100, 1), (90, 2), (80, 4)], 85), 2)

    def test_bucket_invariant_and_expiry(self):
        buckets = []
        bits = [1, 0, 1, 1, 0, 1, 1, 1, 1, 0, 1, 1]
        for timestamp, bit in enumerate(bits):
            dgim_update(buckets, bit, timestamp, max_buckets=2)
        counts = Counter(size for _, size in buckets)
        self.assertTrue(all(count <= 2 for count in counts.values()))
        self.assertEqual(
            [timestamp for timestamp, _ in buckets],
            sorted((timestamp for timestamp, _ in buckets), reverse=True),
        )
        dgim_expire(buckets, 6)
        self.assertTrue(all(timestamp >= 6 for timestamp, _ in buckets))

    def test_out_of_order_timestamp_is_rejected(self):
        buckets = []
        dgim_update(buckets, 1, 10)
        with self.assertRaises(ValueError):
            dgim_update(buckets, 1, 9)


if __name__ == "__main__":
    unittest.main()
