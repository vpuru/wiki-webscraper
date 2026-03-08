import math

import mmh3
from bitarray import bitarray


class BloomFilter:
    def __init__(self, expected_items: int, fp_rate: float) -> None:
        self._count = 0
        self._num_bits = self._optimal_num_bits(expected_items, fp_rate)
        self._num_hashes = self._optimal_num_hashes(self._num_bits, expected_items)
        self._bits = bitarray(self._num_bits)
        self._bits.setall(0)

    @staticmethod
    def _optimal_num_bits(n: int, p: float) -> int:
        return int(-n * math.log(p) / (math.log(2) ** 2))

    @staticmethod
    def _optimal_num_hashes(m: int, n: int) -> int:
        return max(1, round((m / n) * math.log(2)))

    def add(self, item: str) -> None:
        for i in range(self._num_hashes):
            idx = mmh3.hash(item, seed=i) % self._num_bits
            self._bits[idx] = 1
        self._count += 1

    def __contains__(self, item: str) -> bool:
        return all(
            self._bits[mmh3.hash(item, seed=i) % self._num_bits]
            for i in range(self._num_hashes)
        )

    def estimated_count(self) -> int:
        return self._count

    @property
    def num_bits(self) -> int:
        return self._num_bits

    @property
    def num_hashes(self) -> int:
        return self._num_hashes
