from wiki_scraper.bloom import BloomFilter


def test_add_and_contains():
    bf = BloomFilter(1000, 0.01)
    bf.add("https://en.wikipedia.org/wiki/Python")
    assert "https://en.wikipedia.org/wiki/Python" in bf
    assert "https://en.wikipedia.org/wiki/Java" not in bf


def test_estimated_count():
    bf = BloomFilter(1000, 0.01)
    assert bf.estimated_count() == 0
    bf.add("a")
    bf.add("b")
    assert bf.estimated_count() == 2


def test_false_positive_rate():
    n = 10_000
    bf = BloomFilter(n, 0.01)
    for i in range(n):
        bf.add(f"item-{i}")

    false_positives = sum(
        1 for i in range(n) if f"other-{i}" in bf
    )
    # Allow up to 2% (double the target) to account for variance
    assert false_positives / n < 0.02


def test_sizing():
    bf = BloomFilter(8_000_000, 0.001)
    # ~115M bits for 8M items at 0.1% FP
    assert 100_000_000 < bf.num_bits < 130_000_000
    assert bf.num_hashes == 10
