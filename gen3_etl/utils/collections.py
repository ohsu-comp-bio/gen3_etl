"""Utility, helps with collections."""
import itertools


def grouper(n, iterable):
    """Chunk iterable into n size chunks."""
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk
