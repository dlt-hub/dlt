"""
description: Chunking example
tags: performance
"""

import dlt

if __name__ == "__main__":

    from itertools import islice

    def get_rows(limit):
        yield from map(lambda n: {"row": n}, range(limit))

    @dlt.resource
    def database_cursor_chunked():
        # here we yield chunks of size 1000
        rows = get_rows(10000)
        while item_slice := list(islice(rows, 1000)):
            print(f"got chunk of length {len(item_slice)}")
            yield item_slice

    assert len(list(database_cursor_chunked())) == 10000
