"""
description: Chunking example
tags: performance
"""

import dlt

if __name__ == "__main__":

    def get_rows(limit):
        yield from map(lambda n: {"row": n}, range(limit))

    @dlt.resource
    def database_cursor():
        # here we yield each row returned from database separately
        yield from get_rows(10000)

    assert len(list(database_cursor())) == 10000