import os
os.environ["DLT_PROJECT_DIR"] = os.path.dirname(__file__)
# @@@SNIPSTART performance_chunking
import dlt

def get_rows(limit):
    yield from map(lambda n: {"row": n}, range(limit))

@dlt.resource
def database_cursor():
    # here we yield each row returned from database separately
    yield from get_rows(10000)
# @@@SNIPEND
# @@@SNIPSTART performance_chunking_chunk
from itertools import islice

@dlt.resource
def database_cursor_chunked():
    # here we yield chunks of size 1000
    rows = get_rows(10000)
    while item_slice := list(islice(rows, 1000)):
        print(f"got chunk of length {len(item_slice)}")
        yield item_slice
# @@@SNIPEND

assert len(list(database_cursor())) == 10000
assert len(list(database_cursor_chunked())) == 10000