# @@@SNIPSTART parallel_config_example
import dlt
from itertools import islice
from dlt.common import pendulum

@dlt.resource(name="table")
def read_table(limit):
    rows = iter(range(limit))
    while item_slice := list(islice(rows, 1000)):
        now = pendulum.now().isoformat()
        yield [{"row": _id, "description": "this is row with id {_id}", "timestamp": now} for _id in item_slice]


# this prevents process pool to run the initialization code again
if __name__ == "__main__":
    pipeline = dlt.pipeline("parallel_load", destination="duckdb", full_refresh=True)
    pipeline.extract(read_table(1000000))
    # we should have 11 files (10 pieces for `table` and 1 for state)
    print(pipeline.list_extracted_resources())
    # normalize and print counts
    print(pipeline.normalize(loader_file_format="jsonl"))
    # print jobs in load package (10 + 1 as above)
    load_id = pipeline.list_normalized_load_packages()[0]
    print(pipeline.get_load_package_info(load_id))
    print(pipeline.load())
# @@@SNIPEND