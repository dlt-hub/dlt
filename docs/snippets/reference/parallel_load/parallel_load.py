import os
os.environ["DLT_PROJECT_DIR"] = os.path.dirname(__file__)
# @@@SNIPSTART parallel_config_example
import os
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
if __name__ == "__main__" or "PYTEST_CURRENT_TEST" in os.environ:
    pipeline = dlt.pipeline("parallel_load", destination="duckdb", full_refresh=True)
    pipeline.extract(read_table(1000000))
    # we should have 11 files (10 pieces for `table` and 1 for state)
    extracted_files = pipeline.list_extracted_resources()
    print(extracted_files)
    # normalize and print counts
    print(pipeline.normalize(loader_file_format="jsonl"))
    # print jobs in load package (10 + 1 as above)
    load_id = pipeline.list_normalized_load_packages()[0]
    print(pipeline.get_load_package_info(load_id))
    print(pipeline.load())
# @@@SNIPEND

    assert len(extracted_files) == 11
    loaded_package = pipeline.get_load_package_info(load_id)
    assert len(loaded_package.jobs["completed_jobs"]) == 11