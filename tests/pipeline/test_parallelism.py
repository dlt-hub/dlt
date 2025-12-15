"""
Actual parallelism test with the help of custom destination
"""
import gzip
import os

import pytest
import dlt
import time
from typing import Dict, Tuple

from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema
from dlt.common.destination.capabilities import TLoaderParallelismStrategy
from tests.pipeline.utils import (
    assert_table_column,
)

def run_pipeline(
    items_per_table: int,
    max_parallel_load_jobs: int = None,
    loader_parallelism_strategy: TLoaderParallelismStrategy = None,
) -> Tuple[int, Dict[str, int]]:
    """here we create a pipeline and count how many jobs run in parallel overall and per table depending on the settings"""

    # create one job per item
    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "1"

    current_executing: int = 0
    max_current_executing: int = 0

    current_executing_per_table: Dict[str, int] = {}
    max_current_executing_per_table: Dict[str, int] = {}

    @dlt.destination(
        max_parallel_load_jobs=max_parallel_load_jobs,
        loader_parallelism_strategy=loader_parallelism_strategy,
    )
    def test_sink(items: TDataItems, table: TTableSchema) -> None:
        nonlocal current_executing, max_current_executing, current_executing_per_table, max_current_executing_per_table
        table_name = table["name"]
        # remember the amount of concurrent executions
        current_executing += 1
        max_current_executing = max(max_current_executing, current_executing)
        # table
        current_executing_per_table.setdefault(table_name, 0)
        max_current_executing_per_table.setdefault(table_name, 0)
        current_executing_per_table[table_name] += 1
        max_current_executing_per_table[table_name] = max(
            max_current_executing_per_table[table_name], current_executing_per_table[table_name]
        )
        # NOTE: this approach might make the test flaky again, let's see
        time.sleep(0.5)
        current_executing -= 1
        current_executing_per_table[table_name] -= 1

    def t() -> TDataItems:
        nonlocal items_per_table
        for i in range(items_per_table):
            yield {"num": i}

    # we load n items for 3 tables in one run
    p = dlt.pipeline("sink_test", destination=test_sink, dev_mode=True)
    p.run(
        [
            dlt.resource(table_name="t1")(t),
            dlt.resource(table_name="t2")(t),
            dlt.resource(table_name="t3")(t),
        ]
    )

    return max_current_executing, max_current_executing_per_table


def test_max_concurrent() -> None:
    # default is 20, so result is lower than that
    max_concurrent, _ = run_pipeline(10)
    assert max_concurrent <= 20 and max_concurrent >= 18

    # lower it
    max_concurrent, _ = run_pipeline(5, max_parallel_load_jobs=5)
    assert max_concurrent <= 5 and max_concurrent >= 3

    # sequential strategy will make it go to 1
    max_concurrent, _ = run_pipeline(
        2, max_parallel_load_jobs=5, loader_parallelism_strategy="sequential"
    )
    assert max_concurrent == 1


def test_loading_strategy() -> None:
    max_concurrent, max_concurrent_per_table = run_pipeline(
        10, max_parallel_load_jobs=20, loader_parallelism_strategy="parallel"
    )
    # this includes multiple jobs per table being run
    assert max_concurrent <= 20 and max_concurrent >= 18
    assert max_concurrent_per_table["t1"] > 2

    # this strategy only allows one job per table max
    max_concurrent, max_concurrent_per_table = run_pipeline(
        3, loader_parallelism_strategy="table-sequential"
    )
    # we still have concurrent jobs but only one per table max
    assert max_concurrent <= 3 and max_concurrent >= 2
    assert max_concurrent_per_table == {
        "t1": 1,
        "t2": 1,
        "t3": 1,
    }

    # sequential strategy will make it go to 1
    max_concurrent, _ = run_pipeline(
        2, max_parallel_load_jobs=5, loader_parallelism_strategy="sequential"
    )
    assert max_concurrent == 1
    assert max_concurrent_per_table == {
        "t1": 1,
        "t2": 1,
        "t3": 1,
    }


def test_normalize_compression_with_spawn_workers() -> None:
    """Disabling compression should work with multiple workers and spawn method,
    because ConfigSectionContext is restored in worker processes.
    """
    # Set compression disabled via normalize section
    workers = 4
    os.environ["NORMALIZE__DATA_WRITER__DISABLE_COMPRESSION"] = "true"
    os.environ["NORMALIZE__WORKERS"] = str(workers)
    os.environ["NORMALIZE__START_METHOD"] = "spawn"

    data = ["a", "b", "c", "d", "e"]
    dataset_name = "compression_spawn_test_"

    p = dlt.pipeline("compression_spawn_test", dataset_name=dataset_name, destination="duckdb")
    p.extract(dlt.resource(data, name="data"))

    # Normalize with multiple workers and spawn method
    p.normalize(workers=workers)

    # Check that normalized files are not compressed
    normalized_packages = p.list_normalized_load_packages()
    assert len(normalized_packages) > 0, "Should have at least one normalized package"

    job_storage = p._get_load_storage()
    for load_id in normalized_packages:
        # Get all job files from the normalized package
        job_files = job_storage.normalized_packages.list_new_jobs(load_id)
        assert len(job_files) > 0, f"Should have at least one job file in package {load_id}"

        for job_file_name in job_files:
            file_path = job_storage.normalized_packages.storage.make_full_path(job_file_name)
            # If compression is disabled, file should NOT be gzipped
            with pytest.raises(gzip.BadGzipFile):
                with gzip.open(file_path, "rb") as f:
                    f.read()

    info = p.load()
    assert_table_column(p, "data", data, info)
