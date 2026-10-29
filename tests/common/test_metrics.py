import pickle
from typing import List

from dlt.common.metrics import (
    EMPTY_DATA_WRITER_METRICS,
    DataWriterAndCustomMetrics,
    DataWriterMetrics,
    TSchemaReference,
    aggregate_job_metrics,
    data_location_version,
)
from dlt.common.storages.job_metrics import merge_metrics, metrics_by_job, metrics_by_table
from dlt.common.storages.load_package import ParsedLoadJobFileName


def _job(
    table_name: str,
    file_id: str,
    items_count: int,
    file_size: int = 0,
    created: float = 1.0,
    last_modified: float = 2.0,
) -> tuple[ParsedLoadJobFileName, DataWriterMetrics]:
    job = ParsedLoadJobFileName(table_name, file_id, 0, "typed-jsonl")
    metrics = DataWriterMetrics(
        job.file_name(), items_count, file_size, created, last_modified, files_count=1
    )
    return job, metrics


def test_aggregate_job_metrics_sums_interleaved_tables() -> None:
    jobs = dict(
        [
            _job("parent", "aa", 10, file_size=100, created=30.0, last_modified=40.0),
            _job("child", "bb", 5, file_size=50, created=15.0, last_modified=25.0),
            _job("parent", "cc", 20, file_size=200, created=10.0, last_modified=99.0),
            _job("other", "dd", 7, file_size=70, created=5.0, last_modified=8.0),
            _job("child", "ee", 15, file_size=150, created=12.0, last_modified=60.0),
        ]
    )
    table_metrics = aggregate_job_metrics(jobs, lambda job: job.table_name)
    assert table_metrics["parent"].items_count == 30
    assert table_metrics["child"].items_count == 20
    assert table_metrics["other"].items_count == 7

    # __add__ semantics must survive aggregation: file_path reset, sizes summed,
    # created is the group min and last_modified the group max
    parent = table_metrics["parent"]
    assert parent.file_path == ""
    assert parent.file_size == 300
    assert parent.created == 10.0
    assert parent.last_modified == 99.0

    # a singleton group also passes through EMPTY, so the sentinel's created (2**32)
    # and last_modified (0.0) must not leak into the result
    other = table_metrics["other"]
    assert other.file_path == ""
    assert other.file_size == 70
    assert other.created == 5.0
    assert other.last_modified == 8.0


def test_aggregate_job_metrics_sums_interleaved_resources() -> None:
    table_metrics = {
        "alpha": DataWriterMetrics("a", 3, 1, 1.0, 2.0, files_count=1),
        "beta": DataWriterMetrics("b", 4, 1, 1.0, 2.0, files_count=1),
        "gamma": DataWriterMetrics("c", 5, 1, 1.0, 2.0, files_count=1),
    }
    resource_to_table = {"alpha": "res_a", "beta": "res_b", "gamma": "res_a"}
    resource_metrics = aggregate_job_metrics(
        table_metrics, lambda table_name: resource_to_table[table_name]
    )
    assert resource_metrics["res_a"].items_count == 8
    assert resource_metrics["res_b"].items_count == 4
    assert resource_metrics["res_a"].files_count == 2


def test_job_metrics_by_job_and_table() -> None:
    # job files are parsed from their names only, they do not exist
    job_metrics = metrics_by_job(
        [
            _job("parent", "aa", 10, 100)[1],
            _job("parent", "bb", 5, 50)[1],
            _job("child", "cc", 3)[1],
        ]
    )
    assert sorted(job.table_name for job in job_metrics) == ["child", "parent", "parent"]
    table_metrics = metrics_by_table(job_metrics)
    assert table_metrics["parent"].files_count == 2
    assert table_metrics["parent"].items_count == 15
    assert table_metrics["child"].files_count == 1

    # tables of two packages are merged, a table seen once keeps its metrics
    merged = merge_metrics([table_metrics, {"parent": table_metrics["parent"]}])
    assert merged["parent"].files_count == 4
    assert merged["parent"].file_size == 300
    assert merged["child"] == table_metrics["child"]


def test_metrics_of_older_traces_have_no_files_count() -> None:
    # unpickling passes the stored fields to __new__, traces before 1.32 have no files count
    restored = DataWriterMetrics.__new__(DataWriterMetrics, "", 10, 100, 1.0, 2.0)
    assert restored.files_count is None
    # a sum with an unknown count is unknown
    assert (EMPTY_DATA_WRITER_METRICS + restored).files_count is None
    assert (restored + DataWriterMetrics("", 1, 2, 1.0, 2.0, files_count=1)).files_count is None
    custom = DataWriterAndCustomMetrics.__new__(
        DataWriterAndCustomMetrics, "", 1, 2, 1.0, 2.0, custom_metrics={"a": 1}
    )
    restored_custom = pickle.loads(pickle.dumps(custom))
    assert restored_custom.files_count is None
    assert restored_custom.custom_metrics == {"a": 1}
    # the stored count survives a round trip
    custom = DataWriterAndCustomMetrics("", 1, 2, 1.0, 2.0, files_count=3, custom_metrics={"a": 1})
    assert pickle.loads(pickle.dumps(custom)).files_count == 3


def test_data_location_version_is_order_independent() -> None:
    schemas: List[TSchemaReference] = [
        {"name": "a", "version_hash": "hash_a"},
        {"name": "b", "version_hash": "hash_b"},
    ]
    assert data_location_version(schemas) == data_location_version(list(reversed(schemas)))
    # a changed constituent hash changes the version
    assert data_location_version(schemas) != data_location_version(
        [schemas[0], {"name": "b", "version_hash": "hash_b2"}]
    )
    assert data_location_version([]) == data_location_version([])
