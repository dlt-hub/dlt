from dlt.common.metrics import DataWriterMetrics, aggregate_job_metrics
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
    metrics = DataWriterMetrics(job.file_name(), items_count, file_size, created, last_modified)
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
        "alpha": DataWriterMetrics("a", 3, 1, 1.0, 2.0),
        "beta": DataWriterMetrics("b", 4, 1, 1.0, 2.0),
        "gamma": DataWriterMetrics("c", 5, 1, 1.0, 2.0),
    }
    resource_to_table = {"alpha": "res_a", "beta": "res_b", "gamma": "res_a"}
    resource_metrics = aggregate_job_metrics(
        table_metrics, lambda table_name: resource_to_table[table_name]
    )
    assert resource_metrics["res_a"].items_count == 8
    assert resource_metrics["res_b"].items_count == 4
