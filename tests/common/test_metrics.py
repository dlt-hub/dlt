from dlt.common.metrics import DataWriterMetrics, aggregate_job_metrics
from dlt.common.storages.load_package import ParsedLoadJobFileName


def _job(
    table_name: str, file_id: str, items_count: int
) -> tuple[ParsedLoadJobFileName, DataWriterMetrics]:
    job = ParsedLoadJobFileName(table_name, file_id, 0, "typed-jsonl")
    metrics = DataWriterMetrics(job.file_name(), items_count, items_count, 1.0, 2.0)
    return job, metrics


def test_aggregate_job_metrics_sums_interleaved_tables() -> None:
    jobs = dict(
        [
            _job("parent", "aa", 10),
            _job("child", "bb", 5),
            _job("parent", "cc", 20),
            _job("other", "dd", 7),
            _job("child", "ee", 15),
        ]
    )
    table_metrics = aggregate_job_metrics(jobs, lambda job: job.table_name)
    assert table_metrics["parent"].items_count == 30
    assert table_metrics["child"].items_count == 20
    assert table_metrics["other"].items_count == 7


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
