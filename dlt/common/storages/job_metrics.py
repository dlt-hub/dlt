from typing import Dict, Iterable, Mapping

from dlt.common.metrics import EMPTY_DATA_WRITER_METRICS, DataWriterMetrics, aggregate_job_metrics
from dlt.common.storages.load_package import ParsedLoadJobFileName


def metrics_by_job(
    files: Iterable[DataWriterMetrics],
) -> Dict[ParsedLoadJobFileName, DataWriterMetrics]:
    """Keys the metrics of job files by their parsed file name. Does not access the files."""
    return {ParsedLoadJobFileName.parse(metrics.file_path): metrics for metrics in files}


def metrics_by_table(
    job_metrics: Mapping[ParsedLoadJobFileName, DataWriterMetrics],
) -> Dict[str, DataWriterMetrics]:
    """Sums the metrics of job files by table"""
    return aggregate_job_metrics(job_metrics, lambda job: job.table_name)


def merge_metrics(
    metrics_maps: Iterable[Mapping[str, DataWriterMetrics]],
) -> Dict[str, DataWriterMetrics]:
    """Sums metrics with the same key, ie. tables or resources of several load packages"""
    result: Dict[str, DataWriterMetrics] = {}
    for metrics_map in metrics_maps:
        for key, metrics in metrics_map.items():
            result[key] = result.get(key, EMPTY_DATA_WRITER_METRICS) + metrics
    return result
