from typing import Iterable, Sequence, TypedDict, NamedTuple
from prometheus_client import Gauge
from prometheus_client.metrics import MetricWrapperBase

from dlt.common.typing import DictStrAny, StrAny


class TRunHealth(TypedDict):
    # count runs
    runs_count: int
    # count not idle runs
    runs_not_idle_count: int
    # succesfull runs
    runs_healthy_count: int
    # count consecutive successful runs
    runs_cs_healthy_gauge: int
    # count failed runs
    runs_failed_count: int
    # count consecutive failed runs
    runs_cs_failed_gauge: int
    # number of items pending at the end of the run
    runs_pending_items_gauge: int


class TRunMetrics(NamedTuple):
    was_idle: bool
    has_failed: bool
    pending_items: int


def get_metrics_from_prometheus(gauges: Iterable[MetricWrapperBase]) -> StrAny:
    metrics: DictStrAny = {}
    for g in gauges:
        name = g._name
        if g._is_parent():
            # for gauges containing many label values, enumerate all
            metrics.update(get_metrics_from_prometheus([g.labels(*label) for label in g._metrics.keys()]))
            continue
        # for gauges with labels: add the label to the name and enumerate samples
        if g._labelvalues:
            name += "_" + "_".join(g._labelvalues)
        for m in g._child_samples():
            k = name
            if m[0] == "_created":
                continue
            if m[0] != "_total":
                k += m[0]
            if g._type == "info":
                # actual descriptive value is held in [1], [2] is a placeholder in info
                metrics[k] = m[1]
            else:
                metrics[k] = m[2]
    return metrics


def set_gauge_all_labels(gauge: Gauge, value: float) -> None:
    if gauge._is_parent():
        for label in gauge._metrics.keys():
            set_gauge_all_labels(gauge.labels(*label), value)
    else:
        gauge.set(value)


def get_logging_extras(gauges: Iterable[MetricWrapperBase]) -> StrAny:
    return {"metrics": get_metrics_from_prometheus(gauges)}
