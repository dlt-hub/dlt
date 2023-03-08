from typing import NamedTuple, TypedDict


class TRunHealth(TypedDict):
    # count runs
    runs_count: int
    # count not idle runs
    runs_not_idle_count: int
    # successful runs
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