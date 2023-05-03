from typing import NamedTuple


class TRunMetrics(NamedTuple):
    was_idle: bool
    pending_items: int
