import io
from collections import defaultdict

import pytest
from dlt.common.runtime.collector import NullCollector, DictCollector, LogCollector, Collector


def test_null_collector() -> None:
    with NullCollector()("hallo") as collector:
        assert collector.step == "hallo"
        collector.update("hey")


def test_dict_collector_update():
    with DictCollector()("test") as collector:
        collector.update("counter1", inc=2)
        assert collector.counters["counter1"] == 2

        collector.update("counter1", inc=3)
        assert collector.counters["counter1"] == 5

        collector.update("counter2")
        assert collector.counters["counter2"] == 1


def test_dict_collector_context_manager():
    with DictCollector()("Processing Step") as collector:
        assert isinstance(collector, Collector)
        assert collector.counters == defaultdict(int)

        collector.update("counter1", inc=3)
        assert collector.counters["counter1"] == 3

    assert collector.counters is None


def test_dict_collector_no_labels():
    with DictCollector()("test") as collector:
        with pytest.raises(AssertionError, match="labels not supported in dict collector"):
            collector.update("counter1", inc=1, label="label1")


def test_dict_collector_reset_counters():
    with DictCollector()("test1") as collector:
        collector.update("counter1", inc=5)
        assert collector.counters["counter1"] == 5

    with DictCollector()("test2") as collector:
        assert collector.counters == defaultdict(int)


def test_log_collector_respects_log_period() -> None:
    # adding more counters do not dump them all immediately
    clock = [0.0]
    buf = io.StringIO()
    collector = LogCollector(log_period=10.0, logger=buf, dump_system_stats=False)
    collector._clock = lambda: clock[0]  # type: ignore[assignment]

    with collector("Extract"):
        # first update logs immediately so the step shows up at once
        collector.update("resource_0", inc=1)
        assert buf.getvalue().count("Extract") == 1
        # many new counters within the same period add no further logs
        for i in range(1, 100):
            collector.update(f"resource_{i}", inc=1)
        assert buf.getvalue().count("Extract") == 1
        # crossing log_period emits exactly one more log
        clock[0] = 10.0
        collector.update("resource_100", inc=1)
        assert buf.getvalue().count("Extract") == 2

    # _stop always emits a final log
    assert buf.getvalue().count("Extract") == 3
