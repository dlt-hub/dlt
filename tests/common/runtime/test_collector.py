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


def test_log_collector_counter_start_time_set_on_registration() -> None:
    # registering a counter with inc=0 fixes its start_time so a later first
    # increment after a long wait does not produce an astronomical rate (#3518)
    clock = [0.0]
    with LogCollector(dump_system_stats=False)("test") as collector:
        collector._clock = lambda: clock[0]  # type: ignore[assignment]
        # register counter at t=0 before any item arrives
        collector.update("slow_table", inc=0)
        assert collector.counter_info["slow_table"].start_time == 0.0
        # simulate a long wait for the first (and only) response
        clock[0] = 100.0
        collector.update("slow_table", inc=5)
        # start_time must still reflect registration time, not first increment
        assert collector.counter_info["slow_table"].start_time == 0.0
        info = collector.counter_info["slow_table"]
        elapsed = collector._clock() - info.start_time
        assert elapsed == 100.0


def test_log_collector_start_time_late_without_registration() -> None:
    # control: without the inc=0 pre-registration start_time is set on first
    # increment, which is the buggy behavior the fix avoids
    clock = [0.0]
    with LogCollector(dump_system_stats=False)("test") as collector:
        collector._clock = lambda: clock[0]  # type: ignore[assignment]
        clock[0] = 100.0
        collector.update("slow_table", inc=5)
        assert collector.counter_info["slow_table"].start_time == 100.0


def test_collector_discard_is_noop_by_default() -> None:
    # the base Collector exposes discard() so extractors can call it on any
    # collector implementation; only LogCollector actually removes a counter
    with DictCollector()("test") as collector:
        collector.update("some_table", inc=1)
        collector.discard("some_table")
        assert collector.counters["some_table"] == 1

    with LogCollector(dump_system_stats=False)("test") as collector:
        collector.update("some_table", inc=1)
        collector.discard("some_table")
        assert "some_table" not in collector.counters
        assert "some_table" not in collector.counter_info
        # discarding an unknown counter is a no-op, not a KeyError
        collector.discard("never_registered")
