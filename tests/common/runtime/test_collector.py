from collections import defaultdict

import pytest
from dlt.common.runtime.collector import NullCollector, DictCollector, Collector


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
