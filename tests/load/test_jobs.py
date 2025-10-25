import pytest

from dlt.common import pendulum
from dlt.common.destination.client import RunnableLoadJob
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.destinations.job_impl import FinalizedLoadJob


def test_instantiate_job() -> None:
    file_name = "table.1234.0.jsonl"
    file_path = "/path/" + file_name

    class SomeJob(RunnableLoadJob):
        def run(self) -> None:
            pass

    j = SomeJob(file_path)
    assert j._file_name == file_name
    assert j._file_path == file_path

    # providing only a filename is not allowed
    with pytest.raises(AssertionError):
        SomeJob(file_name)


def test_runnable_job_results() -> None:
    file_path = "/table.1234.0.jsonl"

    class MockClient:
        def prepare_load_job_execution(self, j: RunnableLoadJob):
            pass

    class SuccessfulJob(RunnableLoadJob):
        def run(self) -> None:
            5 + 5

    j: RunnableLoadJob = SuccessfulJob(file_path)
    assert j.state() == "ready"
    metrics = j.metrics()
    assert metrics.table_name == "table"
    assert metrics.state == "ready"
    assert metrics.started_at is not None
    assert metrics.finished_at is None
    assert metrics.retry_count == 0

    j.run_managed(MockClient(), None)  # type: ignore
    assert j.state() == "completed"
    metrics_2 = j.metrics()
    assert metrics_2.state == "completed"
    assert metrics.started_at == metrics_2.started_at
    assert metrics_2.finished_at is not None

    class RandomExceptionJob(RunnableLoadJob):
        def run(self) -> None:
            raise Exception("Oh no!")

    j = RandomExceptionJob(file_path)
    assert j.state() == "ready"
    j.run_managed(MockClient(), None)  # type: ignore
    assert j.state() == "retry"
    assert j.exception() == "Oh no!"
    metrics_3 = j.metrics()
    assert metrics_3.state == "retry"
    assert metrics_3.started_at is not None
    assert metrics_3.finished_at is None
    # must change file name to increase retry count
    assert metrics_3.retry_count == 0

    class TerminalJob(RunnableLoadJob):
        def run(self) -> None:
            raise DestinationTerminalException("Oh no!")

    j = TerminalJob(file_path)
    assert j.state() == "ready"
    j.run_managed(MockClient(), None)  # type: ignore
    assert j.state() == "failed"
    assert j.exception() == "Oh no!"
    metrics_4 = j.metrics()
    assert metrics_4.state == "failed"
    assert metrics_4.started_at is not None
    assert metrics_4.finished_at is not None
    assert metrics_4.retry_count == 0


def test_finalized_load_job() -> None:
    file_name = "table.1234.1.jsonl"
    file_path = "/path/" + file_name
    j = FinalizedLoadJob(file_path)
    assert j.state() == "completed"
    assert not j.exception()
    assert isinstance(j._started_at, pendulum.DateTime)
    assert isinstance(j._finished_at, pendulum.DateTime)

    j = FinalizedLoadJob(file_path, status="failed", exception="oh no!")
    assert j.state() == "failed"
    assert j.exception() == "oh no!"
    # start and finish dates will be automatically set for terminal states
    assert isinstance(j._started_at, pendulum.DateTime)
    assert isinstance(j._finished_at, pendulum.DateTime)
    metrics = j.metrics()
    assert metrics.table_name == "table"
    assert metrics.finished_at == j._finished_at
    assert metrics.started_at == j._started_at
    assert metrics.state == "failed"
    assert metrics.retry_count == 1

    j = FinalizedLoadJob(file_path, status="retry")
    assert isinstance(j._started_at, pendulum.DateTime)
    assert j._finished_at is None

    # explicit start and finish
    started_at = pendulum.now().subtract(days=-1)
    finished_at = pendulum.now().subtract(days=1)
    j = FinalizedLoadJob(file_path, status="retry", started_at=started_at, finished_at=finished_at)
    assert isinstance(j._started_at, pendulum.DateTime)
    assert j._finished_at is finished_at
    assert j._started_at is started_at

    # only actionable / terminal states are allowed
    with pytest.raises(AssertionError):
        FinalizedLoadJob(file_path, status="ready")
