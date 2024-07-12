import pytest

from dlt.common.destination.reference import RunnableLoadJob
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.destinations.job_impl import FinalizedLoadJob


def test_instantiate_job() -> None:
    file_name = "table.1234.0.jsonl"
    file_path = "/path/" + file_name

    class SomeJob(RunnableLoadJob):
        def run(self) -> None:
            pass

    j = SomeJob(None, file_path)
    assert j._file_name == file_name
    assert j._file_path == file_path

    # providing only a filename is not allowed
    with pytest.raises(AssertionError):
        SomeJob(None, file_name)


def test_runnable_job_results() -> None:
    file_path = "/table.1234.0.jsonl"

    class SuccessfulJob(RunnableLoadJob):
        def run(self) -> None:
            5 + 5

    j: RunnableLoadJob = SuccessfulJob(None, file_path)
    assert j.state() == "ready"
    j.run_managed()
    assert j.state() == "completed"

    class RandomExceptionJob(RunnableLoadJob):
        def run(self) -> None:
            raise Exception("Oh no!")

    j = RandomExceptionJob(None, file_path)
    assert j.state() == "ready"
    j.run_managed()
    assert j.state() == "retry"
    assert j.exception() == "Oh no!"

    class TerminalJob(RunnableLoadJob):
        def run(self) -> None:
            raise DestinationTerminalException("Oh no!")

    j = TerminalJob(None, file_path)
    assert j.state() == "ready"
    j.run_managed()
    assert j.state() == "failed"
    assert j.exception() == "Oh no!"


def test_finalized_load_job() -> None:
    file_name = "table.1234.0.jsonl"
    file_path = "/path/" + file_name
    j = FinalizedLoadJob(file_path)
    assert j.state() == "completed"
    assert not j.exception()

    j = FinalizedLoadJob(file_path, "failed", "oh no!")
    assert j.state() == "failed"
    assert j.exception() == "oh no!"

    # only actionable / terminal states are allowed
    with pytest.raises(AssertionError):
        FinalizedLoadJob(file_path, "ready")
