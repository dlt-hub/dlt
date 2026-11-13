"""Exit status of a failed job or agent launcher run.

Code 1 is the default. Code 75 is only for a connection or timeout failure.
"""

from typing import Any, Callable, List

import pytest
import requests

import dlt
from dlt.common.destination.exceptions import DestinationTransientException
from dlt.common.exceptions import (
    SignalReceivedException,
    TerminalException,
    TerminalValueError,
    TransientException,
)
from dlt.common.utils import uniq_id
from dlt._workspace.deployment.exceptions import JobAbortedException
from dlt._workspace.deployment.launchers._launcher import (
    EXIT_TERMINAL_ERROR,
    EXIT_TRANSIENT_ERROR,
)
from dlt._workspace.deployment.launchers.job import run_and_print_result
from dlt._workspace.deployment.typing import TJobResult


_FAILURE_CASES: List[Any] = []


def _expect_exit(
    exit_code: int, *, expect_stderr: str
) -> Callable[[Callable[[], None]], Callable[[], None]]:
    def _decorate(run: Callable[[], None]) -> Callable[[], None]:
        case_id = run.__name__.removeprefix("_raise_").replace("_", "-")
        _FAILURE_CASES.append(pytest.param(run, exit_code, expect_stderr, id=case_id))
        return run

    return _decorate


@_expect_exit(EXIT_TERMINAL_ERROR, expect_stderr="TerminalValueError")
def _raise_terminal() -> None:
    raise TerminalValueError("stop")


@_expect_exit(EXIT_TERMINAL_ERROR, expect_stderr="DestinationTransientException")
def _raise_transient_label() -> None:
    raise DestinationTransientException("retry")


@_expect_exit(EXIT_TRANSIENT_ERROR, expect_stderr="TimeoutError")
def _raise_timeout() -> None:
    raise TimeoutError("read timed out")


@_expect_exit(EXIT_TRANSIENT_ERROR, expect_stderr="ConnectionError")
def _raise_connection_error() -> None:
    raise ConnectionError("connection reset by peer")


@_expect_exit(EXIT_TRANSIENT_ERROR, expect_stderr="ConnectionError")
def _raise_requests_connection_error() -> None:
    raise requests.ConnectionError("connection reset by peer")


@_expect_exit(EXIT_TRANSIENT_ERROR, expect_stderr="Timeout")
def _raise_requests_timeout() -> None:
    raise requests.Timeout("read timed out")


@_expect_exit(EXIT_TRANSIENT_ERROR, expect_stderr="TimeoutError")
def _raise_wrapped_timeout() -> None:
    try:
        raise TimeoutError("read timed out")
    except TimeoutError as exc:
        raise RuntimeError("during extract") from exc


@_expect_exit(EXIT_TERMINAL_ERROR, expect_stderr="TerminalValueError")
def _raise_terminal_over_timeout() -> None:
    try:
        raise TimeoutError("read timed out")
    except TimeoutError as exc:
        raise TerminalValueError("stop") from exc


@_expect_exit(EXIT_TERMINAL_ERROR, expect_stderr="TerminalValueError")
def _raise_timeout_over_terminal() -> None:
    try:
        raise TerminalValueError("stop")
    except TerminalValueError as exc:
        raise TimeoutError("read timed out") from exc


@_expect_exit(EXIT_TERMINAL_ERROR, expect_stderr="KeyError")
def _raise_destination_coding_bug() -> None:
    @dlt.destination(name="sink")
    def sink(items, table):
        raise KeyError("a coding bug")

    dlt.pipeline(uniq_id(), destination=sink, dev_mode=True).run([{"a": 1}], table_name="t")


@_expect_exit(EXIT_TERMINAL_ERROR, expect_stderr="ParserException")
def _raise_sql_syntax_error() -> None:
    pipeline = dlt.pipeline(uniq_id(), destination="duckdb", dev_mode=True)
    pipeline.run([{"a": 1}], table_name="t")
    with pipeline.sql_client() as client:
        client.execute_sql("SELEC a FROM t")


@_expect_exit(EXIT_TRANSIENT_ERROR, expect_stderr="ConnectionError")
def _raise_connection_reset() -> None:
    @dlt.resource
    def api():
        raise requests.ConnectionError("connection reset by peer")
        yield

    dlt.pipeline(uniq_id(), destination="duckdb", dev_mode=True).run(api())


@_expect_exit(EXIT_TRANSIENT_ERROR, expect_stderr="TimeoutError")
def _raise_read_timeout() -> None:
    @dlt.resource
    def api():
        raise TimeoutError("read timed out")
        yield

    dlt.pipeline(uniq_id(), destination="duckdb", dev_mode=True).run(api())


def test_transient_exit_matches_ex_tempfail() -> None:
    assert EXIT_TRANSIENT_ERROR == 75


@pytest.mark.parametrize(("run", "exit_code", "stderr_text"), _FAILURE_CASES)
def test_failed_run_exits_with_the_classified_code(
    run: Callable[[], Any],
    exit_code: int,
    stderr_text: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as caught:
        run_and_print_result(run)

    assert caught.value.code == exit_code
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Traceback (most recent call last):" in captured.err
    assert stderr_text in captured.err


def test_successful_run_prints_the_return_value(capsys: pytest.CaptureFixture[str]) -> None:
    run_and_print_result(lambda: "backfill_done")

    captured = capsys.readouterr()
    assert captured.out.strip() == "backfill_done"
    assert captured.err == ""


def test_successful_run_without_a_value_prints_nothing(capsys: pytest.CaptureFixture[str]) -> None:
    run_and_print_result(lambda: None)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_aborted_run_is_reraised(capsys: pytest.CaptureFixture[str]) -> None:
    result: TJobResult = {
        "type": "job.etl_summary",
        "engine_version": 1,
        "result": {"rows": 1},
    }
    aborted = JobAbortedException("stopped on purpose", result)

    def _run() -> None:
        raise aborted

    with pytest.raises(JobAbortedException, match="stopped on purpose") as caught:
        run_and_print_result(_run)

    assert caught.value is aborted
    captured = capsys.readouterr()
    assert "job.etl_summary" in captured.out
    assert '"rows": 1' in captured.out
    assert "Traceback" not in captured.err


def _raiser(exc: BaseException) -> Callable[[], None]:
    def _run() -> None:
        raise exc

    return _run


@pytest.mark.parametrize(
    "exc",
    [
        pytest.param(KeyboardInterrupt(), id="keyboard-interrupt"),
        pytest.param(GeneratorExit(), id="generator-exit"),
        pytest.param(SignalReceivedException(15), id="signal"),
        pytest.param(TerminalException("marker"), id="bare-terminal-marker"),
        pytest.param(TransientException("marker"), id="bare-transient-marker"),
    ],
)
def test_base_exception_signals_propagate(
    exc: BaseException, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(type(exc)) as caught:
        run_and_print_result(_raiser(exc))

    assert caught.value is exc
    assert "Traceback" not in capsys.readouterr().err


@pytest.mark.parametrize(
    "code",
    [pytest.param(0, id="zero"), pytest.param(7, id="seven"), pytest.param("bye", id="message")],
)
def test_system_exit_keeps_its_status(code: object, capsys: pytest.CaptureFixture[str]) -> None:
    exc = SystemExit(code)

    with pytest.raises(SystemExit) as caught:
        run_and_print_result(_raiser(exc))

    assert caught.value is exc
    assert caught.value.code == code
    assert "Traceback" not in capsys.readouterr().err
