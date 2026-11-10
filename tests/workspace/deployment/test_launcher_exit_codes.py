"""Exit status of a failed job or agent launcher run.

Code 1 is the default for a terminal failure and for any failure that is not
explicitly transient. Code 75 is only for an explicitly transient failure.
"""

from typing import Any, Callable, List

import pytest

from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.destination.exceptions import DestinationTransientException
from dlt.common.exceptions import (
    SignalReceivedException,
    TerminalException,
    TerminalValueError,
    TransientException,
)
from dlt._workspace.deployment.exceptions import JobAbortedException
from dlt._workspace.deployment.launchers._launcher import (
    EXIT_TERMINAL_ERROR,
    EXIT_TRANSIENT_ERROR,
)
from dlt._workspace.deployment.launchers.job import run_and_print_result
from dlt._workspace.deployment.typing import TJobResult


class _TerminalAndTransient(ValueError, TerminalException, TransientException):
    """One exception carrying both markers. Terminal wins."""


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


@_expect_exit(EXIT_TERMINAL_ERROR, expect_stderr="ConfigurationException")
def _raise_configuration() -> None:
    raise ConfigurationException("missing")


@_expect_exit(EXIT_TERMINAL_ERROR, expect_stderr="RuntimeError")
def _raise_unknown() -> None:
    raise RuntimeError("plain")


@_expect_exit(EXIT_TERMINAL_ERROR, expect_stderr="ValueError")
def _raise_value_error() -> None:
    raise ValueError("plain")


@_expect_exit(EXIT_TERMINAL_ERROR, expect_stderr="AssertionError")
def _raise_assertion() -> None:
    raise AssertionError("check")


@_expect_exit(EXIT_TRANSIENT_ERROR, expect_stderr="DestinationTransientException")
def _raise_transient() -> None:
    raise DestinationTransientException("retry")


@_expect_exit(EXIT_TERMINAL_ERROR, expect_stderr="_TerminalAndTransient")
def _raise_both_markers() -> None:
    raise _TerminalAndTransient("both")


@_expect_exit(EXIT_TRANSIENT_ERROR, expect_stderr="DestinationTransientException")
def _raise_cause_transient() -> None:
    try:
        raise DestinationTransientException("retry")
    except DestinationTransientException as exc:
        raise RuntimeError("wrapped") from exc


@_expect_exit(EXIT_TRANSIENT_ERROR, expect_stderr="DestinationTransientException")
def _raise_context_transient() -> None:
    try:
        raise DestinationTransientException("retry")
    except DestinationTransientException:
        raise RuntimeError("during")


@_expect_exit(EXIT_TRANSIENT_ERROR, expect_stderr="DestinationTransientException")
def _raise_deep_wrapped_transient() -> None:
    try:
        raise DestinationTransientException("retry")
    except DestinationTransientException as first:
        try:
            raise OSError("mid") from first
        except OSError as mid:
            raise KeyError("outer") from mid


@_expect_exit(EXIT_TRANSIENT_ERROR, expect_stderr="DestinationTransientException")
def _raise_shared_cause_and_context() -> None:
    inner = DestinationTransientException("retry")
    outer = RuntimeError("outer")
    outer.__cause__ = inner
    outer.__context__ = inner
    outer.__suppress_context__ = False
    raise outer


@_expect_exit(EXIT_TERMINAL_ERROR, expect_stderr="TerminalValueError")
def _raise_terminal_cause() -> None:
    try:
        raise TerminalValueError("stop")
    except TerminalValueError as exc:
        raise DestinationTransientException("retry") from exc


@_expect_exit(EXIT_TERMINAL_ERROR, expect_stderr="TerminalValueError")
def _raise_terminal_outer() -> None:
    try:
        raise DestinationTransientException("retry")
    except DestinationTransientException as exc:
        raise TerminalValueError("stop") from exc


@_expect_exit(EXIT_TRANSIENT_ERROR, expect_stderr="DestinationTransientException")
def _raise_cause_hides_terminal_context() -> None:
    # a reported cause replaces the context, including a terminal one
    outer = RuntimeError("outer")
    outer.__cause__ = DestinationTransientException("retry")
    outer.__context__ = TerminalValueError("stop")
    outer.__suppress_context__ = False
    raise outer


@_expect_exit(EXIT_TERMINAL_ERROR, expect_stderr="TerminalValueError")
def _raise_terminal_under_transient_cause() -> None:
    try:
        raise TerminalValueError("stop")
    except TerminalValueError:
        try:
            raise DestinationTransientException("retry")
        except DestinationTransientException as transient:
            raise RuntimeError("outer") from transient


@_expect_exit(EXIT_TERMINAL_ERROR, expect_stderr="RuntimeError")
def _raise_suppressed_transient_context() -> None:
    try:
        raise DestinationTransientException("retry")
    except DestinationTransientException:
        raise RuntimeError("unrelated") from None


@_expect_exit(EXIT_TRANSIENT_ERROR, expect_stderr="DestinationTransientException")
def _raise_suppressed_terminal_context() -> None:
    try:
        raise TerminalValueError("stop")
    except TerminalValueError:
        raise DestinationTransientException("retry") from None


@_expect_exit(EXIT_TERMINAL_ERROR, expect_stderr="SignalReceivedException")
def _raise_wrapped_signal() -> None:
    try:
        raise SignalReceivedException(15)
    except SignalReceivedException as exc:
        raise RuntimeError("extract failed") from exc


@_expect_exit(EXIT_TRANSIENT_ERROR, expect_stderr="DestinationTransientException")
def _raise_cyclic_transient() -> None:
    outer = RuntimeError("outer")
    inner = DestinationTransientException("retry")
    outer.__cause__ = inner
    inner.__cause__ = outer
    raise outer


@_expect_exit(EXIT_TERMINAL_ERROR, expect_stderr="TerminalValueError")
def _raise_cyclic_terminal_and_transient() -> None:
    outer = DestinationTransientException("retry")
    inner = TerminalValueError("stop")
    outer.__cause__ = inner
    inner.__context__ = outer
    inner.__suppress_context__ = False
    raise outer


@_expect_exit(EXIT_TERMINAL_ERROR, expect_stderr="RuntimeError")
def _raise_cyclic_unknown() -> None:
    outer = RuntimeError("outer")
    inner = ValueError("inner")
    outer.__cause__ = inner
    inner.__context__ = outer
    raise outer


@_expect_exit(EXIT_TRANSIENT_ERROR, expect_stderr="DestinationTransientException")
def _raise_self_cycle_transient() -> None:
    exc = DestinationTransientException("retry")
    exc.__cause__ = exc
    raise exc


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
