"""Exit status of a failed job or agent launcher run.

Code 1 is the default for a terminal failure and for any failure that is not
explicitly transient. Code 2 is only for an explicitly transient failure.
"""

from typing import Any, Callable

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


def _raise_terminal() -> None:
    raise TerminalValueError("stop")


def _raise_configuration() -> None:
    raise ConfigurationException("missing")


def _raise_unknown() -> None:
    raise RuntimeError("plain")


def _raise_value_error() -> None:
    raise ValueError("plain")


def _raise_assertion() -> None:
    raise AssertionError("check")


def _raise_transient() -> None:
    raise DestinationTransientException("retry")


def _raise_both_markers() -> None:
    raise _TerminalAndTransient("both")


def _raise_cause_transient() -> None:
    try:
        raise DestinationTransientException("retry")
    except DestinationTransientException as exc:
        raise RuntimeError("wrapped") from exc


def _raise_context_transient() -> None:
    try:
        raise DestinationTransientException("retry")
    except DestinationTransientException:
        raise RuntimeError("during")


def _raise_deep_wrapped_transient() -> None:
    try:
        raise DestinationTransientException("retry")
    except DestinationTransientException as first:
        try:
            raise OSError("mid") from first
        except OSError as mid:
            raise KeyError("outer") from mid


def _raise_terminal_cause() -> None:
    try:
        raise TerminalValueError("stop")
    except TerminalValueError as exc:
        raise DestinationTransientException("retry") from exc


def _raise_terminal_outer() -> None:
    try:
        raise DestinationTransientException("retry")
    except DestinationTransientException as exc:
        raise TerminalValueError("stop") from exc


def _raise_cause_hides_terminal_context() -> None:
    # a reported cause replaces the context, including a terminal one
    outer = RuntimeError("outer")
    outer.__cause__ = DestinationTransientException("retry")
    outer.__context__ = TerminalValueError("stop")
    outer.__suppress_context__ = False
    raise outer


def _raise_terminal_under_transient_cause() -> None:
    try:
        raise TerminalValueError("stop")
    except TerminalValueError:
        try:
            raise DestinationTransientException("retry")
        except DestinationTransientException as transient:
            raise RuntimeError("outer") from transient


def _raise_suppressed_transient_context() -> None:
    try:
        raise DestinationTransientException("retry")
    except DestinationTransientException:
        raise RuntimeError("unrelated") from None


def _raise_suppressed_terminal_context() -> None:
    try:
        raise TerminalValueError("stop")
    except TerminalValueError:
        raise DestinationTransientException("retry") from None


def _raise_wrapped_signal() -> None:
    try:
        raise SignalReceivedException(15)
    except SignalReceivedException as exc:
        raise RuntimeError("extract failed") from exc


def _raise_cyclic_transient() -> None:
    outer = RuntimeError("outer")
    inner = DestinationTransientException("retry")
    outer.__cause__ = inner
    inner.__cause__ = outer
    raise outer


def _raise_cyclic_terminal_and_transient() -> None:
    outer = DestinationTransientException("retry")
    inner = TerminalValueError("stop")
    outer.__cause__ = inner
    inner.__context__ = outer
    inner.__suppress_context__ = False
    raise outer


def _raise_cyclic_unknown() -> None:
    outer = RuntimeError("outer")
    inner = ValueError("inner")
    outer.__cause__ = inner
    inner.__context__ = outer
    raise outer


def _raise_self_cycle_transient() -> None:
    exc = DestinationTransientException("retry")
    exc.__cause__ = exc
    raise exc


def _raise_shared_link() -> None:
    inner = DestinationTransientException("retry")
    outer = RuntimeError("outer")
    outer.__cause__ = inner
    outer.__context__ = inner
    outer.__suppress_context__ = False
    raise outer


@pytest.mark.parametrize(
    ("run", "exit_code", "stderr_text"),
    [
        pytest.param(_raise_terminal, EXIT_TERMINAL_ERROR, "TerminalValueError", id="terminal"),
        pytest.param(
            _raise_configuration,
            EXIT_TERMINAL_ERROR,
            "ConfigurationException",
            id="configuration",
        ),
        pytest.param(_raise_unknown, EXIT_TERMINAL_ERROR, "RuntimeError", id="unknown"),
        pytest.param(_raise_value_error, EXIT_TERMINAL_ERROR, "ValueError", id="value-error"),
        pytest.param(_raise_assertion, EXIT_TERMINAL_ERROR, "AssertionError", id="assertion"),
        pytest.param(
            _raise_transient,
            EXIT_TRANSIENT_ERROR,
            "DestinationTransientException",
            id="transient",
        ),
        pytest.param(
            _raise_both_markers, EXIT_TERMINAL_ERROR, "_TerminalAndTransient", id="both-markers"
        ),
        # wrapped failures
        pytest.param(
            _raise_cause_transient,
            EXIT_TRANSIENT_ERROR,
            "DestinationTransientException",
            id="cause-transient",
        ),
        pytest.param(
            _raise_context_transient,
            EXIT_TRANSIENT_ERROR,
            "DestinationTransientException",
            id="context-transient",
        ),
        pytest.param(
            _raise_deep_wrapped_transient,
            EXIT_TRANSIENT_ERROR,
            "DestinationTransientException",
            id="deep-wrapped-transient",
        ),
        pytest.param(
            _raise_shared_link,
            EXIT_TRANSIENT_ERROR,
            "DestinationTransientException",
            id="shared-cause-and-context",
        ),
        # a terminal failure in the chain wins over a transient one
        pytest.param(
            _raise_terminal_cause,
            EXIT_TERMINAL_ERROR,
            "TerminalValueError",
            id="terminal-cause",
        ),
        pytest.param(
            _raise_terminal_outer,
            EXIT_TERMINAL_ERROR,
            "TerminalValueError",
            id="terminal-outer",
        ),
        pytest.param(
            _raise_cause_hides_terminal_context,
            EXIT_TRANSIENT_ERROR,
            "DestinationTransientException",
            id="cause-hides-terminal-context",
        ),
        pytest.param(
            _raise_terminal_under_transient_cause,
            EXIT_TERMINAL_ERROR,
            "TerminalValueError",
            id="terminal-under-transient-cause",
        ),
        pytest.param(
            _raise_wrapped_signal,
            EXIT_TERMINAL_ERROR,
            "SignalReceivedException",
            id="wrapped-signal",
        ),
        # `raise ... from None` drops the handled exception from the chain
        pytest.param(
            _raise_suppressed_transient_context,
            EXIT_TERMINAL_ERROR,
            "RuntimeError",
            id="suppressed-transient-context",
        ),
        pytest.param(
            _raise_suppressed_terminal_context,
            EXIT_TRANSIENT_ERROR,
            "DestinationTransientException",
            id="suppressed-terminal-context",
        ),
        # cyclic cause/context chains are classified once and do not hang
        pytest.param(
            _raise_cyclic_transient,
            EXIT_TRANSIENT_ERROR,
            "DestinationTransientException",
            id="cyclic-transient",
        ),
        pytest.param(
            _raise_cyclic_terminal_and_transient,
            EXIT_TERMINAL_ERROR,
            "TerminalValueError",
            id="cyclic-terminal-and-transient",
        ),
        pytest.param(
            _raise_cyclic_unknown, EXIT_TERMINAL_ERROR, "RuntimeError", id="cyclic-unknown"
        ),
        pytest.param(
            _raise_self_cycle_transient,
            EXIT_TRANSIENT_ERROR,
            "DestinationTransientException",
            id="self-cycle-transient",
        ),
    ],
)
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
