import atexit
import time
import contextlib
import inspect
from typing import Any, Callable

from dlt.common.configuration.specs import RuntimeConfiguration
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TFun
from dlt.common.configuration import resolve_configuration
from dlt.common.runtime.anon_tracker import (
    TEventCategory,
    init_anon_tracker,
    disable_anon_tracker,
    track,
)

_TELEMETRY_STARTED = False


def start_telemetry(config: RuntimeConfiguration) -> None:
    # enable telemetry only once
    if is_telemetry_started():
        return

    if config.sentry_dsn:
        # may raise if sentry is not installed
        from dlt.common.runtime.sentry import init_sentry

        init_sentry(config)

    if config.dlthub_telemetry:
        init_anon_tracker(config)

    if config.dlthub_dsn:
        # TODO: we need pluggable modules for tracing so import into
        # concrete modules is not needed
        from dlt.pipeline.platform import init_platform_tracker

        init_platform_tracker()

    global _TELEMETRY_STARTED
    _TELEMETRY_STARTED = True


@atexit.register
def stop_telemetry() -> None:
    if not is_telemetry_started():
        return

    try:
        from dlt.common.runtime.sentry import disable_sentry

        disable_sentry()
    except (MissingDependencyException, ImportError):
        pass

    disable_anon_tracker()

    from dlt.pipeline.platform import disable_platform_tracker

    disable_platform_tracker()

    global _TELEMETRY_STARTED
    _TELEMETRY_STARTED = False


def is_telemetry_started() -> bool:
    return _TELEMETRY_STARTED


def with_telemetry(
    category: TEventCategory, command: str, track_before: bool, *args: str, **kwargs: Any
) -> Callable[[TFun], TFun]:
    """Decorator factory that attaches telemetry to a callable.

    The returned decorator wraps a function so that an anonymous telemetry event is
    emitted either before execution (if ``track_before`` is True) or after execution.
    When tracked after execution, the event includes whether the call succeeded.
    Telemetry is initialized lazily on first use if it is not already running.

    Args:
        category: telemetry event category used by the anon tracker.
        command: event/command name to report.
        track_before: if True, emit a single event before calling the function.
            if False, emit a single event after the call, including success state.
        *args: names of parameters from the decorated function whose values should
            be included in the event properties. names must match the function
            signature and can refer to positional or keyword parameters.
        **kwargs: additional key-value pairs to include in the event properties.

    Returns:
        a decorator that takes a function and returns a wrapped function with
        telemetry tracking applied. the wrapped function preserves the original
        signature and return value.

    Notes:
        - success is determined as follows when tracking after execution:
          - if the wrapped function returns an int, 0 is treated as success; any
            other value is treated as failure.
          - for non-int returns, success is True unless an exception is raised.
        - on exception, a failure event is emitted and the exception is re-raised.
        - event properties always include elapsed execution time in seconds under
          the 'elapsed' key and the success flag under 'success'.

    """

    def decorator(f: TFun) -> TFun:
        sig: inspect.Signature = inspect.signature(f)

        def _wrap(*f_args: Any, **f_kwargs: Any) -> Any:
            # look for additional arguments in call arguments
            bound_args = sig.bind(*f_args, **f_kwargs)
            props = {p: bound_args.arguments[p] for p in args if p in bound_args.arguments}
            # append additional props from kwargs
            props.update(kwargs)
            start_ts = time.time()

            def _track(success: bool) -> None:
                with contextlib.suppress(Exception):
                    props["elapsed"] = time.time() - start_ts
                    props["success"] = success
                    # resolve runtime config and init telemetry
                    if not _TELEMETRY_STARTED:
                        c = resolve_configuration(RuntimeConfiguration())
                        start_telemetry(c)
                    track(category, command, props)

            # some commands should be tracked before execution
            if track_before:
                _track(True)
                return f(*f_args, **f_kwargs)
            # some commands we track after, where we can pass the success
            try:
                rv = f(*f_args, **f_kwargs)
                # if decorated function returns int, 0 is a success - used to track dlt commands
                if isinstance(rv, int):
                    success = rv == 0
                else:
                    success = True
                _track(success)
                return rv
            except Exception:
                _track(False)
                raise

        return _wrap  # type: ignore

    return decorator
