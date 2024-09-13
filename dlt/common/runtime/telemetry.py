import atexit
import time
import contextlib
import inspect
from typing import Any, Callable

from dlt.common.configuration.specs import RunConfiguration
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


def start_telemetry(config: RunConfiguration) -> None:
    # enable telemetry only once

    global _TELEMETRY_STARTED
    if _TELEMETRY_STARTED:
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

    _TELEMETRY_STARTED = True


@atexit.register
def stop_telemetry() -> None:
    global _TELEMETRY_STARTED
    if not _TELEMETRY_STARTED:
        return

    try:
        from dlt.common.runtime.sentry import disable_sentry

        disable_sentry()
    except (MissingDependencyException, ImportError):
        pass

    disable_anon_tracker()

    from dlt.pipeline.platform import disable_platform_tracker

    disable_platform_tracker()

    _TELEMETRY_STARTED = False


def is_telemetry_started() -> bool:
    return _TELEMETRY_STARTED


def with_telemetry(
    category: TEventCategory, command: str, track_before: bool, *args: str
) -> Callable[[TFun], TFun]:
    """Adds telemetry to f: TFun and add optional f *args values to `properties` of telemetry event"""

    def decorator(f: TFun) -> TFun:
        sig: inspect.Signature = inspect.signature(f)

        def _wrap(*f_args: Any, **f_kwargs: Any) -> Any:
            # look for additional arguments
            bound_args = sig.bind(*f_args, **f_kwargs)
            props = {p: bound_args.arguments[p] for p in args if p in bound_args.arguments}
            start_ts = time.time()

            def _track(success: bool) -> None:
                with contextlib.suppress(Exception):
                    props["elapsed"] = time.time() - start_ts
                    props["success"] = success
                    # resolve runtime config and init telemetry
                    if not _TELEMETRY_STARTED:
                        c = resolve_configuration(RunConfiguration())
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
