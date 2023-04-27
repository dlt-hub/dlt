from dlt.common.configuration.specs import RunConfiguration
from dlt.common.runtime.segment import init_segment, disable_segment

from dlt.common.runtime.sentry import init_sentry, disable_sentry


_TELEMETRY_ENABLED = False


def start_telemetry(config: RunConfiguration) -> None:
    # enable telemetry only once

    global _TELEMETRY_ENABLED
    if _TELEMETRY_ENABLED:
        return

    if config.sentry_dsn:
        init_sentry(config)

    if config.dlthub_telemetry:
        init_segment(config)

    _TELEMETRY_ENABLED = True


def stop_telemetry() -> None:
    global _TELEMETRY_ENABLED
    if not _TELEMETRY_ENABLED:
        return

    disable_sentry()
    disable_segment()

    _TELEMETRY_ENABLED = False
