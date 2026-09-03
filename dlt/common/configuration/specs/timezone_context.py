import datetime  # noqa: I251
from typing import ClassVar, List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError, available_timezones

from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration.specs.base_configuration import (
    ContainerInjectableContext,
    configspec,
)
from dlt.common.time import UTC_NAME, get_context_timezone_name, set_context_timezone


class InvalidTimezoneName(ConfigurationValueError):
    def __init__(self, timezone: str, reason: str) -> None:
        self.timezone = timezone
        super().__init__(
            f"dlt cannot use timezone `{timezone}`: {reason}. Pass a canonical IANA name, for"
            " example `Europe/Berlin` or `UTC`."
        )


def to_tzinfo(timezone: str) -> datetime.tzinfo:
    """Resolves an IANA name, rejecting anything `zoneinfo` and arrow cannot both use."""
    if not timezone:
        raise InvalidTimezoneName(timezone, "the name is empty")
    if timezone[0] in "+-":
        raise InvalidTimezoneName(timezone, "a fixed offset is not a timezone")
    if timezone == UTC_NAME:
        # the stdlib singleton, so `== timezone.utc` holds and offsets need no lookup
        return datetime.timezone.utc
    # `ZoneInfo` alone accepts `utc` where the filesystem is case-insensitive, as macOS is, while
    # arrow keeps rejecting it. only the canonical spellings are portable
    if timezone not in available_timezones():
        raise InvalidTimezoneName(timezone, "no such timezone")
    try:
        return ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, ValueError) as ex:
        raise InvalidTimezoneName(timezone, str(ex) or "no such timezone") from ex


@configspec
class TimezoneContext(ContainerInjectableContext):
    """Context timezone, the one `dlt` stores loaded values in.

    Process-wide and scoped to the run, UTC by default.
    """

    # a job runs in one process, so this is not per-thread. `worker_affinity` carries it
    # into the normalize process pool
    global_affinity: ClassVar[bool] = True
    worker_affinity: ClassVar[bool] = True
    can_create_default: ClassVar[bool] = True

    timezone: str = None

    def __init__(self, timezone: str = None) -> None:
        super().__init__()
        # a default instance must not undo the timezone a launcher already put in the environment
        self.timezone = timezone or get_context_timezone_name()
        self._tzinfo: Optional[datetime.tzinfo] = None
        self._restore: List[datetime.tzinfo] = []

    def on_resolved(self) -> None:
        # resolve eagerly: `pyarrow.timestamp` takes any string, so an unusable name would
        # otherwise surface much later, when a naive column needs localizing
        self._tzinfo = to_tzinfo(self.timezone)

    @property
    def tzinfo(self) -> datetime.tzinfo:
        """Resolved timezone."""
        if self._tzinfo is None:
            self._tzinfo = to_tzinfo(self.timezone)
        return self._tzinfo

    def after_add(self) -> None:
        super().after_add()
        self._restore.append(set_context_timezone(self.tzinfo))

    def before_remove(self) -> None:
        super().before_remove()
        if self._restore:
            set_context_timezone(self._restore.pop())
