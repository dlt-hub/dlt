from requests import Session as BaseSession
from typing import Optional, TYPE_CHECKING, Union, Tuple, TypeVar

from dlt.sources.helpers.requests.typing import TRequestTimeout
from dlt.common.typing import TimedeltaSeconds
from dlt.common.time import to_seconds
from dlt.version import __version__


TSession = TypeVar("TSession", bound=BaseSession)


DEFAULT_TIMEOUT = 60


def _timeout_to_seconds(timeout: TRequestTimeout) -> Optional[Union[Tuple[float, float], float]]:
    return (
        (to_seconds(timeout[0]), to_seconds(timeout[1]))
        if isinstance(timeout, tuple)
        else to_seconds(timeout)
    )


class Session(BaseSession):
    """Requests session which by default adds a timeout to all requests and calls `raise_for_status()` on response

    Args:
        timeout: Timeout for requests in seconds. May be passed as `timedelta` or `float/int` number of seconds.
            May be a single value or a tuple for separate (connect, read) timeout.
        raise_for_status: Whether to raise exception on error status codes (using `response.raise_for_status()`)
    """

    def __init__(
        self,
        timeout: Optional[
            Union[TimedeltaSeconds, Tuple[TimedeltaSeconds, TimedeltaSeconds]]
        ] = DEFAULT_TIMEOUT,
        raise_for_status: bool = True,
    ) -> None:
        super().__init__()
        self.timeout = _timeout_to_seconds(timeout)
        self.raise_for_status = raise_for_status
        self.headers.update(
            {
                "User-Agent": f"dlt/{__version__}",
            }
        )

    if TYPE_CHECKING:
        request = BaseSession.request

    def request(self, *args, **kwargs):  # type: ignore[no-untyped-def,no-redef]
        kwargs.setdefault("timeout", self.timeout)
        resp = super().request(*args, **kwargs)
        if self.raise_for_status:
            resp.raise_for_status()
        return resp

    def send(self, request, **kwargs):  # type: ignore[no-untyped-def]
        kwargs.setdefault("timeout", self.timeout)
        return super().send(request, **kwargs)
