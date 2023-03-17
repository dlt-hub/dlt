from requests import Session as BaseSession
from tenacity import Retrying, retry_if_exception_type
from typing import Optional, TYPE_CHECKING, Sequence, Union, Tuple, Type, TypeVar

from dlt.common.typing import TimedeltaSeconds
from dlt.common.time import to_seconds


TSession = TypeVar("TSession", bound=BaseSession)


DEFAULT_TIMEOUT = 30


class Session(BaseSession):
    """Requests session which by default adds a timeout to all requests and calls `raise_for_status()` on response

    ### Args
        timeout: Timeout for requests in seconds. May be passed as `timedelta` or `float/int` number of seconds.
        raise_for_status: Whether to raise exception on error status codes (using `response.raise_for_status()`)
    """
    def __init__(
        self,
        timeout: Optional[TimedeltaSeconds] = DEFAULT_TIMEOUT,
        raise_for_status: bool = True,
    ) -> None:
        super().__init__()
        self.timeout = to_seconds(timeout)
        self.raise_for_status = raise_for_status

    if TYPE_CHECKING:
        request = BaseSession.request

    def request(self, *args, **kwargs):  # type: ignore[no-untyped-def,no-redef]
        kwargs.setdefault('timeout', self.timeout)
        resp = super().request(*args, **kwargs)
        if self.raise_for_status:
            resp.raise_for_status()
        return resp
