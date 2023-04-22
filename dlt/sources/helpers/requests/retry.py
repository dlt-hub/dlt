from email.utils import parsedate_tz, mktime_tz
from functools import partial, wraps
import inspect
import re
import time
from typing import Optional, cast, Callable, Type, Union, Sequence, Tuple, List, TYPE_CHECKING, Any
from threading import local

from requests import Response, HTTPError, ConnectionError, Timeout, Session as BaseSession
from requests.adapters import HTTPAdapter
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, RetryCallState, retry_any, wait_exponential
from tenacity.retry import retry_base

from dlt.sources.helpers.requests.session import Session, DEFAULT_TIMEOUT
from dlt.common.typing import TimedeltaSeconds


DEFAULT_RETRY_STATUS = (429, *range(500, 600))
DEFAULT_RETRY_EXCEPTIONS = (ConnectionError, Timeout)
DEFAULT_RETRY_ATTEMPTS = 5
DEFAULT_BACKOFF_FACTOR = 1

RetryPredicate = Callable[[Optional[Response], Optional[BaseException]], bool]


def _get_retry_response(retry_state: RetryCallState) -> Optional[Response]:
    ex = retry_state.outcome.exception()
    if ex:
        if isinstance(ex, HTTPError):
            return cast(Response, ex.response)
        return None
    result = retry_state.outcome.result()
    return result if isinstance(result, Response) else None


class retry_if_status(retry_base):
    """Retry for given response status codes"""

    def __init__(self, status_codes: Sequence[int]) -> None:
        self.status_codes = set(status_codes)

    def __call__(self, retry_state: RetryCallState) -> bool:
        response = _get_retry_response(retry_state)
        if response is None:
            return False
        result = response.status_code in self.status_codes
        return result


class retry_if_predicate(retry_base):
    def __init__(self, predicate: RetryPredicate) -> None:
        self.predicate = predicate

    def __call__(self, retry_state: RetryCallState) -> bool:
        response = _get_retry_response(retry_state)
        exception = retry_state.outcome.exception()
        return self.predicate(response, exception)


class wait_exponential_retry_after(wait_exponential):
    def _parse_retry_after(self, retry_after: str) -> Optional[float]:
        # Borrowed from urllib3
        seconds: float
        # Whitespace: https://tools.ietf.org/html/rfc7230#section-3.2.4
        if re.match(r"^\s*[0-9]+\s*$", retry_after):
            seconds = int(retry_after)
        else:
            retry_date_tuple = parsedate_tz(retry_after)
            if retry_date_tuple is None:
                return None
            retry_date = mktime_tz(retry_date_tuple)
            seconds = retry_date - time.time()
        return max(self.min, min(self.max, seconds))

    def _get_retry_after(self, retry_state: RetryCallState) -> Optional[float]:
        response = _get_retry_response(retry_state)
        if response is None:
            return None
        header = response.headers.get("Retry-After")
        if not header:
            return None
        return self._parse_retry_after(header)

    def __call__(self, retry_state: RetryCallState) -> float:
        retry_after = self._get_retry_after(retry_state)
        if retry_after is not None:
            return retry_after
        return super().__call__(retry_state)


def _make_retry(
    status_codes: Sequence[int],
    exceptions: Sequence[Type[Exception]],
    max_attempts: int,
    condition: Union[RetryPredicate, Sequence[RetryPredicate], None],
    backoff_factor: float,
    respect_retry_after_header: bool,
)-> Retrying:
    retry_conds = [retry_if_status(status_codes), retry_if_exception_type(tuple(exceptions))]
    if condition is not None:
        if callable(condition):
            retry_condition = [condition]
        retry_conds.extend([retry_if_predicate(c) for c in retry_condition])

    wait_cls = wait_exponential_retry_after if respect_retry_after_header else wait_exponential

    return Retrying(
        wait=wait_cls(multiplier=backoff_factor),
        retry=(retry_any(*retry_conds)),
        stop=stop_after_attempt(max_attempts),
        reraise=True
    )


class Client:
    """Wrapper for `requests` to create a `Session` with configurable retry functionality.

    ### Summary
    Create a  `requests.Session` which automatically retries requests in case of error.
    By default retries are triggered for `5xx` and `429` status codes and when the server is unreachable or drops connection.

    ### Custom retry condition
    You can provide one or more custom predicates for specific retry condition. The predicate is called after every request with the resulting response and/or exception.
    For example, this will trigger a retry when the response text is `error`:

    >>> from typing import Optional
    >>> from requests import Response
    >>>
    >>> def should_retry(response: Optional[Response], exception: Optional[BaseException]) -> bool:
    >>>     if response is None:
    >>>         return False
    >>>     return response.text == 'error'

    The retry is triggered when either any of the predicates or the default conditions based on status code/exception are `True`.

    ### Args:
        timeout: Timeout for requests in seconds. May be passed as `timedelta` or `float/int` number of seconds.
        raise_for_status: Whether to raise exception on error status codes (using `response.raise_for_status()`)
        session: Optional `requests.Session` instance to add the retry handler to. A new session is created by default.
        status_codes: Retry when response has any of these status codes. Default `429` and all `5xx` codes. Pass an empty list to disable retry based on status.
        exceptions: Retry on exception of given type(s). Default `(requests.Timeout, requests.ConnectionError)`. Pass an empty list to disable retry on exceptions.
        max_attempts: Max number of retry attempts before giving up
        condition: A predicate or a list of predicates to decide whether to retry. If any predicate returns `True` the request is retried
        backoff_factor: Multiplier used for exponential delay between retries
        respect_retry_after_header: Whether to use the `Retry-After` response header (when available) to determine the retry delay
        **session_attrs: Extra attributes that will be set on the session instance, e.g. `headers: {'Authorization': 'api-key'}` (see `requests.sessions.Session` for possible attributes)
    """
    def __init__(
        self,
        timeout: Optional[TimedeltaSeconds] = DEFAULT_TIMEOUT,
        raise_for_status: bool = True,
        status_codes: Sequence[int] = DEFAULT_RETRY_STATUS,
        exceptions: Sequence[Type[Exception]] = DEFAULT_RETRY_EXCEPTIONS,
        max_attempts: int = DEFAULT_RETRY_ATTEMPTS,
        condition: Union[RetryPredicate, Sequence[RetryPredicate], None] = None,
        backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
        respect_retry_after_header: bool = True,
        **session_attrs: Any
    ) -> None:
        self._adapter = HTTPAdapter()
        self._local = local()
        self._session_kwargs = dict(timeout=timeout, raise_for_status=raise_for_status)
        self._retry_kwargs = dict(
            status_codes=status_codes,
            exceptions=exceptions,
            max_attempts=max_attempts,
            condition=condition,
            backoff_factor=backoff_factor,
            respect_retry_after_header=respect_retry_after_header
        )
        self._session_attrs = session_attrs

        if TYPE_CHECKING:
            self.get = self.session.get
            self.post = self.session.post
            self.put = self.session.put
            self.patch = self.session.patch
            self.delete = self.session.delete
            self.head = self.session.head
            self.options = self.session.options
            self.request = self.session.request

        self.get = lambda *a, **kw: self.session.get(*a, **kw)
        self.post = lambda *a, **kw: self.session.post(*a, **kw)
        self.put = lambda *a, **kw: self.session.put(*a, **kw)
        self.patch = lambda *a, **kw: self.session.patch(*a, **kw)
        self.delete = lambda *a, **kw: self.session.delete(*a, **kw)
        self.head = lambda *a, **kw: self.session.head(*a, **kw)
        self.options = lambda *a, **kw: self.session.options(*a, **kw)
        self.request = lambda *a, **kw: self.session.request(*a, **kw)

    def _make_session(self) -> Session:
        session = Session(**self._session_kwargs)  # type: ignore[arg-type]
        for key, value in self._session_attrs.items():
            setattr(session, key, value)
        session.mount('http://', self._adapter)
        session.mount('https://', self._adapter)
        retry = _make_retry(**self._retry_kwargs)  # type: ignore[arg-type]
        session.request = retry.wraps(session.request)  # type: ignore[method-assign]
        return session

    @property
    def session(self) -> Session:
        session: Optional[Session] = getattr(self._local, 'session', None)
        if session is None:
            session = self._local.session = self._make_session()
        return session
