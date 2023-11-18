from email.utils import parsedate_tz, mktime_tz
import re
import time
from typing import Optional, cast, Callable, Type, Union, Sequence, Tuple, List, TYPE_CHECKING, Any, Dict
from threading import local

from requests import Response, HTTPError, Session as BaseSession
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError
from requests.adapters import HTTPAdapter
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, RetryCallState, retry_any, wait_exponential
from tenacity.retry import retry_base

from dlt.sources.helpers.requests.session import Session, DEFAULT_TIMEOUT
from dlt.sources.helpers.requests.typing import TRequestTimeout
from dlt.common.typing import TimedeltaSeconds
from dlt.common.configuration.specs import RunConfiguration
from dlt.common.configuration import with_config


DEFAULT_RETRY_STATUS = (429, *range(500, 600))
DEFAULT_RETRY_EXCEPTIONS = (ConnectionError, Timeout, ChunkedEncodingError)

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
    max_delay: TimedeltaSeconds,
)-> Retrying:
    retry_conds = [retry_if_status(status_codes), retry_if_exception_type(tuple(exceptions))]
    if condition is not None:
        if callable(condition):
            retry_condition = [condition]
        retry_conds.extend([retry_if_predicate(c) for c in retry_condition])

    wait_cls = wait_exponential_retry_after if respect_retry_after_header else wait_exponential
    return Retrying(
        wait=wait_cls(multiplier=backoff_factor, max=max_delay),
        retry=(retry_any(*retry_conds)),
        stop=stop_after_attempt(max_attempts),
        reraise=True,
        retry_error_callback=lambda state: state.outcome.result(),
    )


class Client:
    """Wrapper for `requests` to create a `Session` with configurable retry functionality.

    #### Note:
    Create a  `requests.Session` which automatically retries requests in case of error.
    By default retries are triggered for `5xx` and `429` status codes and when the server is unreachable or drops connection.

    #### Custom retry condition
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

    #### Args:
        request_timeout: Timeout for requests in seconds. May be passed as `timedelta` or `float/int` number of seconds.
        max_connections: Max connections per host in the HTTPAdapter pool
        raise_for_status: Whether to raise exception on error status codes (using `response.raise_for_status()`)
        session: Optional `requests.Session` instance to add the retry handler to. A new session is created by default.
        status_codes: Retry when response has any of these status codes. Default `429` and all `5xx` codes. Pass an empty list to disable retry based on status.
        exceptions: Retry on exception of given type(s). Default `(requests.Timeout, requests.ConnectionError)`. Pass an empty list to disable retry on exceptions.
        request_max_attempts: Max number of retry attempts before giving up
        retry_condition: A predicate or a list of predicates to decide whether to retry. If any predicate returns `True` the request is retried
        request_backoff_factor: Multiplier used for exponential delay between retries
        request_max_retry_delay: Maximum delay when using exponential backoff
        respect_retry_after_header: Whether to use the `Retry-After` response header (when available) to determine the retry delay
        session_attrs: Extra attributes that will be set on the session instance, e.g. `{headers: {'Authorization': 'api-key'}}` (see `requests.sessions.Session` for possible attributes)
    """
    _session_attrs: Dict[str, Any]

    @with_config(spec=RunConfiguration)
    def __init__(
        self,
        request_timeout: Optional[Union[TimedeltaSeconds, Tuple[TimedeltaSeconds, TimedeltaSeconds]]] = DEFAULT_TIMEOUT,
        max_connections: int = 50,
        raise_for_status: bool = True,
        status_codes: Sequence[int] = DEFAULT_RETRY_STATUS,
        exceptions: Sequence[Type[Exception]] = DEFAULT_RETRY_EXCEPTIONS,
        request_max_attempts: int = RunConfiguration.request_max_attempts,
        retry_condition: Union[RetryPredicate, Sequence[RetryPredicate], None] = None,
        request_backoff_factor: float = RunConfiguration.request_backoff_factor,
        request_max_retry_delay: TimedeltaSeconds = RunConfiguration.request_max_retry_delay,
        respect_retry_after_header: bool = True,
        session_attrs: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._adapter = HTTPAdapter(pool_maxsize=max_connections)
        self._local = local()
        self._session_kwargs = dict(timeout=request_timeout, raise_for_status=raise_for_status)
        self._retry_kwargs: Dict[str, Any] = dict(
            status_codes=status_codes,
            exceptions=exceptions,
            max_attempts=request_max_attempts,
            condition=retry_condition,
            backoff_factor=request_backoff_factor,
            respect_retry_after_header=respect_retry_after_header,
            max_delay=request_max_retry_delay
        )
        self._session_attrs = session_attrs or {}

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

        self._config_version: int = 0  # Incrementing marker to ensure per-thread sessions are recreated on config changes

    def update_from_config(self, config: RunConfiguration) -> None:
        """Update session/retry settings from RunConfiguration"""
        self._session_kwargs['timeout'] = config.request_timeout
        self._retry_kwargs['backoff_factor'] = config.request_backoff_factor
        self._retry_kwargs['max_delay'] = config.request_max_retry_delay
        self._retry_kwargs['max_attempts'] = config.request_max_attempts
        self._config_version += 1

    def _make_session(self) -> Session:
        session = Session(**self._session_kwargs)  # type: ignore[arg-type]
        for key, value in self._session_attrs.items():
            setattr(session, key, value)
        session.mount('http://', self._adapter)
        session.mount('https://', self._adapter)
        retry = _make_retry(**self._retry_kwargs)
        session.request = retry.wraps(session.request)  # type: ignore[method-assign]
        return session

    @property
    def session(self) -> Session:
        session: Optional[Session] = getattr(self._local, 'session', None)
        version = self._config_version
        if session is not None:
            version = self._local.config_version
        if session is None or version != self._config_version:
            # Create a new session if config has changed
            session = self._local.session = self._make_session()
            self._local.config_version = self._config_version
        return session
