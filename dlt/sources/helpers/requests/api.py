import threading
from typing import Optional, TYPE_CHECKING

import requests

from dlt.sources.helpers.requests.session import Session
from dlt.sources.helpers.requests.retry import requests_with_retry


_local = threading.local()


def _get_session() -> Session:
    # Manage a thread local session with default settings
    session: Optional[Session] = getattr(_local, 'session', None)
    if not session:
        session = _local.session = requests_with_retry()
    return session


if TYPE_CHECKING:
    request = requests.request
    get = requests.get
    post = requests.post
    options = requests.options
    head = requests.head
    put = requests.put
    patch = requests.patch
    delete = requests.delete


def request(*args, **kwargs):  # type: ignore
    return _get_session().request(*args, **kwargs)


def get(*args, **kwargs): # type: ignore
    return _get_session().get(*args, **kwargs)


def post(*args, **kwargs):  # type: ignore
    return _get_session().post(*args, **kwargs)


def options(*args, **kwargs):  # type: ignore
    return _get_session().options(*args, **kwargs)


def head(*args, **kwargs):  # type: ignore
    return _get_session().head(*args, **kwargs)


def put(*args, **kwargs):  # type: ignore
    return _get_session().put(*args, **kwargs)


def patch(*args, **kwargs):  # type: ignore
    return _get_session().patch(*args, **kwargs)


def delete(*args, **kwargs):  # type: ignore
    return _get_session().delete(*args, **kwargs)
