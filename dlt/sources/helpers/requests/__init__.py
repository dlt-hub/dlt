from tenacity import RetryError
from requests import (
    Request,
    Response,
    ConnectionError,
    ConnectTimeout,
    FileModeWarning,
    HTTPError,
    ReadTimeout,
    RequestException,
    Timeout,
    TooManyRedirects,
    URLRequired,
)
from requests.exceptions import ChunkedEncodingError
from dlt.sources.helpers.requests.retry import Client
from dlt.sources.helpers.requests.session import Session
from dlt.common.configuration.specs import RunConfiguration

client = Client()

get, post, put, patch, delete, options, head, request = (
    client.get,
    client.post,
    client.put,
    client.patch,
    client.delete,
    client.options,
    client.head,
    client.request,
)


def init(config: RunConfiguration) -> None:
    """Initialize the default requests client from config"""
    client.update_from_config(config)


__all__ = [
    "client",
    "get",
    "post",
    "put",
    "patch",
    "delete",
    "options",
    "head",
    "request",
    "init",
    "Session",
    "Request",
    "Response",
    "ConnectionError",
    "ConnectTimeout",
    "FileModeWarning",
    "HTTPError",
    "ReadTimeout",
    "RequestException",
    "Timeout",
    "TooManyRedirects",
    "URLRequired",
    "ChunkedEncodingError",
    "RetryErrorClient",
    "RetryError",
]
