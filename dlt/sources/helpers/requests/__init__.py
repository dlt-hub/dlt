from tenacity import RetryError
from requests import (
    Request, Response,
    ConnectionError,
    ConnectTimeout,
    FileModeWarning,
    HTTPError,
    JSONDecodeError,
    ReadTimeout,
    RequestException,
    Timeout,
    TooManyRedirects,
    URLRequired,
)
from dlt.sources.helpers.requests.retry import Client
from dlt.sources.helpers.requests.session import Session

client = Client()

get, post, put, patch, delete, options, head, request = (
    client.get, client.post, client.put, client.patch, client.delete, client.options, client.head, client.request
)
