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
