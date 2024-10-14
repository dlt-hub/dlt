from typing import Any, Dict, Iterator, List, Type
from unittest import mock
import os
import random

import pytest
import requests
import requests_mock
from tenacity import wait_exponential

import dlt
from dlt.common.configuration.specs import RuntimeConfiguration
from dlt.sources.helpers.requests import Client, client as default_client
from dlt.sources.helpers.requests.retry import (
    DEFAULT_RETRY_EXCEPTIONS,
    DEFAULT_RETRY_STATUS,
    retry_if_status,
    retry_any,
    Retrying,
    wait_exponential_retry_after,
)

from tests.utils import preserve_environ


@pytest.fixture(scope="function", autouse=True)
def mock_sleep() -> Iterator[mock.MagicMock]:
    with mock.patch("time.sleep") as m:
        yield m
    # restore standard settings on default client
    default_client.configure(RuntimeConfiguration())


def test_default_session_retry_settings() -> None:
    retry: Retrying = Client().session.send.retry  # type: ignore
    assert retry.stop.max_attempt_number == 5  # type: ignore
    assert isinstance(retry.retry, retry_any)
    retries = retry.retry.retries
    assert retries[0].status_codes == set(DEFAULT_RETRY_STATUS)  # type: ignore
    assert retries[1].exception_types == DEFAULT_RETRY_EXCEPTIONS  # type: ignore
    assert isinstance(retry.wait, wait_exponential_retry_after)
    assert retry.wait.multiplier == 1


@pytest.mark.parametrize("respect_retry_after_header", (True, False))
def test_custom_session_retry_settings(respect_retry_after_header: bool) -> None:
    def custom_retry_cond(response, exception):
        return True

    session = Client(
        request_max_attempts=14,
        retry_condition=custom_retry_cond,
        request_backoff_factor=2,
        respect_retry_after_header=False,
    ).session

    retry: Retrying = session.send.retry  # type: ignore
    assert retry.stop.max_attempt_number == 14  # type: ignore
    assert isinstance(retry.retry, retry_any)
    retries = retry.retry.retries
    assert retries[2].predicate == custom_retry_cond  # type: ignore
    assert isinstance(retry.wait, wait_exponential)
    assert retry.wait.multiplier == 2


def test_retry_on_status_all_fails(mock_sleep: mock.MagicMock) -> None:
    session = Client().session
    url = "https://example.com/data"
    m = requests_mock.Adapter()
    session.mount("https://", m)
    m.register_uri("GET", url, status_code=503)

    with pytest.raises(requests.HTTPError):
        session.get(url)

    assert m.call_count == RuntimeConfiguration.request_max_attempts


def test_retry_on_status_success_after_2(mock_sleep: mock.MagicMock) -> None:
    """Test successful request after 2 retries"""
    session = Client().session
    url = "https://example.com/data"
    m = requests_mock.Adapter()
    session.mount("https://", m)

    responses = [
        dict(text="error", status_code=503),
        dict(text="error", status_code=503),
        dict(text="error", status_code=200),
    ]

    m.register_uri("GET", url, responses)
    resp = session.get(url)

    assert resp.status_code == 200
    assert m.call_count == 3


def test_retry_on_status_without_raise_for_status(mock_sleep: mock.MagicMock) -> None:
    url = "https://example.com/data"
    session = Client(raise_for_status=False).session
    m = requests_mock.Adapter()
    session.mount("https://", m)

    m.register_uri("GET", url, status_code=503)
    response = session.get(url)
    assert response.status_code == 503

    assert m.call_count == RuntimeConfiguration.request_max_attempts


def test_hooks_with_raise_for_statue() -> None:
    url = "https://example.com/data"
    session = Client(raise_for_status=True).session
    m = requests_mock.Adapter()
    session.mount("https://", m)

    def _no_content(resp: requests.Response, *args, **kwargs) -> requests.Response:
        resp.status_code = 204
        resp._content = b"[]"
        return resp

    m.register_uri("GET", url, status_code=503)
    response = session.get(url, hooks={"response": _no_content})
    # we simulate empty response
    assert response.status_code == 204
    assert response.json() == []

    assert m.call_count == 1


@pytest.mark.parametrize(
    "exception_class",
    [requests.ConnectionError, requests.ConnectTimeout, requests.exceptions.ChunkedEncodingError],
)
def test_retry_on_exception_all_fails(
    exception_class: Type[Exception], mock_sleep: mock.MagicMock
) -> None:
    session = Client().session
    m = requests_mock.Adapter()
    session.mount("https://", m)
    url = "https://example.com/data"

    m.register_uri("GET", url, exc=exception_class)
    with pytest.raises(exception_class):
        session.get(url)

    assert m.call_count == RuntimeConfiguration.request_max_attempts


def test_retry_on_custom_condition(mock_sleep: mock.MagicMock) -> None:
    def retry_on(response: requests.Response, exception: BaseException) -> bool:
        return response.text == "error"

    session = Client(retry_condition=retry_on).session
    m = requests_mock.Adapter()
    session.mount("https://", m)
    url = "https://example.com/data"

    m.register_uri("GET", url, text="error")
    response = session.get(url)
    assert response.content == b"error"

    assert m.call_count == RuntimeConfiguration.request_max_attempts


def test_retry_on_custom_condition_success_after_2(mock_sleep: mock.MagicMock) -> None:
    def retry_on(response: requests.Response, exception: BaseException) -> bool:
        return response.text == "error"

    session = Client(retry_condition=retry_on).session
    m = requests_mock.Adapter()
    session.mount("https://", m)
    url = "https://example.com/data"

    m.register_uri("GET", url, [dict(text="error"), dict(text="error"), dict(text="success")])
    resp = session.get(url)

    assert resp.text == "success"
    assert m.call_count == 3


def test_wait_retry_after_int(mock_sleep: mock.MagicMock) -> None:
    session = Client(request_backoff_factor=0).session
    url = "https://example.com/data"
    m = requests_mock.Adapter()
    session.mount("https://", m)
    m.register_uri("GET", url, text="error")
    responses: List[Dict[str, Any]] = [
        dict(text="error", headers={"retry-after": "4"}, status_code=429),
        dict(text="success"),
    ]

    m.register_uri("GET", url, responses)
    session.get(url)

    mock_sleep.assert_called_once()
    assert 4 <= mock_sleep.call_args[0][0] <= 5  # Adds jitter up to 1s


def test_init_default_client() -> None:
    """Test that the default client config is updated from runtime configuration.
    Run twice. 1. Clean start with no existing session attached.
    2. With session in thread local (session is updated)
    """
    cfg = {
        "RUNTIME__REQUEST_TIMEOUT": random.randrange(1, 100),
        "RUNTIME__REQUEST_MAX_ATTEMPTS": random.randrange(1, 100),
        "RUNTIME__REQUEST_BACKOFF_FACTOR": random.randrange(1, 100),
        "RUNTIME__REQUEST_MAX_RETRY_DELAY": random.randrange(1, 100),
    }

    os.environ.update({key: str(value) for key, value in cfg.items()})

    dlt.pipeline(pipeline_name="dummy_pipeline")

    session = default_client.session
    assert session.timeout == cfg["RUNTIME__REQUEST_TIMEOUT"]
    retry = session.send.retry  # type: ignore[attr-defined]
    assert retry.wait.multiplier == cfg["RUNTIME__REQUEST_BACKOFF_FACTOR"]
    assert retry.stop.max_attempt_number == cfg["RUNTIME__REQUEST_MAX_ATTEMPTS"]
    assert retry.wait.max == cfg["RUNTIME__REQUEST_MAX_RETRY_DELAY"]


@pytest.mark.parametrize("existing_session", (False, True))
def test_client_instance_with_config(existing_session: bool) -> None:
    cfg = {
        "RUNTIME__REQUEST_TIMEOUT": random.randrange(1, 100),
        "RUNTIME__REQUEST_MAX_ATTEMPTS": random.randrange(1, 100),
        "RUNTIME__REQUEST_BACKOFF_FACTOR": random.randrange(1, 100),
        "RUNTIME__REQUEST_MAX_RETRY_DELAY": random.randrange(1, 100),
    }
    os.environ.update({key: str(value) for key, value in cfg.items()})

    if existing_session:
        client = default_client
        client.configure()
    else:
        client = Client()

    session = client.session
    assert session.timeout == cfg["RUNTIME__REQUEST_TIMEOUT"]
    retry = session.send.retry  # type: ignore[attr-defined]
    assert retry.wait.multiplier == cfg["RUNTIME__REQUEST_BACKOFF_FACTOR"]
    assert retry.stop.max_attempt_number == cfg["RUNTIME__REQUEST_MAX_ATTEMPTS"]
    assert retry.wait.max == cfg["RUNTIME__REQUEST_MAX_RETRY_DELAY"]
