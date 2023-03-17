from contextlib import contextmanager
from typing import Iterator, Any, cast
from unittest import mock
from email.utils import format_datetime

import pytest
import requests
import requests_mock
from tenacity import wait_exponential, RetryCallState, RetryError

from dlt.sources.helpers.requests import Session, Client
from dlt.sources.helpers.requests.retry import (
    DEFAULT_RETRY_EXCEPTIONS, DEFAULT_RETRY_STATUS, DEFAULT_RETRY_ATTEMPTS, retry_if_status, retry_any, Retrying, wait_exponential_retry_after
)


@pytest.fixture(scope='function', autouse=True)
def mock_sleep() -> Iterator[mock.MagicMock]:
    with mock.patch('time.sleep') as m:
        yield m


def test_default_session_retry_settings() -> None:
    retry: Retrying = Client().session.request.retry # type: ignore
    assert retry.stop.max_attempt_number == 5  # type: ignore
    assert isinstance(retry.retry, retry_any)
    retries = retry.retry.retries
    assert retries[0].status_codes == set(DEFAULT_RETRY_STATUS)  # type: ignore
    assert retries[1].exception_types == DEFAULT_RETRY_EXCEPTIONS  # type: ignore
    assert isinstance(retry.wait, wait_exponential_retry_after)
    assert retry.wait.multiplier == 1


@pytest.mark.parametrize('respect_retry_after_header', (True, False))
def test_custom_session_retry_settings(respect_retry_after_header: bool) -> None:
    def custom_retry_cond(response, exception):  # type: ignore
        return True

    session = Client(
        max_attempts=14,
        condition=custom_retry_cond,
        backoff_factor=2,
        respect_retry_after_header=False,
    ).session

    retry: Retrying = session.request.retry  # type: ignore
    assert retry.stop.max_attempt_number == 14  # type: ignore
    assert isinstance(retry.retry, retry_any)
    retries = retry.retry.retries
    assert retries[2].predicate == custom_retry_cond # type: ignore
    assert isinstance(retry.wait, wait_exponential)
    assert retry.wait.multiplier == 2


def test_retry_on_status_all_fails(mock_sleep: mock.MagicMock) -> None:
    session = Client().session
    url = 'https://example.com/data'

    with requests_mock.mock(session=session) as m:
        m.get(url, status_code=503)
        with pytest.raises(requests.HTTPError):
            session.get(url)

    assert m.call_count == DEFAULT_RETRY_ATTEMPTS

def test_retry_on_status_success_after_2(mock_sleep: mock.MagicMock) -> None:
    """Test successful request after 2 retries
    """
    session = Client().session
    url = 'https://example.com/data'

    responses = [
        dict(text='error', status_code=503),
        dict(text='error', status_code=503),
        dict(text='error', status_code=200)
    ]

    with requests_mock.mock(session=session) as m:
        m.get(url, responses)
        resp = session.get(url)

    assert resp.status_code == 200
    assert m.call_count == 3

def test_retry_on_status_without_raise_for_status(mock_sleep: mock.MagicMock) -> None:
    url = 'https://example.com/data'
    session = Client(raise_for_status=False).session

    with requests_mock.mock(session=session) as m:
        m.get(url, status_code=503)
        with pytest.raises(RetryError):
            session.get(url)

    assert m.call_count == DEFAULT_RETRY_ATTEMPTS

def test_retry_on_exception_all_fails(mock_sleep: mock.MagicMock) -> None:
    session = Client().session
    url = 'https://example.com/data'

    with requests_mock.mock(session=session) as m:
        m.get(url, exc=requests.ConnectionError)
        with pytest.raises(requests.ConnectionError):
            session.get(url)

    assert m.call_count == DEFAULT_RETRY_ATTEMPTS

def test_retry_on_custom_condition(mock_sleep: mock.MagicMock) -> None:
    def retry_on(response: requests.Response, exception: BaseException) -> bool:
        return response.text == 'error'

    session = Client(condition=retry_on).session
    url = 'https://example.com/data'

    with requests_mock.mock(session=session) as m:
        m.get(url, text='error')
        with pytest.raises(RetryError):
            session.get(url)

    assert m.call_count == DEFAULT_RETRY_ATTEMPTS

def test_retry_on_custom_condition_success_after_2(mock_sleep: mock.MagicMock) -> None:
    def retry_on(response: requests.Response, exception: BaseException) -> bool:
        return response.text == 'error'

    session = Client(condition=retry_on).session
    url = 'https://example.com/data'
    responses = [dict(text='error'), dict(text='error'), dict(text='success')]

    with requests_mock.mock(session=session) as m:
        m.get(url, responses)
        resp = session.get(url)

    assert resp.text == 'success'
    assert m.call_count == 3

def test_wait_retry_after_int(mock_sleep: mock.MagicMock) -> None:
    session = Client(backoff_factor=0).session
    url = 'https://example.com/data'
    responses = [
        dict(text='error', headers={'retry-after': '4'}, status_code=429),
        dict(text='success')
    ]

    with requests_mock.mock(session=session) as m:
        m.get(url, responses)
        session.get(url)

    mock_sleep.assert_called_once()
    assert 4 <= mock_sleep.call_args[0][0] <= 5  # Adds jitter up to 1s
