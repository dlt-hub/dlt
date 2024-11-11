from typing import Optional, Dict, Any
from requests.exceptions import HTTPError


def find_http_error(exception: Exception) -> Optional[Dict[str, Any]]:
    """Recursively searches through exception chain to find and extract HTTPError.

    Args:
        exception: The exception to analyze

    Returns:
        The HTTPError instance if found, None otherwise.

    Example:
        try:
            # Some code that may raise exceptions caused by requests.HTTPError
            pipeline.run(get_issues)
        except Exception as e:
            if http_error := find_http_error(e):
                print(f"HTTP {http_error.response.status_code}: {http_error.response.content}")
            else:
                raise
    """

    def _get_next_cause(e: Exception) -> Optional[Exception]:
        # Handle both __cause__ (from raise ... from) and __context__ (from bare raise)
        return e.__cause__ if e.__cause__ is not None else e.__context__

    current_exc = exception
    while current_exc is not None:
        if isinstance(current_exc, HTTPError):
            return current_exc
        current_exc = _get_next_cause(current_exc)

    return None


class HTTPErrorCatcher:
    def __init__(self):
        self.http_error: Optional[HTTPError] = None

    def __enter__(self) -> "HTTPErrorCatcher":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_val is not None:
            self.http_error = find_http_error(exc_val)
            # Suppress exception if we found an HTTPError
            return bool(self.http_error)
        return False

    def __bool__(self):
        return self.http_error is not None

    def __getattr__(self, attr):
        if self.http_error:
            return getattr(self.http_error, attr)
        raise AttributeError(f"{attr} not found in {self.__class__.__name__}")


def catch_http_error() -> HTTPErrorCatcher:
    """Catches and extracts HTTP errors.

    Example:
        with catch_http_error() as http_error:
            load_info = pipeline.run(get_issues)

        if http_error:
            print(f"HTTP {http_error.response.status_code}: {http_error.response.content}")
    """
    return HTTPErrorCatcher()
