from __future__ import annotations

from dlt.common.exceptions import DltException


class HotdataDestinationError(DltException):
    pass


class HotdataTransientError(HotdataDestinationError):
    pass


class HotdataTerminalError(HotdataDestinationError):
    pass


def classify_sdk_error(error: Exception) -> HotdataDestinationError:
    if isinstance(error, TimeoutError):
        return HotdataTransientError(str(error))
    if isinstance(error, ConnectionError):
        return HotdataTransientError(str(error))
    try:
        from hotdata.rest import ApiException
        from hotdata.uploads import UploadError
    except ModuleNotFoundError:
        return HotdataTerminalError(str(error))

    if isinstance(error, ApiException):
        status_code = int(error.status or 0)
        message = f"{status_code}: {error.reason or 'unknown error'}"
        if status_code in (408, 409, 425, 429):
            return HotdataTransientError(message)
        if 500 <= status_code <= 599:
            return HotdataTransientError(message)
        return HotdataTerminalError(message)

    if isinstance(error, UploadError):
        status_code = int(getattr(error, "status", 0) or 0)
        if status_code in (408, 409, 425, 429) or 500 <= status_code <= 599:
            return HotdataTransientError(str(error))
        return HotdataTerminalError(str(error))
    return HotdataTerminalError(str(error))
