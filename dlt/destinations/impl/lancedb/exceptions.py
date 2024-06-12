from functools import wraps
from typing import (
    Any,
)

from lancedb.exceptions import MissingValueError, MissingColumnError  # type: ignore[import-untyped]
from pyarrow import ArrowInvalid

from dlt.common.destination.exceptions import (
    DestinationUndefinedEntity,
    DestinationTerminalException,
    DestinationException,
    DestinationTransientException,
)
from dlt.common.destination.reference import JobClientBase
from dlt.common.typing import TFun


class LanceDBBatchError(DestinationException):
    pass


def lancedb_error(f: TFun) -> TFun:
    @wraps(f)
    def _wrap(self: JobClientBase, *args: Any, **kwargs: Any) -> Any:
        try:
            return f(self, *args, **kwargs)
        except (
            FileNotFoundError,
            MissingValueError,
            MissingColumnError,
        ) as status_ex:
            raise DestinationUndefinedEntity(status_ex) from status_ex
        except Exception as e:
            raise DestinationTerminalException(e) from e

    return _wrap  # type: ignore[return-value]


def lancedb_batch_error(f: TFun) -> TFun:
    @wraps(f)
    def _wrap(*args: Any, **kwargs: Any) -> Any:
        try:
            return f(*args, **kwargs)
        except LanceDBBatchError as batch_ex:
            if isinstance(batch_ex.__cause__, FileNotFoundError):
                # Could be transient due to networking issues to lancedb cloud?
                raise DestinationTransientException(
                    "Couldn't open lancedb database. Batch WILL BE RETRIED"
                ) from batch_ex
            if isinstance(batch_ex.__cause__, ArrowInvalid):
                raise DestinationTerminalException(
                    "Python and Arrow datatype mismatch - batch failed AND WILL **NOT** BE RETRIED."
                ) from batch_ex
            else:
                raise DestinationTerminalException(
                    "Batch failed AND WILL **NOT** BE RETRIED"
                ) from batch_ex
        except Exception as e:
            raise DestinationTransientException("Batch failed AND WILL BE RETRIED") from e

    return _wrap  # type: ignore[return-value]
