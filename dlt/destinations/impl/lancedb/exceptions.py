from functools import wraps
from typing import (
    Any,
)

from lancedb.exceptions import MissingValueError, MissingColumnError  # type: ignore[import-untyped]

from dlt.common.destination.exceptions import (
    DestinationUndefinedEntity,
    DestinationTerminalException, DestinationException, DestinationTransientException,
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
        except (
            LanceDBBatchError
        ) as batch_ex:
            errors = batch_ex.args[0]
            message = errors["error"][0]["message"]
            # TODO: Categorise batch error messages more precisely.
            raise DestinationTransientException(f"Batch failed \n{errors}:{message}\n AND WILL BE RETRIED") from batch_ex
        except Exception as e:
            raise DestinationTransientException("Batch failed AND WILL BE RETRIED") from e


    return _wrap  # type: ignore[return-value]
