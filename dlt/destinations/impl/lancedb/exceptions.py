from functools import wraps
from typing import (
    Any,
)

from lancedb.exceptions import MissingValueError, MissingColumnError  # type: ignore

from dlt.common.destination.exceptions import (
    DestinationUndefinedEntity,
    DestinationTerminalException,
)
from dlt.common.destination.reference import JobClientBase
from dlt.common.typing import TFun


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
