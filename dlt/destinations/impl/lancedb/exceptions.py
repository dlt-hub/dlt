from functools import wraps
import re
from typing import (
    Any,
)

from lancedb.exceptions import MissingValueError, MissingColumnError

from dlt.common.destination.exceptions import (
    DestinationUndefinedEntity,
    DestinationTransientException,
)
from dlt.common.destination.client import JobClientBase
from dlt.common.typing import TFun

lancedb_not_found_pattern = re.compile(
    r"(?i)(not\s+found|unknown\s+table|missing\s+value|missing\s+column)"
)


def is_lancedb_not_found_error(error_message: str) -> bool:
    """Check if the error message indicates a LanceDB not found error."""
    return bool(lancedb_not_found_pattern.search(error_message))


def lancedb_error(f: TFun) -> TFun:
    @wraps(f)
    def _wrap(self: JobClientBase, *args: Any, **kwargs: Any) -> Any:
        try:
            return f(self, *args, **kwargs)
        except ValueError as e:
            if is_lancedb_not_found_error(str(e)):
                raise DestinationUndefinedEntity(e) from e
            raise e
        except (
            MissingValueError,
            MissingColumnError,
        ) as status_ex:
            raise DestinationUndefinedEntity(status_ex) from status_ex
        except Exception as e:
            raise DestinationTransientException(e) from e

    return _wrap  # type: ignore[return-value]
