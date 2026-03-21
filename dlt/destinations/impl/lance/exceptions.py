from functools import wraps
import re
from typing import (
    Any,
)

from dlt.common.destination.exceptions import (
    DestinationUndefinedEntity,
    DestinationTransientException,
)
from dlt.common.destination.client import JobClientBase
from dlt.common.typing import TFun


LANCE_NOT_FOUND = r"not\s+found"
LANCE_DOES_NOT_EXIST = r"does\s+not\s+exist"
LANCE_MANIFEST_MODE = r"manifest\s+mode\s+is\s+enabled"

LANCE_UNDEFINED_ENTITY_PATTERN = re.compile(
    rf"(?i){LANCE_NOT_FOUND}|{LANCE_DOES_NOT_EXIST}|{LANCE_MANIFEST_MODE}"
)


def is_lance_undefined_entity_exception(e: Exception) -> bool:
    """Returns True if exception indicates an undefined entity (e.g. missing namespace or table).

    Used to work around bug: https://github.com/lance-format/lance/issues/6240.
    """
    return isinstance(e, RuntimeError) and bool(LANCE_UNDEFINED_ENTITY_PATTERN.search(str(e)))


def lance_error(f: TFun) -> TFun:
    @wraps(f)
    def _wrap(self: JobClientBase, *args: Any, **kwargs: Any) -> Any:
        try:
            return f(self, *args, **kwargs)
        except Exception as e:
            if is_lance_undefined_entity_exception(e):
                raise DestinationUndefinedEntity(e) from e
            raise DestinationTransientException(e) from e

    return _wrap  # type: ignore[return-value]
