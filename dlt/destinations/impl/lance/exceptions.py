from functools import wraps
import re
from typing import Any, List

from dlt.common.destination.exceptions import (
    DestinationUndefinedEntity,
    DestinationTerminalException,
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


class LanceEmbeddingsConfigurationMissing(DestinationTerminalException):
    def __init__(self, table_name: str, columns: List[str]) -> None:
        columns_str = ", ".join(f"'{col}'" for col in columns)
        super().__init__(
            f"Table '{table_name}' has columns marked for embedding ({columns_str}) but is"
            " missing embeddings configuration. Either configure `embeddings` on the lance"
            " destination or remove the `embed` argument from `lancedb_adapter()`."
        )


def is_lance_undefined_entity_exception(e: Exception) -> bool:
    """Returns True if exception indicates an undefined entity (e.g. missing namespace, table, or branch).

    Used to work around:
    - bug: https://github.com/lance-format/lance/issues/6240
    - fact that `LanceDataset.checkout_version()` raises `ValueError` with "not found" message if
    version does not exist
    """
    return isinstance(e, (RuntimeError, ValueError)) and bool(
        LANCE_UNDEFINED_ENTITY_PATTERN.search(str(e))
    )


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
