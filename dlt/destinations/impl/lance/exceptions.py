from functools import wraps
import re
from typing import Any, List, Optional

from dlt.common.destination.exceptions import (
    DestinationException,
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
LANCE_MANIFEST_MODE_PATTERN = re.compile(rf"(?i){LANCE_MANIFEST_MODE}")


class LanceEmbeddingsConfigurationMissing(DestinationTerminalException):
    def __init__(self, table_name: str, columns: List[str]) -> None:
        columns_str = ", ".join(f"'{col}'" for col in columns)
        super().__init__(
            f"Table `{table_name}` has columns marked for embedding ({columns_str}) but is"
            " missing embeddings configuration. Either configure `embeddings` on the lance"
            " destination or remove the `embed` argument from `lance_adapter()`."
        )


class LanceManifestMisconfiguration(DestinationTerminalException):
    def __init__(self, original: Exception) -> None:
        super().__init__(
            "Manifest namespace failed to initialize and lance fell back to directory listing"
            " while manifest mode is enabled in the configuration. Check storage credentials"
            f" and connectivity. Original error: {original}"
        )


def is_lance_manifest_misconfiguration(e: Exception, manifest_enabled: Optional[bool]) -> bool:
    """Tells if lance reports manifest mode as disabled while the configuration enables it,
    which happens when the manifest namespace fails to initialize (eg. bad credentials)."""
    return manifest_enabled is True and bool(LANCE_MANIFEST_MODE_PATTERN.search(str(e)))


def is_lance_undefined_entity_exception(
    e: Exception, manifest_enabled: Optional[bool] = None
) -> bool:
    """Returns True if exception indicates an undefined entity (e.g. missing namespace, table, or branch).

    The message check covers untyped lance core errors:
    - bug: https://github.com/lance-format/lance/issues/6240
    - `checkout_version()` raises `ValueError` for a missing branch and `OSError` for a
      missing version, both with "not found" messages
    """
    if is_lance_manifest_misconfiguration(e, manifest_enabled):
        return False

    from lance_namespace.errors import LanceNamespaceError

    if isinstance(e, LanceNamespaceError):
        return type(e).__name__.endswith("NotFoundError")
    return isinstance(e, (RuntimeError, ValueError, OSError)) and bool(
        LANCE_UNDEFINED_ENTITY_PATTERN.search(str(e))
    )


def raise_destination_error(f: TFun) -> TFun:
    @wraps(f)
    def _wrap(self: JobClientBase, *args: Any, **kwargs: Any) -> Any:
        try:
            return f(self, *args, **kwargs)
        except DestinationException:
            # already converted (eg. raised by a nested decorated call)
            raise
        except Exception as e:
            manifest_enabled = getattr(self.config, "manifest_enabled", None)
            if is_lance_manifest_misconfiguration(e, manifest_enabled):
                raise LanceManifestMisconfiguration(e) from e
            if is_lance_undefined_entity_exception(e, manifest_enabled):
                raise DestinationUndefinedEntity(e) from e
            raise DestinationTransientException(e) from e

    return _wrap  # type: ignore[return-value]
