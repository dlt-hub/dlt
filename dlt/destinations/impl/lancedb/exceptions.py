from functools import wraps
import re
from typing import Any

from lancedb.exceptions import MissingValueError, MissingColumnError

from dlt.common.destination.exceptions import (
    DestinationException,
    DestinationTerminalException,
    DestinationUndefinedEntity,
    DestinationTransientException,
)
from dlt.common.destination.client import JobClientBase
from dlt.common.typing import TFun
from dlt.destinations.impl.lance.exceptions import LANCE_DOES_NOT_EXIST, LANCE_NOT_FOUND

LANCEDB_UNDEFINED_ENTITY_PATTERN = re.compile(
    rf"(?i){LANCE_NOT_FOUND}|{LANCE_DOES_NOT_EXIST}|unknown\s+table|missing\s+value|missing\s+column"
)


def is_lancedb_not_found_error(error_message: str) -> bool:
    """Returns True if the error message indicates a missing namespace, table or column."""
    return bool(LANCEDB_UNDEFINED_ENTITY_PATTERN.search(error_message))


class LanceDBCommitTagNotApplied(DestinationTerminalException):
    def __init__(
        self, tag: str, table_name: str, version: int, database: str, load_id: str
    ) -> None:
        # raised after the load is committed, so `dlt` will not retry it and the user must finish
        remediation = "\n".join(
            [
                "    import lancedb",
                f'    db = lancedb.connect("db://{database}", api_key=..., host_override=...)',
                f'    db.open_table("{table_name}").tags.create("{tag}", {version})',
            ]
        )
        super().__init__(
            f"Load {load_id} is committed but the commit tag `{tag}` could not be applied to"
            f" `{table_name}` at version {version}. The data is loaded and complete, only the tag"
            f" is missing, so the dataset cannot be rolled back to `{tag}` and that version is not"
            " pinned against the background cleanup of the cluster. `dlt` does not retry a"
            f" committed load, so create the tag yourself:\n\n{remediation}\n"
        )


def lancedb_error(f: TFun) -> TFun:
    @wraps(f)
    def _wrap(self: JobClientBase, *args: Any, **kwargs: Any) -> Any:
        try:
            return f(self, *args, **kwargs)
        except DestinationException:
            # already converted (eg. raised by a nested decorated call)
            raise
        except (MissingValueError, MissingColumnError) as status_ex:
            raise DestinationUndefinedEntity(status_ex) from status_ex
        except Exception as e:
            # the managed client reports missing entities as untyped errors from the server
            if is_lancedb_not_found_error(str(e)):
                raise DestinationUndefinedEntity(e) from e
            raise DestinationTransientException(e) from e

    return _wrap  # type: ignore[return-value]
