"""Compiles the declarative SQL database config into `sql_table` arguments and table hints."""

from typing import Any, get_args

from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.exceptions import DictValidationException
from dlt.common.libs.sql_alchemy import Engine
from dlt.common.typing import get_type_hints
from dlt.common.utils import exclude_keys
from dlt.common.validation import validate_dict
from dlt.extract.hints import TResourceHintsBase
from dlt.extract.incremental import Incremental
from dlt.extract.resource import DltResource
from dlt.sources.sql_database.typing import (
    SqlDatabaseConfig,
    SqlTableResource,
    SqlTableResourceBase,
)

SQL_TABLE_ARGS = frozenset(
    (
        "table",
        "schema",
        "incremental",
        "chunk_size",
        "backend",
        "backend_kwargs",
        "reflection_level",
        "defer_table_reflect",
        "included_columns",
        "excluded_columns",
        "resolve_foreign_keys",
        "table_adapter_callback",
        "type_adapter_callback",
        "query_adapter_callback",
        "table_loader_class",
        "write_disposition",
        "primary_key",
        "merge_key",
    )
)
"""Arguments passed to `sql_database()` source factory."""

RESOURCE_ARGS = frozenset(("max_table_nesting", "selected", "parallelized"))
"""Arguments passed to an individual resource after creation."""

TABLE_HINT_ARGS = frozenset(get_type_hints(TResourceHintsBase)) - SQL_TABLE_ARGS
"""Other table hints that can be applied via `.apply_hints()`, but not `__init__()` args."""


def validate_config(config: SqlDatabaseConfig) -> None:
    """Validates `config` against `SqlDatabaseConfig` config specs.

    `credentials` are validated separately to ensure they are never included in an error message.
    """
    credentials = config.get("credentials")
    if credentials is not None and not isinstance(
        credentials, (str, ConnectionStringCredentials, Engine)
    ):
        raise DictValidationException(
            msg="field `credentials` expects a connection string, `ConnectionStringCredentials` or"
            f" `Engine` instance but got `{type(credentials).__name__}`",
            path=".",
            field="credentials",
        )

    validate_dict(
        SqlDatabaseConfig,
        exclude_keys(config, {"credentials"}),
        path=".",
        validator_f=_validate_class_type,
    )
    for index, table in enumerate(config.get("tables") or []):
        if isinstance(table, (str, DltResource)):
            continue

        if not (table.get("name") or table.get("table")):
            raise DictValidationException(
                "table must define `name` (the resource name), `table` (the name of the table in"
                " the database) or both",
                f"./tables[{index}]",
            )


def merge_table_defaults(
    table_defaults: SqlTableResourceBase,
    table: str | SqlTableResource,
) -> SqlTableResource:
    """Merges `table_defaults` into a single table config and resolves `name` and `table`,
    each defaulting to the other. Table settings take precedence over the defaults.
    """
    if isinstance(table, str):
        table = SqlTableResource(table=table)

    merged = SqlTableResource({**table_defaults, **table})
    merged["name"] = merged.get("name") or merged["table"]
    merged["table"] = merged.get("table") or merged["name"]
    return merged


def split_table_config(
    table: SqlTableResource,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Splits a table config into `sql_table` arguments, `apply_hints` arguments and
    settings of the resource itself.
    """
    table_args: dict[str, Any] = {
        key: value for key, value in table.items()
        if key in SQL_TABLE_ARGS
    }
    if incremental := table.get("incremental"):
        table_args["incremental"] = Incremental.ensure_instance(incremental)

    return (
        table_args,
        {key: value for key, value in table.items() if key in TABLE_HINT_ARGS},
        {key: value for key, value in table.items() if key in RESOURCE_ARGS},
    )


def _validate_class_type(path: str, pk: str, pv: Any, t: Any) -> bool:
    """Validates `Type[C]` fields ie. `table_loader_class`, which `validate_dict` skips."""
    if getattr(t, "__origin__", None) is not type:
        return False
    (expected,) = get_args(t)
    if not (isinstance(pv, type) and issubclass(pv, expected)):
        raise DictValidationException(
            f"field `{pk}` expects a subclass of `{expected.__name__}` but got `{pv}`",
            path,
            t,
            pk,
            pv,
        )
    return True
