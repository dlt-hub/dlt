from typing import Any, Dict, Literal, Optional, cast

from dlt.common.schema.typing import TTableSchemaColumns
from dlt.destinations.utils import get_resource_for_adapter
from dlt.extract import DltResource
from dlt.extract.items import TTableHintTemplate


TABLE_COMMENT_HINT: Literal["x-snowflake-table-comment"] = "x-snowflake-table-comment"
TABLE_TAGS_HINT: Literal["x-snowflake-table-tags"] = "x-snowflake-table-tags"
COLUMN_TAGS_HINT: Literal["x-snowflake-column-tags"] = "x-snowflake-column-tags"
# COLUMN_COMMENT_HINT is already defined in snowflake.py (the destination
# resolves it together with the generic `description` column hint).
# Re-exported here so adapter callers don't need to know which module it lives in.
from dlt.destinations.impl.snowflake.snowflake import COLUMN_COMMENT_HINT


def _validate_tags_dict(tags: Dict[str, Any], *, label: str) -> None:
    """Snowflake tags are first-class objects with names + string values.
    Validate the user-supplied shape early so the eventual ALTER TABLE
    SET TAG runs cleanly."""
    if not isinstance(tags, dict):
        raise ValueError(
            f"`{label}` must be a dict of {{tag_name: value}}. Snowflake tags"
            " differ from Databricks tags in that the tag name must reference"
            " a tag object that already exists in your Snowflake account"
            " (see CREATE TAG). The adapter does NOT create tag objects —"
            " issue CREATE TAG <name> via your governance tooling first."
        )
    for k, v in tags.items():
        if not isinstance(k, str) or not k.strip():
            raise ValueError(
                f"`{label}` keys must be non-empty strings naming a tag object."
                " Qualified names (db.schema.tag) and unqualified names are both"
                f" accepted. Got: {k!r}"
            )
        if not isinstance(v, (str, int, bool, float)):
            raise ValueError(
                f"`{label}[{k!r}]` value must be a string, int, bool, or float."
                f" Got {type(v).__name__}. Snowflake stores tag values as STRING;"
                " non-strings are CAST to STRING."
            )


def snowflake_adapter(
    data: Any,
    table_comment: Optional[str] = None,
    table_tags: Optional[Dict[str, Any]] = None,
    column_comments: Optional[Dict[str, str]] = None,
    column_tags: Optional[Dict[str, Dict[str, Any]]] = None,
) -> DltResource:
    """Apply Snowflake-specific table and column metadata to a dlt resource.

    Mirrors the pattern of `databricks_adapter` / `bigquery_adapter`.
    The hints are stored as extension keys (`x-snowflake-…`) and emitted
    as DDL when the table is created or altered by the Snowflake
    destination:

    - `table_comment` → `COMMENT ON TABLE <name> IS '…'`. Falls back to
      the generic `description` table hint when not set, so any resource
      already using `apply_hints(additional_table_hints={"description": …})`
      gets table comments for free without re-wiring.
    - `table_tags` → `ALTER TABLE <name> SET TAG <key> = '<value>'` per
      pair. Tag objects must already exist in Snowflake (CREATE TAG); the
      adapter is intentionally non-DDL-creating to avoid privilege bloat
      on the loader role.
    - `column_comments` → emitted inline in CREATE TABLE as
      `<col> <type> COMMENT '<value>'`. The Snowflake destination already
      honors a `description` column hint; this argument is for callers who
      want to keep table-level prose separate from column metadata.
    - `column_tags` → `ALTER TABLE <name> ALTER COLUMN <col> SET TAG
      <key> = '<value>'` per (column, tag) pair.

    Args:
        data: A dlt resource (or anything dlt can wrap into one).
        table_comment: One-line human description of the table. Markdown
            is not interpreted by Snowflake — keep it short.
        table_tags: ``{tag_name: value}`` dict. Tag names may be
            qualified (``governance.cost_center``) or unqualified
            (resolved in the session's current schema).
        column_comments: ``{column_name: comment}`` dict. Equivalent
            to passing ``description`` in a ``columns={...}`` hint, but
            scoped per the adapter API.
        column_tags: ``{column_name: {tag_name: value, ...}}`` dict.
            Same tag semantics as ``table_tags``.

    Returns:
        The same `DltResource`, with hints applied. Chainable.

    Examples:
        >>> @dlt.resource
        ... def orders(): ...
        >>> snowflake_adapter(
        ...     orders,
        ...     table_comment="Shopify orders, one row per checkout",
        ...     table_tags={"governance.domain": "commerce"},
        ...     column_comments={"customer_email": "Buyer email (PII)"},
        ...     column_tags={"customer_email": {"governance.pii_class": "internal"}},
        ... )
    """
    resource = get_resource_for_adapter(data)

    additional_table_hints: Dict[str, TTableHintTemplate[Any]] = {}
    additional_column_hints: Dict[str, Dict[str, Any]] = {}

    if table_comment is not None:
        if not isinstance(table_comment, str):
            raise ValueError("`table_comment` must be a string.")
        additional_table_hints[TABLE_COMMENT_HINT] = table_comment

    if table_tags is not None:
        _validate_tags_dict(table_tags, label="table_tags")
        additional_table_hints[TABLE_TAGS_HINT] = dict(table_tags)

    if column_comments is not None:
        if not isinstance(column_comments, dict):
            raise ValueError("`column_comments` must be a dict of {column_name: comment}.")
        for col, comment in column_comments.items():
            if not isinstance(col, str) or not col:
                raise ValueError(f"`column_comments` keys must be non-empty column names. Got: {col!r}")
            if not isinstance(comment, str):
                raise ValueError(f"`column_comments[{col!r}]` must be a string.")
            additional_column_hints.setdefault(col, {"name": col})
            additional_column_hints[col][COLUMN_COMMENT_HINT] = comment

    if column_tags is not None:
        if not isinstance(column_tags, dict):
            raise ValueError(
                "`column_tags` must be a dict of {column_name: {tag_name: value, ...}}."
            )
        for col, tags in column_tags.items():
            if not isinstance(col, str) or not col:
                raise ValueError(f"`column_tags` keys must be non-empty column names. Got: {col!r}")
            _validate_tags_dict(tags, label=f"column_tags[{col!r}]")
            additional_column_hints.setdefault(col, {"name": col})
            additional_column_hints[col][COLUMN_TAGS_HINT] = dict(tags)

    if not additional_table_hints and not additional_column_hints:
        raise ValueError(
            "`snowflake_adapter` requires at least one of `table_comment`,"
            " `table_tags`, `column_comments`, or `column_tags`."
        )

    resource.apply_hints(
        columns=(
            cast(TTableSchemaColumns, additional_column_hints)
            if additional_column_hints
            else None
        ),
        additional_table_hints=additional_table_hints or None,
    )
    return resource
