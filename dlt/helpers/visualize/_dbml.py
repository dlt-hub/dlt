from collections.abc import Mapping
import pathlib
from typing import Any, Union, overload, Optional, cast

import dlt
from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema.typing import TColumnSchema, TTableReference, TTableSchema
from dlt.common.schema.utils import group_tables_by_resource
from dlt.common.utils import without_none

try:
    from pydbml import Database  # type: ignore[import-untyped]
    from pydbml.classes import Table, Column, Reference, TableGroup  # type: ignore[import-untyped]
except ModuleNotFoundError:
    raise MissingDependencyException("PyDBML", ["pydbml>=1.2.0"])


DEFAULT_RELATION_TYPE = "-"


def _stringify_dict(d: Mapping[str, Any]) -> dict[str, str]:
    return {k: str(v) for k, v in d.items()}


def _destringify_dict(d: Mapping[str, str]) -> dict[str, Union[bool, None, str]]:
    output = {}
    for k, v in d.items():
        value: Union[bool, None, str]
        if v == "True":
            value = True
        elif v == "False":
            value = False
        elif v == "None":
            value = None
        else:
            value = v

        output[k] = value

    return output


def _to_dbml_column(hints: TColumnSchema) -> Column:
    """Convert a dlt column schema to a dbml column

    `properties` are not rendered; we use them to store the original hints.

    NOTE: dbml column has boolean properties and doesn't support `None`.
    For example, we can't disambiguate `unique=False` in DBML from
    `unique=False or `unique=None` in dlt.
    """
    return Column(
        name=hints["name"],
        type=hints["data_type"],
        unique=hints.get("unique") is True,
        not_null=hints.get("nullable") is False,
        pk=hints.get("primary_key") is True,
        note=hints.get("description"),
        # we stringify because of `pydbml` constraints
        properties=_stringify_dict(hints),
    )


def _from_dbml_column(column: Column) -> TColumnSchema:
    """Convert a dbml column to a dlt column schema"""
    original_hints = _destringify_dict(column.properties)
    retrieved_hints = {
        "name": column.name,
        "data_type": column.type,
        "unique": None if original_hints.get("unique") is None else column.unique,
        "nullable": None if original_hints.get("nullable") is None else not column.not_null,
        "primary_key": None if original_hints.get("primary_key") is None else column.pk,
        "description": None if original_hints.get("description") is None else column.note.text,
    }
    return cast(TColumnSchema, without_none(retrieved_hints))


def _to_dbml_table(table: TTableSchema) -> Table:
    """Convert a dlt table schema to a dbml table

    `properties` are not rendered; we use them to store the original hints.
    """
    columns = [_to_dbml_column(col) for col in table["columns"].values()]
    return Table(
        name=table["name"],
        columns=columns,
        note=table.get("description"),
        properties=_stringify_dict(table),
    )


def _from_dbml_table(table: Table) -> TTableSchema:
    """Convert a dbml table to a dlt table schema"""
    original_hints = _destringify_dict(table.properties)
    columns = {col.name: _from_dbml_column(col) for col in table.columns}
    retrieved_hints = {
        "name": table.name,
        "columns": columns,
        "description": None if original_hints.get("description") is None else table.note.text,
    }
    return cast(TTableSchema, without_none(retrieved_hints))


def _to_dbml_reference(
    *, from_table: str, from_columns: str, to_table: str, to_columns: str, tables: list[Table]
) -> Reference:
    """Convert a dlt table reference to a dbml reference

    NOTE DBML doesn't have a concept of `from` and `to` columns. DBML can represent
    relationship cardinality, but `dlt` doesn't store this information. We default to `"-"`
    """
    from_dbml_columns: list[Column] = []
    to_dbml_columns: list[Column] = []

    for table in tables:
        if table.name == from_table:
            for column in table.columns:
                if column.name == from_columns:
                    from_dbml_columns.append(column)

        if table.name == to_table:
            for column in table.columns:
                if column.name == to_columns:
                    to_dbml_columns.append(column)

    if not from_dbml_columns:
        raise ValueError(f"Column `{from_columns}` not found in table `{from_table}`")

    if not to_dbml_columns:
        raise ValueError(f"Column `{to_columns}` not found in table `{to_table}`")

    return Reference(type=DEFAULT_RELATION_TYPE, col1=from_dbml_columns, col2=to_dbml_columns)


def _from_dbml_reference(reference: Reference) -> TTableReference:
    """Convert a dbml reference to a dlt table reference"""
    return TTableReference(
        referenced_table=reference.col2[0].table.name,
        columns=[col.name for col in reference.col1],
        referenced_columns=[col.name for col in reference.col2],
    )


# NOTE `TableGroup` seem to not be displayed on `dbdiagram.io`
def _group_tables_by_resource(schema: dlt.Schema, db: Database) -> list[TableGroup]:
    table_groups = []
    data_tables = dict(zip(schema.data_table_names(), schema.data_tables()))

    for resource, tables in group_tables_by_resource(data_tables).items():
        table_names_in_group = [table["name"] for table in tables]
        dbml_tables = [
            dbml_table for dbml_table in db.tables if dbml_table.name in table_names_in_group
        ]
        table_groups.append(TableGroup(name=resource, items=dbml_tables))

    dlt_dbml_tables = [
        dbml_table for dbml_table in db.tables if dbml_table.name in schema.dlt_table_names()
    ]
    dlt_group = TableGroup(name="_dlt", items=dlt_dbml_tables)
    table_groups.append(dlt_group)

    return table_groups


def schema_to_dbml(
    schema: dlt.Schema,
    *,
    group_by_resource: bool = False,
) -> Database:
    db = Database()

    for table in schema.tables.values():
        # skip tables that have no columns;
        # a DBML table must have at least one column.
        if not table.get("columns"):
            continue

        dbml_table = _to_dbml_table(table)
        db.add_table(dbml_table)

    # need to create all tables first because `Reference` requires `Table` objects
    for table in schema.tables.values():
        for reference in table.get("references", []):
            dbml_reference = _to_dbml_reference(
                from_table=table["name"],
                from_columns=reference["columns"][0],
                to_table=reference["referenced_table"],
                to_columns=reference["referenced_columns"][0],
                tables=db.tables,
            )
            # modify in place the `db` object
            # that we pass to `_to_dbml_reference()`
            db.add_reference(dbml_reference)

    if group_by_resource is True:
        for table_group in _group_tables_by_resource(schema, db):
            # operation is inplace
            db.add_table_group(table_group)

    return db


@overload
def export_to_dbml(schema: dlt.Schema, *, path: None = None) -> str:
    """Convert `dlt.Schema` to DBML and return the string"""
    ...


@overload
def export_to_dbml(schema: dlt.Schema, *, path: Union[pathlib.Path, str]) -> None:
    """Convert `dlt.Schema` to DBML and write result to path at specified `path`"""
    ...


def export_to_dbml(
    schema: dlt.Schema,
    *,
    path: Optional[Union[pathlib.Path, str]] = None,
) -> Optional[str]:
    """Convert a `dlt.Schema` to a a DBML string.
    If `path` is specified, write to file.
    Else, return the string.
    """
    dbml_schema = schema_to_dbml(schema)
    dbml_string: str = dbml_schema.dbml

    if path is None:
        return dbml_string

    path = pathlib.Path(path)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)

    if path.suffix != ".dbml":
        path = path.with_suffix(".dbml")

    path.write_text(dbml_string)
    return None
