from collections.abc import Mapping
import pathlib
from typing import Any, Literal, Union, overload, Optional, cast

import dlt
from dlt.common.json import json
from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema.typing import (
    C_DLT_LOAD_ID,
    VERSION_TABLE_NAME,
    TColumnSchema,
    TTableReference,
    TTableSchema,
)
from dlt.common.schema.utils import (
    create_load_table_reference,
    create_version_and_loads_hash_reference,
    create_version_and_loads_schema_name_reference,
    get_first_column_name_with_prop,
    group_tables_by_resource,
    create_root_child_reference,
    create_parent_child_reference,
    is_nested_table,
)
from dlt.common.utils import without_none

try:
    from pydbml import Database  # type: ignore[import-untyped]
    from pydbml.classes import Table, Column, Reference, TableGroup  # type: ignore[import-untyped]
except ModuleNotFoundError:
    raise MissingDependencyException("PyDBML", ["pydbml>=1.2.0"])


TDBMLReferenceCardinality = Literal["<", ">", "-", "<>"]


def _stringify_dict(d: Mapping[str, Any]) -> dict[str, str]:
    return {k:json.dumps(v) for k, v in d.items()}


def _destringify_dict(d: Mapping[str, str]) -> dict[str, Union[bool, None, str]]:
    return {k:json.loads(v) for k,v in d.items()}


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


# TODO store cardinality directly on TTTableReference
# NOTE iterating over all tables each time we create a reference can become inefficient
def _to_dbml_reference(
    *,
    from_table_name: str,
    reference: TTableReference,
    tables: list[Table],
    cardinality: TDBMLReferenceCardinality,
) -> Reference:
    """Convert a dlt table reference to a dbml reference

    NOTE DBML doesn't have a concept of `from` and `to` columns. DBML can represent
    relationship cardinality, but `dlt` doesn't store this information. We default to `"-"`
    """
    from_columns = reference["columns"]
    to_table = reference["referenced_table"]
    to_columns = reference["referenced_columns"]

    from_dbml_columns: list[Column] = []
    to_dbml_columns: list[Column] = []

    for table in tables:
        if table.name == from_table_name:
            for column in table.columns:
                if column.name == from_columns[0]:
                    from_dbml_columns.append(column)

        if table.name == to_table:
            for column in table.columns:
                if column.name == to_columns[0]:
                    to_dbml_columns.append(column)

    if not from_dbml_columns:
        raise ValueError(f"Column `{from_columns}` not found in table `{from_table_name}`")

    if not to_dbml_columns:
        raise ValueError(f"Column `{to_columns}` not found in table `{to_table}`")

    return Reference(type=cardinality, col1=from_dbml_columns, col2=to_dbml_columns)


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


def _add_tables(
    schema: dlt.Schema,
    dbml_schema: Database,
    *,
    include_dlt_tables: bool,
) -> Database:
    """Add a DBML Table for each dlt table.

    The operation is in-place and returns the `dbml_schema` object passed as input.
    """
    tables: list[TTableSchema]
    if include_dlt_tables is True:
        # the unpacking order ensures dlt tables are at the end
        tables = [*schema.data_tables(), *schema.dlt_tables()]
    else:
        tables = schema.dlt_tables()

    for table in tables:
        # skip tables that have no columns; DBML table requires at least one column.
        if not table.get("columns"):
            continue

        dbml_table = _to_dbml_table(table)
        dbml_schema.add_table(dbml_table)

    return dbml_schema


def _add_references(
    schema: dlt.Schema,
    dbml_schema: Database,
    *,
    include_dlt_tables: bool,
    include_internal_dlt_ref: bool,
    include_parent_child_ref: bool,
    include_root_child_ref: bool,
) -> Database:
    """Add a DBML Reference for each dlt reference.

    The operation is in-place and returns the `dbml_schema` object passed as input.
    """
    # need to create all tables first because `Reference` references `Table` objects
    for table in schema.tables.values():
        table_name: str = table["name"]
        for reference in table.get("references", []):
            # user-defined references can have arbitrary cardinality and may incorrectly describe the data
            dbml_reference = _to_dbml_reference(
                tables=dbml_schema.tables,
                from_table_name=table_name,
                reference=reference,
                cardinality="<>",  # because it's the loosest
            )
            dbml_schema.add_reference(dbml_reference)

        # link root -> loads table
        if include_internal_dlt_ref is True and bool(table["columns"].get(C_DLT_LOAD_ID)):
            # root table contains 1 to many rows associated with a single row in loads table
            # possible cardinality: `-` (1-to-1) or `>` (m-to-1)
            dbml_reference = _to_dbml_reference(
                tables=dbml_schema.tables,
                from_table_name=table_name,
                reference=create_load_table_reference(table),
                cardinality=">",  # m-to-1
            )
            dbml_schema.add_reference(dbml_reference)

        # link child -> parent
        if include_parent_child_ref is True and is_nested_table(table):
            # child table contains 1 to many rows associated with has a single row in parent table
            # possible cardinality: `-` (1-to-1) or `>` (m-to-1)
            dbml_parent_reference = _to_dbml_reference(
                tables=dbml_schema.tables,
                from_table_name=table_name,
                reference=create_parent_child_reference(schema.tables, table_name),
                cardinality=">",  # m-to-1
            )
            dbml_schema.add_reference(dbml_parent_reference)

        # link child -> root
        if (
            include_root_child_ref is True
            and is_nested_table(table)
            # the table must have a root key column; can be enabled via `@dlt.source(root_key=True)` or write_disposition
            and get_first_column_name_with_prop(table, "root_key")
        ):
            # child table contains 1 to many rows associated with has a single row in root table
            # possible cardinality: `-` (1-to-1) or `>` (m-to-1)
            dbml_root_reference = _to_dbml_reference(
                tables=dbml_schema.tables,
                from_table_name=table_name,
                reference=create_root_child_reference(schema.tables, table_name),
                cardinality=">",  # m-to-1
            )
            dbml_schema.add_reference(dbml_root_reference)

    # generate links between internal dlt tables
    if include_dlt_tables is True and include_internal_dlt_ref is True:
        # a schema version hash can have multiple runs in the loads table
        # schema version hash is unique
        # possible: cardinality: `-` (1-to-1) or `<` (1-to-m)
        dbml_version_and_loads_hash_ref = _to_dbml_reference(
            tables=dbml_schema.tables,
            from_table_name=VERSION_TABLE_NAME,
            reference=create_version_and_loads_hash_reference(schema.tables),
            cardinality="<",
        )
        # a schema name can have multiple multiple runs in the loads table
        # schema name is not unique; it can have multiple version hash
        # possible: cardinality: `-` (1-to-1), `<` (1-to-m), or `<>` (m-to-n)
        dbml_version_and_loads_schema_name_ref = _to_dbml_reference(
            tables=dbml_schema.tables,
            from_table_name=VERSION_TABLE_NAME,
            reference=create_version_and_loads_schema_name_reference(schema.tables),
            cardinality="<>",
        )

        dbml_schema.add_reference(dbml_version_and_loads_hash_ref)
        dbml_schema.add_reference(dbml_version_and_loads_schema_name_ref)

    return dbml_schema


def _add_table_groups(schema: dlt.Schema, dbml_schema: Database) -> Database:
    """Add a DBML TableGroup to group tables by resources

    The operation is in-place and returns the `dbml_schema` object passed as input.
    """
    for table_group in _group_tables_by_resource(schema, dbml_schema):
        # operation is inplace
        dbml_schema.add_table_group(table_group)

    return dbml_schema


def schema_to_dbml(
    schema: dlt.Schema,
    *,
    include_dlt_tables: bool = True,
    include_internal_dlt_ref: bool = True,
    include_parent_child_ref: bool = True,
    include_root_child_ref: bool = True,
    group_by_resource: bool = False,
    **kwargs: Any,
) -> Database:
    """Convert a dlt.Schema object to a PyDBML representation."""

    dbml_schema = Database()

    _add_tables(schema, dbml_schema, include_dlt_tables=include_dlt_tables)
    _add_references(
        schema,
        dbml_schema,
        include_dlt_tables=include_internal_dlt_ref,
        include_internal_dlt_ref=include_internal_dlt_ref,
        include_parent_child_ref=include_parent_child_ref,
        include_root_child_ref=include_root_child_ref,
    )
    if group_by_resource is True:
        _add_table_groups(schema, dbml_schema)

    return dbml_schema


@overload
def export_to_dbml(schema: dlt.Schema, *, path: None = None) -> str:
    """Convert `dlt.Schema` to DBML and return the string"""
    ...


@overload
def export_to_dbml(schema: dlt.Schema, *, path: Union[pathlib.Path, str]) -> None:
    """Convert `dlt.Schema` to DBML and write result to path at specified `path`"""
    ...


# TODO this function might not be needed
def export_to_dbml(
    schema: dlt.Schema,
    *,
    path: Optional[Union[pathlib.Path, str]] = None,
    **schema_to_dbml_kwargs: Any,
) -> Optional[str]:
    """Convert a `dlt.Schema` to a a DBML string.
    If `path` is specified, write to file.
    Else, return the string.
    """
    dbml_schema = schema_to_dbml(schema, **schema_to_dbml_kwargs)
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
