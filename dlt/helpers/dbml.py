import pathlib
from collections.abc import Mapping
from typing import Any, Literal, Union, overload, Optional, cast

import dlt
from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema.typing import (
    C_DLT_LOAD_ID,
    VERSION_TABLE_NAME,
    TColumnSchema,
    TTableReference,
    TTableSchema,
    TStoredSchema,
)
from dlt.common.schema.utils import (
    create_load_table_reference,
    create_version_and_loads_hash_reference,
    create_version_and_loads_schema_name_reference,
    get_data_and_dlt_tables,
    get_first_column_name_with_prop,
    group_tables_by_resource,
    create_root_child_reference,
    create_parent_child_reference,
    is_nested_table,
    remove_column_defaults,
)
from dlt.common.utils import without_none

try:
    from pydbml import Database  # type: ignore[import-untyped]
    from pydbml.classes import Table, Column, Reference, TableGroup  # type: ignore[import-untyped]
except ModuleNotFoundError:
    raise MissingDependencyException("PyDBML", ["pydbml>=1.2.0"])


__all__ = (
    "schema_to_dbml",
    "export_to_dbml",
)

# dlt sets no `data_type` field for columns that haven't seen data yet.
# Since DBML doesn't support empty `type` field, we set a constant
# NOTE should we use `UNKNOWN` or `None` or something else?
UNKNOWN_DATA_TYPE = "UNKNOWN"
TDBMLReferenceCardinality = Literal["<", ">", "-", "<>"]


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
            value = str(v)

        output[k] = value

    return output


def _to_dbml_column(column_hints: TColumnSchema) -> Column:
    """Convert a dlt column schema to a DBML column

    `properties` include remaining dlt hints. They can be rendered by `PyDBML`, but
    most DBML frontends will raised errors when included.
    """
    # remove hints that can be retrieved from DBML `Column()`
    properties = remove_column_defaults(
        cast(
            TColumnSchema,
            {
                k: v
                for k, v in column_hints.items()
                if k
                not in (
                    "name",
                    "data_type",
                    "unique",
                    "nullable",
                    "primary_key",
                    "description",
                    "x-normalizer",
                    "x-loader",
                    "x-extractor",
                )
            },
        )
    )
    return Column(
        name=column_hints["name"],
        type=column_hints.get("data_type", UNKNOWN_DATA_TYPE),
        unique=bool(column_hints.get("unique")),
        not_null=not bool(column_hints.get("nullable", True)),
        pk=bool(column_hints.get("primary_key")),
        note=column_hints.get("description"),
        properties=None if properties == {} else _stringify_dict(properties),
    )


def _from_dbml_column(column: Column) -> TColumnSchema:
    """Convert a DBML column to a dlt column schema"""
    hints = cast(
        TColumnSchema,
        {
            "name": column.name,
            "data_type": None if column.type == UNKNOWN_DATA_TYPE else column.type,
            "unique": column.unique,
            "nullable": not column.not_null,
            "primary_key": column.pk,
            "description": None if column.note.text == "" else column.note.text,
            # `column.properties` should contain keys mutually exclusive with the above
            **_destringify_dict(column.properties),
        },
    )
    return cast(TColumnSchema, remove_column_defaults(hints))


def _to_dbml_table(table_hints: TTableSchema) -> Table:
    """Convert a dlt table schema to a DBML table

    `properties` include remaining dlt hints. They can be rendered by `PyDBML`, but
    most DBML frontends will raised errors when included.
    """
    # remove hints that can be retrieved from DBML `Table()`
    properties = {
        k: v for k, v in table_hints.items() if k not in ("name", "columns", "description")
    }
    return Table(
        name=table_hints["name"],
        columns=[_to_dbml_column(col) for col in table_hints["columns"].values()],
        note=table_hints.get("description"),
        properties=None if properties == {} else _stringify_dict(properties),
    )


def _from_dbml_table(table: Table) -> TTableSchema:
    """Convert a DBML table to a dlt table schema"""
    original_hints = _destringify_dict(table.properties)
    original_hints.pop("columns", None)
    columns = {col.name: _from_dbml_column(col) for col in table.columns}
    retrieved_hints = {
        "name": table.name,
        "columns": columns,
        "description": None if table.note.text == "" else table.note.text,
        **original_hints,
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
    """Convert a dlt table reference to a DBML reference

    NOTE DBML can represent relationship cardinality, but `dlt` doesn't store this information.
    The calling function specifies the loosest possible cardinality.
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
    """Convert a DBML reference to a dlt table reference"""
    return TTableReference(
        referenced_table=reference.col2[0].table.name,
        columns=[col.name for col in reference.col1],
        referenced_columns=[col.name for col in reference.col2],
    )


# NOTE `TableGroup` seem to not be displayed on `dbdiagram.io`
def _group_tables_by_resource(schema: TStoredSchema, db: Database) -> list[TableGroup]:
    """Create DBML table groups for dlt tables.

    Data tables are grouped by resource.
    Internal dlt tables are grouped together despite each having its own resource.
    """
    _, dlt_tables = get_data_and_dlt_tables(schema["tables"])
    dlt_table_names = [t["name"] for t in dlt_tables]
    table_groups = []

    # data tables groups
    for resource, tables in group_tables_by_resource(schema["tables"]).items():
        group_members_table_name = [table["name"] for table in tables]
        group_members_dbml_tables: list[Table] = []
        for dbml_table in db.tables:
            if dbml_table.name in dlt_table_names:
                continue

            if dbml_table.name in group_members_table_name:
                group_members_dbml_tables.append(dbml_table)

        # avoid creating empty groups
        if group_members_dbml_tables:
            table_groups.append(TableGroup(name=resource, items=group_members_dbml_tables))

    # internal dlt tables group
    dlt_dbml_tables = [dbml_table for dbml_table in db.tables if dbml_table.name in dlt_table_names]
    dlt_group = TableGroup(name="_dlt", items=dlt_dbml_tables)
    table_groups.append(dlt_group)

    return table_groups


def _add_tables(
    schema: TStoredSchema,
    dbml_schema: Database,
    *,
    include_dlt_tables: bool,
) -> Database:
    """Add a DBML Table for each dlt table.

    The operation is in-place and returns the `dbml_schema` object passed as input.

    Args:
        schema: dlt schema to convert
        dbml_schema: DBML schema
        include_dlt_tables: If True, include data tables and internal dlt tables.

    Returns:
        DBML schema
    """
    data_tables, dlt_tables = get_data_and_dlt_tables(schema["tables"])

    if include_dlt_tables is True:
        # order allows to keep _dlt tables at the end
        tables = data_tables + dlt_tables
    else:
        tables = data_tables

    for table in tables:
        # skip tables that have no columns; DBML table requires at least one column.
        if not table.get("columns"):
            continue

        dbml_table = _to_dbml_table(table)
        dbml_schema.add_table(dbml_table)

    return dbml_schema


def _add_references(
    schema: TStoredSchema,
    dbml_schema: Database,
    *,
    include_internal_dlt_ref: bool,
    include_parent_child_ref: bool,
    include_root_child_ref: bool,
) -> Database:
    """Add a DBML Reference for each dlt reference.

    The operation is in-place and returns the `dbml_schema` object passed as input.

    Args:
        schema: dlt schema to convert
        dbml_schema: DBML schema to add references to
        include_internal_dlt_ref: If True, include references from `root._dlt_load_id` to `_dlt_loads.load_id`
            If `_dlt_version` and `_dlt_loads` are present, include references between them.
        include_parent_child_ref: If True, include references from `child._dlt_parent_id` to `parent._dlt_id`
        include_root_child_ref: If True, include references from `child._dlt_root_id` to `root._dlt_id`

    Returns:
        DBML schema
    """
    # need to create all tables first because `Reference` references `Table` objects
    for table in schema["tables"].values():
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
                reference=create_parent_child_reference(schema["tables"], table_name),
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
                reference=create_root_child_reference(schema["tables"], table_name),
                cardinality=">",  # m-to-1
            )
            dbml_schema.add_reference(dbml_root_reference)

    # generate links between internal dlt tables
    if (
        VERSION_TABLE_NAME in schema["tables"]
        and VERSION_TABLE_NAME in schema["tables"]
        and include_internal_dlt_ref is True
    ):
        # a schema version hash can have multiple runs in the loads table
        # schema version hash is unique
        # possible: cardinality: `-` (1-to-1) or `<` (1-to-m)
        dbml_version_and_loads_hash_ref = _to_dbml_reference(
            tables=dbml_schema.tables,
            from_table_name=VERSION_TABLE_NAME,
            reference=create_version_and_loads_hash_reference(schema["tables"]),
            cardinality="<",
        )
        # a schema name can have multiple multiple runs in the loads table
        # schema name is not unique; it can have multiple version hash
        # possible: cardinality: `-` (1-to-1), `<` (1-to-m), or `<>` (m-to-n)
        dbml_version_and_loads_schema_name_ref = _to_dbml_reference(
            tables=dbml_schema.tables,
            from_table_name=VERSION_TABLE_NAME,
            reference=create_version_and_loads_schema_name_reference(schema["tables"]),
            cardinality="<>",
        )

        dbml_schema.add_reference(dbml_version_and_loads_hash_ref)
        dbml_schema.add_reference(dbml_version_and_loads_schema_name_ref)

    return dbml_schema


def _add_table_groups(schema: TStoredSchema, dbml_schema: Database) -> Database:
    """Add a DBML TableGroup to group tables by resources

    The operation is in-place and returns the `dbml_schema` object passed as input.

    Args:
        schema: dlt schema to convert
        dbml_schema: DBML schema

    Returns:
        DBML schema
    """
    for table_group in _group_tables_by_resource(schema, dbml_schema):
        # operation is inplace
        dbml_schema.add_table_group(table_group)

    return dbml_schema


def schema_to_dbml(
    schema: TStoredSchema,
    *,
    allow_custom_dbml_properties: bool = False,
    include_dlt_tables: bool = True,
    include_internal_dlt_ref: bool = True,
    include_parent_child_ref: bool = True,
    include_root_child_ref: bool = True,
    group_by_resource: bool = False,
) -> Database:
    """Convert a dlt.Schema object to a PyDBML representation.

    Args:
        schema: dlt schema to convert
        allow_custom_dbml_properties: If True, store all dlt metadata on DBML properties. This will
            cause rendering errors in most DBML frontends. If True, dlt to DBML all metadata is preserved.
            If False, the conversion loses some details to reconstruct the dlt schema.
        include_dlt_tables: If True, include data tables and internal dlt tables. This will influence table
            references and groups produced.
        include_internal_dlt_ref: If True, include references between tables `_dlt_version`, `_dlt_loads` and `_dlt_pipeline_state`
        include_parent_child_ref: If True, include references from `child._dlt_parent_id` to `parent._dlt_id`
        include_root_child_ref: If True, include references from `child._dlt_root_id` to `root._dlt_id`
        group_by_resource: If True, group tables by resource.

    Returns:
        DBML schema
    """

    dbml_schema = Database(allow_properties=allow_custom_dbml_properties)

    _add_tables(schema, dbml_schema, include_dlt_tables=include_dlt_tables)
    _add_references(
        schema,
        dbml_schema,
        include_internal_dlt_ref=include_internal_dlt_ref,
        include_parent_child_ref=include_parent_child_ref,
        include_root_child_ref=include_root_child_ref,
    )
    if group_by_resource is True:
        _add_table_groups(schema, dbml_schema)

    return dbml_schema


# TODO convert DBML back to dlt schema
# def dbml_to_schema(dbml_schema: Union[Database, str]) -> TStoredSchema:
#     return


@overload
def export_to_dbml(schema: dlt.Schema, *, path: None = None) -> str:
    """Convert `dlt.Schema` to DBML and return the string"""
    ...


@overload
def export_to_dbml(schema: dlt.Schema, *, path: Union[pathlib.Path, str]) -> str:
    """Convert `dlt.Schema` to DBML and write result to path at specified `path`"""
    ...


# TODO this function might not be needed
def export_to_dbml(
    schema: dlt.Schema,
    *,
    path: Optional[Union[pathlib.Path, str]] = None,
    allow_custom_dbml_properties: bool = False,
    include_dlt_tables: bool = True,
    include_internal_dlt_ref: bool = True,
    include_parent_child_ref: bool = True,
    include_root_child_ref: bool = True,
    group_by_resource: bool = False,
) -> str:
    """Convert a `dlt.Schema` to a a DBML string and return its value.

    Args:
        schema: dlt schema to convert
        path: If specified, write the DBML string to the file at the specified path.
        allow_custom_dbml_properties: If True, store all dlt metadata on DBML properties. This will
            cause rendering errors in most DBML frontends. If True, dlt to DBML all metadata is preserved.
            If False, the conversion loses some details to reconstruct the dlt schema.
        include_dlt_tables: If True, include data tables and internal dlt tables. This will influence table
            references and groups produced.
        include_internal_dlt_ref: If True, include references between tables `_dlt_version`, `_dlt_loads` and `_dlt_pipeline_state`
        include_parent_child_ref: If True, include references from `child._dlt_parent_id` to `parent._dlt_id`
        include_root_child_ref: If True, include references from `child._dlt_root_id` to `root._dlt_id`
        group_by_resource: If True, group tables by resource.

    Returns:
        DBML string
    """
    stored_schema = schema.to_dict()
    dbml_schema = schema_to_dbml(
        stored_schema,
        allow_custom_dbml_properties=allow_custom_dbml_properties,
        include_dlt_tables=include_dlt_tables,
        include_internal_dlt_ref=include_internal_dlt_ref,
        include_parent_child_ref=include_parent_child_ref,
        include_root_child_ref=include_root_child_ref,
        group_by_resource=group_by_resource,
    )
    dbml_string: str = dbml_schema.dbml

    if path is None:
        return dbml_string

    path = pathlib.Path(path)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)

    if path.suffix != ".dbml":
        path = path.with_suffix(".dbml")

    path.write_text(dbml_string)
    return dbml_string
