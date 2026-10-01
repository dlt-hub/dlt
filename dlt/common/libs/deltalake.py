from packaging.version import Version
from collections.abc import Mapping
from typing import Optional, Dict, Union, List, Tuple, cast
from pathlib import Path

from dlt import version, Pipeline
from dlt.common import logger
from dlt.common.libs.pyarrow import pyarrow as pa
from dlt.common.libs.pyarrow import cast_arrow_schema_types
from dlt.common.libs.utils import load_open_tables
from dlt.common.schema.typing import UPSERT_MERGE_STRATEGIES, TWriteDisposition, TTableSchema
from dlt.common.schema.utils import get_first_column_name_with_prop, get_columns_names_with_prop
from dlt.common.destination.utils import get_merge_changed_cond
from dlt.common.exceptions import MissingDependencyException, ValueErrorWithKnownValues
from dlt.common.typing import DictStrAny
from dlt.common.utils import assert_min_pkg_version
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.configuration.specs.mixins import WithObjectStoreRsCredentials

try:
    import deltalake
    from deltalake import write_deltalake, DeltaTable

    deltalake_semver = Version(deltalake.__version__)
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dlt deltalake helpers",
        [f"{version.DLT_PKG_NAME}[deltalake]"],
        "Install `deltalake` so dlt can create Delta tables in the `filesystem` destination.",
    )


def ensure_delta_compatible_arrow_schema(
    schema: pa.Schema,
    partition_by: Optional[Union[List[str], str]] = None,
) -> pa.Schema:
    """Returns Arrow schema compatible with Delta table format.

    Casts schema to replace data types not supported by Delta.
    """
    ARROW_TO_DELTA_COMPATIBLE_ARROW_TYPE_MAP = {
        # maps type check function to type factory function
        pa.types.is_null: pa.string(),
        pa.types.is_time: pa.string(),
        pa.types.is_decimal256: pa.string(),  # pyarrow does not allow downcasting to decimal128
    }

    # partition fields can't be dictionary: https://github.com/delta-io/delta-rs/issues/2969
    if partition_by is not None:
        if isinstance(partition_by, str):
            partition_by = [partition_by]
        if any(pa.types.is_dictionary(schema.field(col).type) for col in partition_by):
            # cast all dictionary fields to string — this is rogue because
            # 1. dictionary value type is disregarded
            # 2. any non-partition dictionary fields are cast too
            ARROW_TO_DELTA_COMPATIBLE_ARROW_TYPE_MAP[pa.types.is_dictionary] = (
                lambda t_: t_.value_type
            )

    # NOTE: also consider calling _convert_pa_schema_to_delta() from delta.schema which casts unsigned types
    return cast_arrow_schema_types(schema, ARROW_TO_DELTA_COMPATIBLE_ARROW_TYPE_MAP)


def ensure_delta_compatible_arrow_data(
    data: Union[pa.Table, pa.RecordBatchReader],
    partition_by: Optional[Union[List[str], str]] = None,
) -> Union[pa.Table, pa.RecordBatchReader]:
    """Returns Arrow data compatible with Delta table format.

    Casts `data` schema to replace data types not supported by Delta.
    """
    # RecordBatchReader.cast() requires pyarrow>=17.0.0
    # cast() got introduced in 16.0.0, but with bug
    assert_min_pkg_version(
        pkg_name="pyarrow",
        version="17.0.0",
        msg="`pyarrow>=17.0.0` is needed for `delta` table format on `filesystem` destination.",
    )
    schema = ensure_delta_compatible_arrow_schema(data.schema, partition_by)
    return data.cast(schema)


def get_delta_write_mode(write_disposition: TWriteDisposition) -> str:
    """Translates dlt write disposition to Delta write mode."""
    if write_disposition in ("append", "merge"):  # `merge` disposition resolves to `append`
        return "append"
    elif write_disposition == "replace":
        return "overwrite"
    else:
        raise ValueErrorWithKnownValues(
            "write_disposition", write_disposition, ["append", "replace", "merge"]
        )


def write_delta_table(
    table_or_uri: Union[str, Path, DeltaTable],
    data: Union[pa.Table, pa.RecordBatchReader],
    write_disposition: TWriteDisposition,
    partition_by: Optional[Union[List[str], str]] = None,
    storage_options: Optional[Dict[str, str]] = None,
    configuration: Optional[Mapping[str, Optional[str]]] = None,
) -> None:
    """Writes in-memory Arrow data to on-disk Delta table.

    Thin wrapper around `deltalake.write_deltalake`.
    """
    write_deltalake(  # type: ignore[call-overload]
        table_or_uri=table_or_uri,
        data=ensure_delta_compatible_arrow_data(data, partition_by),
        partition_by=partition_by,
        mode=get_delta_write_mode(write_disposition),
        schema_mode="merge",  # enable schema evolution (adding new columns)
        storage_options=storage_options,
        configuration=configuration,
    )


def merge_delta_table(
    table: DeltaTable,
    data: Union[pa.Table, pa.RecordBatchReader],
    schema: TTableSchema,
    load_table_name: str,
    streamed_exec: bool,
) -> None:
    """Merges in-memory Arrow data into on-disk Delta table."""

    strategy = schema["x-merge-strategy"]  # type: ignore[typeddict-item]
    if strategy != "insert-only" and strategy not in UPSERT_MERGE_STRATEGIES:
        # the filesystem destination rejects unsupported strategies when it verifies the schema
        raise ValueError(
            f'Merge strategy "{strategy}" is not supported for Delta tables. '
            f'Table: "{load_table_name}".'
        )
    evolve_delta_table_schema(table, data.schema)

    if "parent" in schema:
        unique_column = get_first_column_name_with_prop(schema, "unique")
        predicate = f"target.{unique_column} = source.{unique_column}"
    else:
        primary_keys = get_columns_names_with_prop(schema, "primary_key")
        predicate = " AND ".join([f"target.{c} = source.{c}" for c in primary_keys])

    source_predicate, delete_predicate = _delta_merge_predicates(schema)
    if strategy != "insert-only" and source_predicate:
        # discarded records match nothing, so their stored copies are absent from the merge source
        predicate = _and_predicates(predicate, source_predicate)

    partition_by = get_columns_names_with_prop(schema, "partition")
    qry = table.merge(
        source=ensure_delta_compatible_arrow_data(data, partition_by),
        predicate=predicate,
        source_alias="source",
        target_alias="target",
        streamed_exec=streamed_exec,
    )
    if strategy == "insert-only":
        qry = qry.when_not_matched_insert_all()
    else:
        insert_predicate = source_predicate
        if hard_delete_col := get_first_column_name_with_prop(schema, "hard_delete"):
            deleted_cond, not_deleted_cond = _delta_hard_delete_conds(schema, hard_delete_col)
            # the first matching clause wins, so the delete clause comes before the update clause
            qry = qry.when_matched_delete(predicate=deleted_cond)
            insert_predicate = _and_predicates(not_deleted_cond, source_predicate)
        changed_cond = None
        if schema.get("x-merge-skip-unchanged-rows"):
            changed_cond = get_merge_changed_cond(schema, "source", "target")
        qry = qry.when_matched_update_all(predicate=changed_cond)
        qry = qry.when_not_matched_insert_all(predicate=insert_predicate)
        if strategy == "cdc":
            qry = qry.when_not_matched_by_source_delete(predicate=delete_predicate)
    qry.execute()


def _delta_hard_delete_conds(schema: TTableSchema, column_name: str) -> Tuple[str, str]:
    """Returns a predicate that selects source rows marked as deleted and one for the other rows."""
    column = f"source.{column_name}"
    if schema["columns"][column_name].get("data_type") == "bool":
        return f"{column} = true", f"({column} IS NULL OR {column} = false)"
    return f"{column} IS NOT NULL", f"{column} IS NULL"


def _and_predicates(*predicates: Optional[str]) -> Optional[str]:
    present = [p for p in predicates if p]
    if not present:
        return None
    return " AND ".join(f"({p})" for p in present)


def _delta_merge_predicates(schema: TTableSchema) -> Tuple[Optional[str], Optional[str]]:
    """Returns `source_filter` and `destination_scope` as Delta merge predicates."""
    source_filter = cast(Optional[str], schema.get("x-merge-source-filter"))
    destination_scope = cast(Optional[str], schema.get("x-merge-destination-scope"))
    # `{staging_table}` is the `source` alias and `{table}` the `target` alias. schema verification
    # rejects any other placeholder before the load
    return (
        source_filter.format(staging_table="source", table="target") if source_filter else None,
        destination_scope.format(table="target") if destination_scope else None,
    )


def truncate_delta_table(table: DeltaTable) -> None:
    """Deletes all rows in a single transactional commit, preserving schema and version history."""
    table.delete()


def get_delta_tables(
    pipeline: Pipeline, *tables: str, schema_name: str = None, include_dlt_tables: bool = False
) -> Dict[str, DeltaTable]:
    """Returns Delta tables in `pipeline.default_schema (default)` or `schema_name` as `deltalake.DeltaTable` objects.

    Returned object is a dictionary with table names as keys and `DeltaTable` objects as values.
    Optionally filters dictionary by table names specified as `*tables*`.
    Raises ValueError if table name specified as `*tables` is not found. You may try to switch to other
    schemas via `schema_name` argument.
    """
    return load_open_tables(
        pipeline, "delta", *tables, schema_name=schema_name, include_dlt_tables=include_dlt_tables
    )


def deltalake_storage_options(
    credentials: Optional[CredentialsConfiguration] = None,
    storage_options: Optional[DictStrAny] = None,
) -> Dict[str, str]:
    """Returns dict that can be passed as `storage_options` in `deltalake` library."""
    creds = {}
    extra_options = {}
    if credentials is not None and isinstance(credentials, WithObjectStoreRsCredentials):
        creds = credentials.to_object_store_rs_credentials()
    if storage_options is not None:
        extra_options = storage_options
    shared_keys = creds.keys() & extra_options.keys()
    if len(shared_keys) > 0:
        logger.warning(
            "The `deltalake_storage_options` configuration dictionary contains "
            "keys also provided by dlt's credential system: "
            + ", ".join([f"`{key}`" for key in shared_keys])
            + ". dlt will use the values in `deltalake_storage_options`."
        )
    return {**creds, **extra_options}


def evolve_delta_table_schema(delta_table: DeltaTable, arrow_schema: pa.Schema) -> DeltaTable:
    """Evolves `delta_table` schema if different from `arrow_schema`.

    We compare fields via names. Actual types and nullability are ignored. This is
    how schemas are evolved for other destinations. Existing columns are never modified.
    Variant columns are created.

    Adds column(s) to `delta_table` present in `arrow_schema` but not in `delta_table`.
    """

    if deltalake_semver.major == 0:
        new_fields = [
            deltalake.Field.from_pyarrow(field)  # type: ignore[attr-defined]
            for field in ensure_delta_compatible_arrow_schema(arrow_schema)
            if field.name not in delta_table.schema().to_pyarrow().names  # type: ignore[attr-defined]
        ]
    else:
        # deltalake 1.x changed pyarrow to arrow
        new_fields = [
            deltalake.Field.from_arrow(field)
            for field in ensure_delta_compatible_arrow_schema(arrow_schema)
            if field.name not in delta_table.schema().to_arrow().names
        ]
    if new_fields:
        delta_table.alter.add_columns(new_fields)
    return delta_table
