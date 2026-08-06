from typing import Sequence, Union

from dlt.common import logger
from dlt.common.data_writers.escape import escape_datafusion_literal
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.libs.pyarrow import pyarrow as pa
from dlt.common.schema import TSchemaTables, TTableSchema
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    get_first_column_name_with_prop,
    get_inherited_table_hint,
    is_nested_table,
)
from dlt.destinations.impl.lance.exceptions import LanceEmbeddingsConfigurationMissing
from dlt.destinations.impl.lance.lance_adapter import (
    DEFAULT_REMOVE_ORPHANS,
    REMOVE_ORPHANS_HINT,
    VECTORIZE_HINT,
)
from dlt.destinations.sql_jobs import SqlMergeFollowupJob


def get_canonical_vector_database_doc_id_merge_key(
    load_table: TTableSchema,
) -> str:
    if merge_key := get_first_column_name_with_prop(load_table, "merge_key"):
        return merge_key
    elif primary_key := get_columns_names_with_prop(load_table, "primary_key"):
        # No merge key defined, warn and assume the first element of the primary key is `doc_id`.
        logger.warning(
            "Merge strategy selected without defined merge key - using the first element of the"
            f" primary key ({primary_key}) as merge key."
        )
        return primary_key[0]
    else:
        raise DestinationTerminalException(
            "You must specify at least a primary key in order to perform orphan removal."
        )


def _cast_to_target_types(
    source: pa.RecordBatchReader, target_schema: pa.Schema
) -> pa.RecordBatchReader:
    """Casts source columns whose types differ from `target_schema`. Columns missing from
    `target_schema` or `source` are left untouched — `ds.insert()` handles those natively.
    """
    target_types = {field.name: field.type for field in target_schema}
    cols_to_cast = {
        field.name: target_types[field.name]
        for field in source.schema
        if field.name in target_types and field.type != target_types[field.name]
    }
    if not cols_to_cast:
        return source

    cast_schema = pa.schema(
        [
            pa.field(f.name, cols_to_cast[f.name], f.nullable) if f.name in cols_to_cast else f
            for f in source.schema
        ]
    )
    return pa.RecordBatchReader.from_batches(cast_schema, (b.cast(cast_schema) for b in source))


def create_in_filter(field_name: str, array: Union[pa.Array, pa.ChunkedArray]) -> str:
    """Filters all rows where `field_name` is one of the distinct values in the `array`."""
    # a key column repeats its value per row and the filter is bounded by `max_query_length`
    values = array.unique()
    if pa.types.is_dictionary(array.type):
        # a chunked array carries one dictionary per chunk, `unique` unifies them first
        values = values.dictionary
    return f"{field_name} IN ({', '.join(map(escape_datafusion_literal, values.to_pylist()))})"


def set_remove_orphans_hint(
    table: PreparedTableSchema, schema_tables: TSchemaTables
) -> PreparedTableSchema:
    """Resolves `REMOVE_ORPHANS_HINT` on `table`, inheriting it from the parent table chain."""
    if REMOVE_ORPHANS_HINT not in table:
        inherited_hint = get_inherited_table_hint(
            schema_tables, table["name"], REMOVE_ORPHANS_HINT, allow_none=True
        )
        table[REMOVE_ORPHANS_HINT] = (  # type: ignore[literal-required]
            DEFAULT_REMOVE_ORPHANS if inherited_hint is None else inherited_hint
        )
    return table


def verify_lance_tables(
    loaded_tables: Sequence[PreparedTableSchema],
    has_embeddings: bool,
    destination_name: str = "lance",
) -> None:
    """Raises if a table cannot be loaded into a lance format destination."""
    for load_table in loaded_tables:
        # nested tables inherit the behavior of their parent
        if is_nested_table(load_table):
            continue

        merge_keys = get_columns_names_with_prop(load_table, "merge_key")
        if load_table.get(REMOVE_ORPHANS_HINT, DEFAULT_REMOVE_ORPHANS) and len(merge_keys) > 1:
            raise DestinationTerminalException(
                f"Multiple merge keys are not supported when {destination_name} orphan removal is"
                f" enabled: {merge_keys}"
            )

        if not has_embeddings:
            if embed_columns := get_columns_names_with_prop(load_table, VECTORIZE_HINT):
                raise LanceEmbeddingsConfigurationMissing(
                    load_table["name"], embed_columns, destination_name
                )


def get_orphan_scope_key_col(load_table: PreparedTableSchema, dataset_name: str) -> str:
    """Returns the column identifying the documents a load owns in `load_table`: the root key for a
    nested table, the canonical doc id merge key for a root table."""
    if is_nested_table(load_table):
        return SqlMergeFollowupJob.get_root_key_col(
            [load_table], load_table, dataset_name, dataset_name
        )
    return get_canonical_vector_database_doc_id_merge_key(load_table)


def build_orphan_scope_filter(
    load_table: PreparedTableSchema, file_paths: Sequence[str], dataset_name: str
) -> str:
    """Builds a SQL filter scoping orphan deletion to the documents present in `file_paths`.

    `file_paths` must be all job files of `load_table`: a subset scopes the delete to itself and
    removes what the remaining files wrote.
    """
    key_col = get_orphan_scope_key_col(load_table, dataset_name)
    # only the key column is materialized, the payload stays on disk
    keys = pa.concat_tables(pa.parquet.read_table(path, columns=[key_col]) for path in file_paths)[
        key_col
    ]
    return create_in_filter(key_col, keys)
