from dlt.common import logger
from dlt.common.data_writers.escape import escape_lancedb_literal
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.libs.pyarrow import pyarrow as pa
from dlt.common.schema import TTableSchema
from dlt.common.schema.utils import get_columns_names_with_prop, get_first_column_name_with_prop


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


def create_in_filter(field_name: str, array: pa.Array) -> str:
    """Filters all rows where `field_name` is one of the values in the `array`

    If `array` is dictionary-encoded (pa.DictionaryType) we emit the
     *distinct* values stored in its dictionary.
    """
    if pa.types.is_dictionary(array.type):
        # use the dictionary payload (unique categorical values).
        values_py = array.dictionary.to_pylist()
    else:
        values_py = array.to_pylist()
    return f"{field_name} IN ({', '.join(map(escape_lancedb_literal, values_py))})"
