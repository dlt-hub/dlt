from typing import Any, Optional, Union, Dict, List

import pyarrow as pa
from pyarrow import ArrowInvalid
from pyarrow import types as pat

import lance
from lance.namespace import LanceNamespace
from lancedb.table import _append_vector_columns

from dlt.common import logger
from dlt.common.data_writers.escape import escape_lancedb_literal
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.schema import TTableSchema
from dlt.common.schema.typing import TWriteDisposition
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


def create_in_filter(field_name: str, array: pa.Array) -> str:
    """Filters all rows where `field_name` is one of the values in the `array`

    If `array` is dictionary-encoded (pa.DictionaryType) we emit the
     *distinct* values stored in its dictionary.
    """
    if pat.is_dictionary(array.type):
        # use the dictionary payload (unique categorical values).
        values_py = array.dictionary.to_pylist()
    else:
        values_py = array.to_pylist()
    return f"{field_name} IN ({', '.join(map(escape_lancedb_literal, values_py))})"


def write_records(
    records: Union[pa.RecordBatchReader, List[Dict[str, Any]]],
    /,
    *,
    namespace: LanceNamespace,
    table_id: List[str],
    write_disposition: Optional[TWriteDisposition] = "append",
    merge_key: Optional[str] = None,
    when_not_matched_by_source_delete_expr: Optional[str] = None,
) -> None:
    """Inserts records into lLance table with automatic embedding computation.

    Args:
        records: The data to be inserted as payload.
        namespace: Lance namespace containing the table.
        table_id: Hierarchical table identifier, e.g. [dataset_name, table_name].
        merge_key: Keys for update/merge operations.
        write_disposition: The write disposition - one of 'skip', 'append', 'replace', 'merge'.
        when_not_matched_by_source_delete_expr: Optional SQL filter applied to
            `when_not_matched_by_source_delete` during a merge.
    """
    ds = lance.dataset(namespace=namespace, table_id=table_id)

    if isinstance(records, pa.RecordBatchReader):
        records = _append_vector_columns(records, schema=ds.schema)
        records = _align_schema(records, ds.schema)

    try:
        if write_disposition in ("append", "skip", "replace"):
            ds.insert(records)
        elif write_disposition == "merge":
            merge_builder = (
                ds.merge_insert(merge_key).when_matched_update_all().when_not_matched_insert_all()
            )
            if when_not_matched_by_source_delete_expr:
                merge_builder = merge_builder.when_not_matched_by_source_delete(
                    when_not_matched_by_source_delete_expr
                )
            merge_builder.execute(records)
        else:
            raise DestinationTerminalException(
                f"Unsupported `{write_disposition=:}` for Lance Destination - batch"
                " failed AND WILL **NOT** BE RETRIED."
            )
    except ArrowInvalid as e:
        raise DestinationTerminalException(
            "Python and Arrow datatype mismatch - batch failed AND WILL **NOT** BE RETRIED."
        ) from e


def _align_schema(source: pa.RecordBatchReader, target_schema: pa.Schema) -> pa.RecordBatchReader:
    """Aligns schema of `source` to match `target_schema`.

    No-op if schemas are identical. Else, reorders columns and casts `source` to match `target_schema`.

    Assumes all columns in `target_schema` are present in `source`.
    """
    if source.schema != target_schema:
        return pa.RecordBatchReader.from_batches(
            target_schema, (b.select(target_schema.names).cast(target_schema) for b in source)
        )
    return source
