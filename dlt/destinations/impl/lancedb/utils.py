import os
from typing import Optional, Union, Dict, List

import pyarrow as pa
import pyarrow as pa
from pyarrow import ArrowInvalid
from pyarrow import types as pat
from lancedb import DBConnection
from lancedb.common import DATA

from dlt.common import logger
from dlt.common.data_writers.escape import escape_lancedb_literal
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.schema import TTableSchema
from dlt.common.schema.typing import TWriteDisposition
from dlt.common.schema.utils import get_columns_names_with_prop, get_first_column_name_with_prop
from dlt.destinations.impl.lancedb.configuration import TEmbeddingProvider
from dlt.destinations.impl.lancedb.schema import add_vector_column

PROVIDER_ENVIRONMENT_VARIABLES_MAP: Dict[TEmbeddingProvider, str] = {
    "cohere": "COHERE_API_KEY",
    "gemini-text": "GOOGLE_API_KEY",
    "openai": "OPENAI_API_KEY",
    "huggingface": "HUGGINGFACE_API_KEY",
}


def set_non_standard_providers_environment_variables(
    embedding_model_provider: TEmbeddingProvider, api_key: Union[str, None]
) -> None:
    if embedding_model_provider in PROVIDER_ENVIRONMENT_VARIABLES_MAP:
        os.environ[PROVIDER_ENVIRONMENT_VARIABLES_MAP[embedding_model_provider]] = api_key or ""


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
    records: DATA,
    /,
    *,
    db_client: DBConnection,
    table_name: str,
    vector_field_name: str,
    write_disposition: Optional[TWriteDisposition] = "append",
    merge_key: Optional[str] = None,
    remove_orphans: Optional[bool] = False,
    delete_condition: Optional[str] = None,
) -> None:
    """Inserts records into a LanceDB table with automatic embedding computation.

    Args:
        records: The data to be inserted as payload.
        db_client: The LanceDB client connection.
        table_name: The name of the table to insert into.
        merge_key: Keys for update/merge operations.
        write_disposition: The write disposition - one of 'skip', 'append', 'replace', 'merge'.
        remove_orphans (bool): Whether to remove orphans after insertion or not (only merge disposition).
        filter_condition (str): If None, then all such rows will be deleted.
            Otherwise, the condition will be used as an SQL filter to limit what rows are deleted.

    Raises:
        ValueError: If the write disposition is unsupported, or `id_field_name` is not
            provided for update/merge operations.
    """
    tbl = db_client.open_table(table_name)
    tbl.checkout_latest()
    try:
        if write_disposition in ("append", "skip", "replace"):
            tbl.add(records)
        elif write_disposition == "merge":
            # LanceDB requires identical schemas for when_not_matched_by_source_delete
            # The source data schema must exactly match the target table schema (column names,
            # order, and types). Only after 22. does it work with chunks and embeddings
            target_schema = tbl.schema
            records = add_vector_column(records, target_schema, vector_field_name)
            if remove_orphans:
                tbl.merge_insert(merge_key).when_not_matched_by_source_delete(
                    delete_condition
                ).execute(records)
            else:
                tbl.merge_insert(
                    merge_key
                ).when_matched_update_all().when_not_matched_insert_all().execute(records)
        else:
            raise DestinationTerminalException(
                f"Unsupported `{write_disposition=:}` for LanceDB Destination - batch"
                " failed AND WILL **NOT** BE RETRIED."
            )
    except ArrowInvalid as e:
        raise DestinationTerminalException(
            "Python and Arrow datatype mismatch - batch failed AND WILL **NOT** BE RETRIED."
        ) from e
