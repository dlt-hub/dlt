import os
from typing import List, Dict, Tuple, Protocol

from dlt.common import json, logger
from dlt.common.json import custom_pua_decode
from dlt.common.runtime import signals
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.storages import NormalizeStorage, LoadStorage, NormalizeStorageConfiguration
from dlt.common.typing import TDataItem
from dlt.common.schema import TSchemaUpdate, Schema
from dlt.common.utils import TRowCount, merge_row_count, increase_row_count


class ItemsNormalizer(Protocol):
    def __call__(
        self,
        extracted_items_file: str,
        load_storage: LoadStorage,
        normalize_storage: NormalizeStorage,
        schema: Schema,
        load_id: str,
        root_table_name: str,
    ) -> Tuple[List[TSchemaUpdate], int, TRowCount]:
        ...


class JsonLItemsNormalizer(ItemsNormalizer):
    def _normalize_chunk(
        self,
        load_storage: LoadStorage,
        schema: Schema,
        load_id: str,
        root_table_name: str,
        items: List[TDataItem],
    ) -> Tuple[TSchemaUpdate, int, TRowCount]:
        column_schemas: Dict[
            str, TTableSchemaColumns
        ] = {}  # quick access to column schema for writers below
        schema_update: TSchemaUpdate = {}
        schema_name = schema.name
        items_count = 0
        row_counts: TRowCount = {}

        for item in items:
            for (table_name, parent_table), row in schema.normalize_data_item(
                item, load_id, root_table_name
            ):
                # filter row, may eliminate some or all fields
                row = schema.filter_row(table_name, row)
                # do not process empty rows
                if row:
                    # decode pua types
                    for k, v in row.items():
                        row[k] = custom_pua_decode(v)  # type: ignore
                    # coerce row of values into schema table, generating partial table with new columns if any
                    row, partial_table = schema.coerce_row(
                        table_name, parent_table, row
                    )
                    # theres a new table or new columns in existing table
                    if partial_table:
                        # update schema and save the change
                        schema.update_table(partial_table)
                        table_updates = schema_update.setdefault(table_name, [])
                        table_updates.append(partial_table)
                        # update our columns
                        column_schemas[table_name] = schema.get_table_columns(
                            table_name
                        )
                    # get current columns schema
                    columns = column_schemas.get(table_name)
                    if not columns:
                        columns = schema.get_table_columns(table_name)
                        column_schemas[table_name] = columns
                    # store row
                    # TODO: it is possible to write to single file from many processes using this: https://gitlab.com/warsaw/flufl.lock
                    load_storage.write_data_item(
                        load_id, schema_name, table_name, row, columns
                    )
                    # count total items
                    items_count += 1
                    increase_row_count(row_counts, table_name, 1)
            signals.raise_if_signalled()
        return schema_update, items_count, row_counts

    def __call__(
        self,
        extracted_items_file: str,
        load_storage: LoadStorage,
        normalize_storage: NormalizeStorage,
        schema: Schema,
        load_id: str,
        root_table_name: str,
    ) -> Tuple[List[TSchemaUpdate], int, TRowCount]:
        schema_updates: List[TSchemaUpdate] = []
        row_counts: TRowCount = {}
        with normalize_storage.storage.open_file(extracted_items_file) as f:
            # enumerate jsonl file line by line
            items_count = 0
            for line_no, line in enumerate(f):
                items: List[TDataItem] = json.loads(line)
                partial_update, items_count, r_counts = self._normalize_chunk(
                    load_storage, schema, load_id, root_table_name, items
                )
                schema_updates.append(partial_update)
                merge_row_count(row_counts, r_counts)
                logger.debug(
                    f"Processed {line_no} items from file {extracted_items_file}, items {items_count}"
                )

        return schema_updates, items_count, row_counts


class ParquetItemsNormalizer(ItemsNormalizer):
    def __call__(
        self,
        extracted_items_file: str,
        load_storage: LoadStorage,
        normalize_storage: NormalizeStorage,
        schema: Schema,
        load_id: str,
        root_table_name: str,
    ) -> Tuple[List[TSchemaUpdate], int, TRowCount]:
        from dlt.common.libs import pyarrow
        items_count = pyarrow.get_row_count(extracted_items_file)
        target_folder = load_storage.storage.make_full_path(os.path.join(load_id, LoadStorage.NEW_JOBS_FOLDER))
        load_storage.storage.atomic_import(normalize_storage.storage.make_full_path(extracted_items_file), target_folder)
        return [], items_count, {root_table_name: items_count}
