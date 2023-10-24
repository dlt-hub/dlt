import os
from typing import List, Dict, Tuple, Protocol, Any
from pathlib import Path

from dlt.common import json, logger
from dlt.common.json import custom_pua_decode
from dlt.common.runtime import signals
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.storages import NormalizeStorage, LoadStorage, NormalizeStorageConfiguration, FileStorage
from dlt.common.typing import TDataItem
from dlt.common.schema import TSchemaUpdate, Schema
from dlt.common.utils import TRowCount, merge_row_count, increase_row_count
from dlt.normalize.configuration import NormalizeConfiguration
from dlt.common.exceptions import MissingDependencyException
from dlt.common.normalizers.utils import generate_dlt_ids

try:
    from dlt.common.libs import pyarrow
    from dlt.common.libs.pyarrow import pyarrow as pa
except MissingDependencyException:
    pyarrow = None
    pa = None


class ItemsNormalizer(Protocol):
    def __call__(
        self,
        extracted_items_file: str,
        load_storage: LoadStorage,
        normalize_storage: NormalizeStorage,
        schema: Schema,
        load_id: str,
        root_table_name: str,
        config: NormalizeConfiguration
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
        config: NormalizeConfiguration
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
    RECORD_BATCH_SIZE = 1000

    def _write_with_dlt_columns(
        self, extracted_items_file: str,
        normalize_storage: NormalizeStorage,
        load_storage: LoadStorage, load_id: str, schema: Schema, root_table_name: str,
        add_load_id: bool, add_dlt_id: bool
    ) -> int:
        new_columns: List[Any] = []
        if add_load_id:
            schema.update_table({"name": root_table_name, "columns": {"_dlt_load_id": {"name": "_dlt_load_id", "data_type": "text", "nullable": False}}})
            load_id_type = pa.dictionary(pa.int8(), pa.string())
            new_columns.append((
                pa.field("_dlt_load_id", load_id_type, nullable=False),
                lambda batch: pa.array([load_id] * batch.num_rows, type=load_id_type)
            ))
        if add_dlt_id:
            schema.update_table({"name": root_table_name, "columns": {"_dlt_id": {"name": "_dlt_id", "data_type": "text", "nullable": False}}})
            new_columns.append((
                pa.field("_dlt_id", pyarrow.pyarrow.string(), nullable=False),
                lambda batch: generate_dlt_ids(batch.num_rows)
            ))
        items_count = 0
        as_py = load_storage.loader_file_format != "arrow"
        with normalize_storage.storage.open_file(extracted_items_file, "rb") as f:
            for batch in pyarrow.pq_stream_with_new_columns(f, new_columns, batch_size=self.RECORD_BATCH_SIZE):
                items_count += batch.num_rows
                if as_py:
                    # Write python rows to jsonl, insert-values, etc... storage
                    load_storage.write_data_item(
                        load_id, schema.name, root_table_name, batch.to_pylist(), schema.get_table_columns(root_table_name)
                    )
                else:
                    load_storage.write_data_item(
                        load_id, schema.name, root_table_name, batch, schema.get_table_columns(root_table_name)
                    )
        # TODO: Return schema update
        return items_count

    def __call__(
        self,
        extracted_items_file: str,
        load_storage: LoadStorage,
        normalize_storage: NormalizeStorage,
        schema: Schema,
        load_id: str,
        root_table_name: str,
        config: NormalizeConfiguration
    ) -> Tuple[List[TSchemaUpdate], int, TRowCount]:
        import pyarrow as pa

        add_dlt_id = config.parquet_normalizer_config.add_dlt_id
        add_dlt_load_id = config.parquet_normalizer_config.add_dlt_load_id

        if add_dlt_id or add_dlt_load_id or load_storage.loader_file_format != "arrow":
            items_count = self._write_with_dlt_columns(
                extracted_items_file,
                normalize_storage,
                load_storage,
                load_id,
                schema,
                root_table_name,
                add_dlt_load_id,
                add_dlt_id
            )
            return [], items_count, {root_table_name: items_count}

        from dlt.common.libs.pyarrow import get_row_count
        with normalize_storage.storage.open_file(extracted_items_file, "rb") as f:
            items_count = get_row_count(f)
        target_folder = load_storage.storage.make_full_path(os.path.join(load_id, LoadStorage.NEW_JOBS_FOLDER))
        parts = NormalizeStorage.parse_normalize_file_name(extracted_items_file)
        new_file_name = load_storage.build_job_file_name(parts.table_name, parts.file_id, with_extension=True)
        FileStorage.link_hard_with_fallback(
            normalize_storage.storage.make_full_path(extracted_items_file),
            os.path.join(target_folder, new_file_name)
        )
        return [], items_count, {root_table_name: items_count}
