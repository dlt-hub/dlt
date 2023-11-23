import os
from typing import List, Dict, Set, Tuple, Any
from abc import abstractmethod

from dlt.common import json, logger
from dlt.common.json import custom_pua_decode, may_have_pua
from dlt.common.runtime import signals
from dlt.common.schema.typing import TSchemaEvolutionMode, TTableSchemaColumns, TSchemaContractDict
from dlt.common.storages import NormalizeStorage, LoadStorage, FileStorage
from dlt.common.typing import DictStrAny, TDataItem
from dlt.common.schema import TSchemaUpdate, Schema
from dlt.common.utils import TRowCount, merge_row_count, increase_row_count
from dlt.common.exceptions import MissingDependencyException
from dlt.common.normalizers.utils import generate_dlt_ids

from dlt.normalize.configuration import NormalizeConfiguration

try:
    from dlt.common.libs import pyarrow
    from dlt.common.libs.pyarrow import pyarrow as pa
except MissingDependencyException:
    pyarrow = None
    pa = None


class ItemsNormalizer:
    def __init__(
        self,
        load_storage: LoadStorage,
        normalize_storage: NormalizeStorage,
        schema: Schema,
        load_id: str,
        config: NormalizeConfiguration,
    ) -> None:
        self.load_storage = load_storage
        self.normalize_storage = normalize_storage
        self.schema = schema
        self.load_id = load_id
        self.config = config

    @abstractmethod
    def __call__(
        self, extracted_items_file: str, root_table_name: str
    ) -> Tuple[List[TSchemaUpdate], int, TRowCount]: ...


class JsonLItemsNormalizer(ItemsNormalizer):
    def __init__(
        self,
        load_storage: LoadStorage,
        normalize_storage: NormalizeStorage,
        schema: Schema,
        load_id: str,
        config: NormalizeConfiguration,
    ) -> None:
        super().__init__(load_storage, normalize_storage, schema, load_id, config)
        self._table_contracts: Dict[str, TSchemaContractDict] = {}
        self._filtered_tables: Set[str] = set()
        self._filtered_tables_columns: Dict[str, Dict[str, TSchemaEvolutionMode]] = {}
        # quick access to column schema for writers below
        self._column_schemas: Dict[str, TTableSchemaColumns] = {}

    def _filter_columns(
        self, filtered_columns: Dict[str, TSchemaEvolutionMode], row: DictStrAny
    ) -> DictStrAny:
        for name, mode in filtered_columns.items():
            if name in row:
                if mode == "discard_row":
                    return None
                elif mode == "discard_value":
                    row.pop(name)
        return row

    def _normalize_chunk(
        self, root_table_name: str, items: List[TDataItem], may_have_pua: bool
    ) -> Tuple[TSchemaUpdate, int, TRowCount]:
        column_schemas = self._column_schemas
        schema_update: TSchemaUpdate = {}
        schema = self.schema
        schema_name = schema.name
        items_count = 0
        row_counts: TRowCount = {}
        normalize_data_fun = self.schema.normalize_data_item

        for item in items:
            items_gen = normalize_data_fun(item, self.load_id, root_table_name)
            try:
                should_descend: bool = None
                # use send to prevent descending into child rows when row was discarded
                while row_info := items_gen.send(should_descend):
                    should_descend = True
                    (table_name, parent_table), row = row_info

                    # rows belonging to filtered out tables are skipped
                    if table_name in self._filtered_tables:
                        # stop descending into further rows
                        should_descend = False
                        continue

                    # filter row, may eliminate some or all fields
                    row = schema.filter_row(table_name, row)
                    # do not process empty rows
                    if not row:
                        should_descend = False
                        continue

                    # filter columns or full rows if schema contract said so
                    # do it before schema inference in `coerce_row` to not trigger costly migration code
                    filtered_columns = self._filtered_tables_columns.get(table_name, None)
                    if filtered_columns:
                        row = self._filter_columns(filtered_columns, row)  # type: ignore[arg-type]
                        # if whole row got dropped
                        if not row:
                            should_descend = False
                            continue

                    # decode pua types
                    if may_have_pua:
                        for k, v in row.items():
                            row[k] = custom_pua_decode(v)  # type: ignore

                    # coerce row of values into schema table, generating partial table with new columns if any
                    row, partial_table = schema.coerce_row(table_name, parent_table, row)

                    # if we detect a migration, check schema contract
                    if partial_table:
                        schema_contract = self._table_contracts.setdefault(
                            table_name,
                            schema.resolve_contract_settings_for_table(
                                parent_table or table_name
                            ),  # parent_table, if present, exists in the schema
                        )
                        partial_table, filters = schema.apply_schema_contract(
                            schema_contract, partial_table, data_item=row
                        )
                        if filters:
                            for entity, name, mode in filters:
                                if entity == "tables":
                                    self._filtered_tables.add(name)
                                elif entity == "columns":
                                    filtered_columns = self._filtered_tables_columns.setdefault(
                                        table_name, {}
                                    )
                                    filtered_columns[name] = mode

                        if partial_table is None:
                            # discard migration and row
                            should_descend = False
                            continue
                        # theres a new table or new columns in existing table
                        # update schema and save the change
                        schema.update_table(partial_table)
                        table_updates = schema_update.setdefault(table_name, [])
                        table_updates.append(partial_table)

                        # update our columns
                        column_schemas[table_name] = schema.get_table_columns(table_name)

                        # apply new filters
                        if filtered_columns and filters:
                            row = self._filter_columns(filtered_columns, row)
                            # do not continue if new filters skipped the full row
                            if not row:
                                should_descend = False
                                continue

                    # get current columns schema
                    columns = column_schemas.get(table_name)
                    if not columns:
                        columns = schema.get_table_columns(table_name)
                        column_schemas[table_name] = columns
                    # store row
                    # TODO: store all rows for particular items all together after item is fully completed
                    #   will be useful if we implement bad data sending to a table
                    self.load_storage.write_data_item(
                        self.load_id, schema_name, table_name, row, columns
                    )
                    # count total items
                    # TODO: take counts and bytes from buffered file writers instead of taking those here
                    items_count += 1
                    increase_row_count(row_counts, table_name, 1)
            except StopIteration:
                pass
            signals.raise_if_signalled()
        return schema_update, items_count, row_counts

    def __call__(
        self,
        extracted_items_file: str,
        root_table_name: str,
    ) -> Tuple[List[TSchemaUpdate], int, TRowCount]:
        schema_updates: List[TSchemaUpdate] = []
        row_counts: TRowCount = {}
        with self.normalize_storage.storage.open_file(extracted_items_file, "rb") as f:
            # enumerate jsonl file line by line
            items_count = 0
            line: bytes
            for line_no, line in enumerate(f):
                items: List[TDataItem] = json.loadb(line)
                partial_update, items_count, r_counts = self._normalize_chunk(
                    root_table_name, items, may_have_pua(line)
                )
                schema_updates.append(partial_update)
                merge_row_count(row_counts, r_counts)
                logger.debug(
                    f"Processed {line_no} items from file {extracted_items_file}, items"
                    f" {items_count}"
                )

        return schema_updates, items_count, row_counts


class ParquetItemsNormalizer(ItemsNormalizer):
    REWRITE_ROW_GROUPS = 1

    def _write_with_dlt_columns(
        self, extracted_items_file: str, root_table_name: str, add_load_id: bool, add_dlt_id: bool
    ) -> Tuple[List[TSchemaUpdate], int]:
        new_columns: List[Any] = []
        schema = self.schema
        load_id = self.load_id
        schema_update: TSchemaUpdate = {}

        if add_load_id:
            table_update = schema.update_table(
                {
                    "name": root_table_name,
                    "columns": {
                        "_dlt_load_id": {
                            "name": "_dlt_load_id",
                            "data_type": "text",
                            "nullable": False,
                        }
                    },
                }
            )
            table_updates = schema_update.setdefault(root_table_name, [])
            table_updates.append(table_update)
            load_id_type = pa.dictionary(pa.int8(), pa.string())
            new_columns.append(
                (
                    -1,
                    pa.field("_dlt_load_id", load_id_type, nullable=False),
                    lambda batch: pa.array([load_id] * batch.num_rows, type=load_id_type),
                )
            )

        if add_dlt_id:
            table_update = schema.update_table(
                {
                    "name": root_table_name,
                    "columns": {
                        "_dlt_id": {"name": "_dlt_id", "data_type": "text", "nullable": False}
                    },
                }
            )
            table_updates = schema_update.setdefault(root_table_name, [])
            table_updates.append(table_update)
            new_columns.append(
                (
                    -1,
                    pa.field("_dlt_id", pyarrow.pyarrow.string(), nullable=False),
                    lambda batch: pa.array(generate_dlt_ids(batch.num_rows)),
                )
            )

        items_count = 0
        as_py = self.load_storage.loader_file_format != "arrow"
        with self.normalize_storage.storage.open_file(extracted_items_file, "rb") as f:
            for batch in pyarrow.pq_stream_with_new_columns(
                f, new_columns, row_groups_per_read=self.REWRITE_ROW_GROUPS
            ):
                items_count += batch.num_rows
                if as_py:
                    # Write python rows to jsonl, insert-values, etc... storage
                    self.load_storage.write_data_item(
                        load_id,
                        schema.name,
                        root_table_name,
                        batch.to_pylist(),
                        schema.get_table_columns(root_table_name),
                    )
                else:
                    self.load_storage.write_data_item(
                        load_id,
                        schema.name,
                        root_table_name,
                        batch,
                        schema.get_table_columns(root_table_name),
                    )
        return [schema_update], items_count

    def _fix_schema_precisions(self, root_table_name: str) -> List[TSchemaUpdate]:
        """Reduce precision of timestamp columns if needed, according to destination caps"""
        schema = self.schema
        table = schema.tables[root_table_name]
        max_precision = self.config.destination_capabilities.timestamp_precision

        new_cols: TTableSchemaColumns = {}
        for key, column in table["columns"].items():
            if column.get("data_type") in ("timestamp", "time"):
                if (prec := column.get("precision")) and prec > max_precision:
                    new_cols[key] = dict(column, precision=max_precision)  # type: ignore[assignment]

        if not new_cols:
            return []
        return [
            {root_table_name: [schema.update_table({"name": root_table_name, "columns": new_cols})]}
        ]

    def __call__(
        self, extracted_items_file: str, root_table_name: str
    ) -> Tuple[List[TSchemaUpdate], int, TRowCount]:
        base_schema_update = self._fix_schema_precisions(root_table_name)

        add_dlt_id = self.config.parquet_normalizer.add_dlt_id
        add_dlt_load_id = self.config.parquet_normalizer.add_dlt_load_id

        if add_dlt_id or add_dlt_load_id or self.load_storage.loader_file_format != "arrow":
            schema_update, items_count = self._write_with_dlt_columns(
                extracted_items_file, root_table_name, add_dlt_load_id, add_dlt_id
            )
            return base_schema_update + schema_update, items_count, {root_table_name: items_count}

        from dlt.common.libs.pyarrow import get_row_count

        with self.normalize_storage.storage.open_file(extracted_items_file, "rb") as f:
            items_count = get_row_count(f)
        target_folder = self.load_storage.storage.make_full_path(
            os.path.join(self.load_id, LoadStorage.NEW_JOBS_FOLDER)
        )
        parts = NormalizeStorage.parse_normalize_file_name(extracted_items_file)
        new_file_name = self.load_storage.build_job_file_name(
            parts.table_name, parts.file_id, with_extension=True
        )
        FileStorage.link_hard_with_fallback(
            self.normalize_storage.storage.make_full_path(extracted_items_file),
            os.path.join(target_folder, new_file_name),
        )
        return base_schema_update, items_count, {root_table_name: items_count}
