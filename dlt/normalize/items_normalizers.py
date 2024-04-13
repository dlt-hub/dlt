from typing import List, Dict, Set, Any
from abc import abstractmethod

from dlt.common import logger
from dlt.common.json import json
from dlt.common.data_writers import DataWriterMetrics
from dlt.common.data_writers.writers import ArrowToObjectAdapter
from dlt.common.json import custom_pua_decode, may_have_pua
from dlt.common.runtime import signals
from dlt.common.schema.typing import TSchemaEvolutionMode, TTableSchemaColumns, TSchemaContractDict
from dlt.common.schema.utils import has_table_seen_data
from dlt.common.storages import (
    NormalizeStorage,
    LoadStorage,
)
from dlt.common.storages.data_item_storage import DataItemStorage
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.common.typing import DictStrAny, TDataItem
from dlt.common.schema import TSchemaUpdate, Schema
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
        item_storage: DataItemStorage,
        normalize_storage: NormalizeStorage,
        schema: Schema,
        load_id: str,
        config: NormalizeConfiguration,
    ) -> None:
        self.item_storage = item_storage
        self.normalize_storage = normalize_storage
        self.schema = schema
        self.load_id = load_id
        self.config = config

    @abstractmethod
    def __call__(self, extracted_items_file: str, root_table_name: str) -> List[TSchemaUpdate]: ...


class JsonLItemsNormalizer(ItemsNormalizer):
    def __init__(
        self,
        item_storage: DataItemStorage,
        normalize_storage: NormalizeStorage,
        schema: Schema,
        load_id: str,
        config: NormalizeConfiguration,
    ) -> None:
        super().__init__(item_storage, normalize_storage, schema, load_id, config)
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
        self, root_table_name: str, items: List[TDataItem], may_have_pua: bool, skip_write: bool
    ) -> TSchemaUpdate:
        column_schemas = self._column_schemas
        schema_update: TSchemaUpdate = {}
        schema = self.schema
        schema_name = schema.name
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
                    # we skip write when discovering schema for empty file
                    if not skip_write:
                        self.item_storage.write_data_item(
                            self.load_id, schema_name, table_name, row, columns
                        )
            except StopIteration:
                pass
            signals.raise_if_signalled()
        return schema_update

    def __call__(
        self,
        extracted_items_file: str,
        root_table_name: str,
    ) -> List[TSchemaUpdate]:
        schema_updates: List[TSchemaUpdate] = []
        with self.normalize_storage.extracted_packages.storage.open_file(
            extracted_items_file, "rb"
        ) as f:
            # enumerate jsonl file line by line
            line: bytes = None
            for line_no, line in enumerate(f):
                items: List[TDataItem] = json.loadb(line)
                partial_update = self._normalize_chunk(
                    root_table_name, items, may_have_pua(line), skip_write=False
                )
                schema_updates.append(partial_update)
                logger.debug(f"Processed {line_no+1} lines from file {extracted_items_file}")
            if line is None and root_table_name in self.schema.tables:
                # TODO: we should push the truncate jobs via package state
                # not as empty jobs. empty jobs should be reserved for
                # materializing schemas and other edge cases ie. empty parquet files
                root_table = self.schema.tables[root_table_name]
                if not has_table_seen_data(root_table):
                    # if this is a new table, add normalizer columns
                    partial_update = self._normalize_chunk(
                        root_table_name, [{}], False, skip_write=True
                    )
                    schema_updates.append(partial_update)
                self.item_storage.write_empty_items_file(
                    self.load_id,
                    self.schema.name,
                    root_table_name,
                    self.schema.get_table_columns(root_table_name),
                )
                logger.debug(
                    f"No lines in file {extracted_items_file}, written empty load job file"
                )

        return schema_updates


class ArrowItemsNormalizer(ItemsNormalizer):
    REWRITE_ROW_GROUPS = 1

    def _write_with_dlt_columns(
        self, extracted_items_file: str, root_table_name: str, add_load_id: bool, add_dlt_id: bool
    ) -> List[TSchemaUpdate]:
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
        columns_schema = schema.get_table_columns(root_table_name)
        # if we use adapter to convert arrow to dicts, then normalization is not necessary
        is_native_arrow_writer = not issubclass(self.item_storage.writer_cls, ArrowToObjectAdapter)
        should_normalize: bool = None
        with self.normalize_storage.extracted_packages.storage.open_file(
            extracted_items_file, "rb"
        ) as f:
            for batch in pyarrow.pq_stream_with_new_columns(
                f, new_columns, row_groups_per_read=self.REWRITE_ROW_GROUPS
            ):
                items_count += batch.num_rows
                # we may need to normalize
                if is_native_arrow_writer and should_normalize is None:
                    should_normalize, _, _, _ = pyarrow.should_normalize_arrow_schema(
                        batch.schema, columns_schema, schema.naming
                    )
                    if should_normalize:
                        logger.info(
                            f"When writing arrow table to {root_table_name} the schema requires"
                            " normalization because its shape does not match the actual schema of"
                            " destination table. Arrow table columns will be reordered and missing"
                            " columns will be added if needed."
                        )
                if should_normalize:
                    batch = pyarrow.normalize_py_arrow_item(
                        batch, columns_schema, schema.naming, self.config.destination_capabilities
                    )
                self.item_storage.write_data_item(
                    load_id,
                    schema.name,
                    root_table_name,
                    batch,
                    columns_schema,
                )
        # TODO: better to check if anything is in the buffer and skip writing file
        if items_count == 0 and not is_native_arrow_writer:
            self.item_storage.write_empty_items_file(
                load_id,
                schema.name,
                root_table_name,
                columns_schema,
            )

        return [schema_update]

    def _fix_schema_precisions(
        self, root_table_name: str, arrow_schema: Any
    ) -> List[TSchemaUpdate]:
        """Update precision of timestamp columns to the precision of parquet being normalized.
        Reduce the precision if it is out of range of destination timestamp precision.
        """
        schema = self.schema
        table = schema.tables[root_table_name]
        max_precision = self.config.destination_capabilities.timestamp_precision

        new_cols: TTableSchemaColumns = {}
        for key, column in table["columns"].items():
            if column.get("data_type") in ("timestamp", "time"):
                if prec := column.get("precision"):
                    # apply the arrow schema precision to dlt column schema
                    data_type = pyarrow.get_column_type_from_py_arrow(arrow_schema.field(key).type)
                    if data_type["data_type"] in ("timestamp", "time"):
                        prec = data_type["precision"]
                    # limit with destination precision
                    if prec > max_precision:
                        prec = max_precision
                    new_cols[key] = dict(column, precision=prec)  # type: ignore[assignment]
        if not new_cols:
            return []
        return [
            {root_table_name: [schema.update_table({"name": root_table_name, "columns": new_cols})]}
        ]

    def __call__(self, extracted_items_file: str, root_table_name: str) -> List[TSchemaUpdate]:
        # read schema and counts from file metadata
        from dlt.common.libs.pyarrow import get_parquet_metadata

        with self.normalize_storage.extracted_packages.storage.open_file(
            extracted_items_file, "rb"
        ) as f:
            num_rows, arrow_schema = get_parquet_metadata(f)
            file_metrics = DataWriterMetrics(extracted_items_file, num_rows, f.tell(), 0, 0)
        # when parquet files is saved, timestamps will be truncated and coerced. take the updated values
        # and apply them to dlt schema
        base_schema_update = self._fix_schema_precisions(root_table_name, arrow_schema)

        add_dlt_id = self.config.parquet_normalizer.add_dlt_id
        add_dlt_load_id = self.config.parquet_normalizer.add_dlt_load_id
        # if we need to add any columns or the file format is not parquet, we can't just import files
        must_rewrite = (
            add_dlt_id or add_dlt_load_id or self.item_storage.writer_spec.file_format != "parquet"
        )
        if not must_rewrite:
            # in rare cases normalization may be needed
            must_rewrite, _, _, _ = pyarrow.should_normalize_arrow_schema(
                arrow_schema, self.schema.get_table_columns(root_table_name), self.schema.naming
            )
        if must_rewrite:
            logger.info(
                f"Table {root_table_name} parquet file {extracted_items_file} must be rewritten:"
                f" add_dlt_id: {add_dlt_id} add_dlt_load_id: {add_dlt_load_id} destination file"
                f" format: {self.item_storage.writer_spec.file_format} or due to required"
                " normalization "
            )
            schema_update = self._write_with_dlt_columns(
                extracted_items_file, root_table_name, add_dlt_load_id, add_dlt_id
            )
            return base_schema_update + schema_update

        logger.info(
            f"Table {root_table_name} parquet file {extracted_items_file} will be directly imported"
            " without normalization"
        )
        parts = ParsedLoadJobFileName.parse(extracted_items_file)
        self.item_storage.import_items_file(
            self.load_id,
            self.schema.name,
            parts.table_name,
            self.normalize_storage.extracted_packages.storage.make_full_path(extracted_items_file),
            file_metrics,
        )

        return base_schema_update
