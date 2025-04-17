from typing import List, Dict, Set, Any, cast, Literal, Optional
from abc import abstractmethod

import sqlglot
import sqlglot.expressions

from dlt.common import logger
from dlt.common.json import json
from dlt.common.data_writers.writers import ArrowToObjectAdapter
from dlt.common.json import custom_pua_decode, may_have_pua
from dlt.common.metrics import DataWriterMetrics
from dlt.common.normalizers.json.relational import DataItemNormalizer as RelationalNormalizer
from dlt.common.runtime import signals
from dlt.common.schema.typing import (
    C_DLT_ID,
    C_DLT_LOAD_ID,
    TSchemaEvolutionMode,
    TTableSchemaColumns,
    TSchemaContractDict,
)
from dlt.common.schema.utils import (
    dlt_id_column,
    dlt_load_id_column,
    has_table_seen_data,
    normalize_table_identifiers,
)
from dlt.common.utils import read_dialect_and_sql
from dlt.common.storages import NormalizeStorage
from dlt.common.storages.data_item_storage import DataItemStorage
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.common.typing import DictStrAny, TDataItem
from dlt.common.schema import TSchemaUpdate, Schema
from dlt.common.exceptions import MissingDependencyException
from dlt.common.normalizers.utils import generate_dlt_ids
from dlt.extract.hints import SqlModel

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


class ModelItemsNormalizer(ItemsNormalizer):
    def _adjust_with_dlt_columns(
        self,
        parsed_select: sqlglot.exp.Select,
        root_table_name: str,
    ) -> Optional[TSchemaUpdate]:
        schema_update: TSchemaUpdate = {}
        schema = self.schema
        dialect = self.config.destination_capabilities.sqlglot_dialect

        # Build dlt column aliases based on config
        dlt_columns: dict[str, Optional[sqlglot.exp.Alias]] = {}

        NORM_C_DLT_LOAD_ID = self.config.destination_capabilities.casefold_identifier(C_DLT_LOAD_ID)
        NORM_C_DLT_ID = self.config.destination_capabilities.casefold_identifier(C_DLT_ID)

        if self.config.model_normalizer.add_dlt_load_id:
            dlt_columns[C_DLT_LOAD_ID] = sqlglot.exp.Alias(
                this=sqlglot.exp.Literal.string(self.load_id),
                alias=sqlglot.exp.to_identifier(NORM_C_DLT_LOAD_ID),
            )

        if self.config.model_normalizer.add_dlt_id and dialect != "redshift":
            function_name = "generateUUIDv4" if dialect == "clickhouse" else "UUID"
            dlt_columns[C_DLT_ID] = sqlglot.exp.Alias(
                this=sqlglot.exp.func(function_name),
                alias=sqlglot.exp.to_identifier(NORM_C_DLT_ID),
            )

        # Replace if dlt columns exist in the select statement, otherwise append
        for i, select in enumerate(parsed_select.selects):
            for column_name, alias_expr in dlt_columns.items():
                if alias_expr is None:
                    continue
                select_alias = (
                    select.alias_or_name.lower()
                    if isinstance(select, sqlglot.exp.Alias)
                    else (
                        select.output_name.lower()
                        if isinstance(select, sqlglot.exp.Column)
                        else None
                    )
                )
                if select_alias == column_name:
                    parsed_select.selects[i] = alias_expr
                    dlt_columns[column_name] = None  # Mark as replaced

        # Append any not-replaced dlt column aliases and update schema
        for column_name, alias_expr in dlt_columns.items():
            if alias_expr is None:
                continue
            parsed_select.selects.append(alias_expr)

            partial_table = normalize_table_identifiers(
                {
                    "name": root_table_name,
                    "columns": {
                        column_name: (
                            dlt_id_column() if column_name == "_dlt_id" else dlt_load_id_column()
                        )
                    },
                },
                schema.naming,
            )
            schema.update_table(partial_table)
            table_updates = schema_update.setdefault(root_table_name, [])
            table_updates.append(partial_table)

        return schema_update if schema_update else None

    def _normalize_selected_columns(
        self,
        parsed_select: sqlglot.exp.Select,
    ) -> None:
        """
        1. referenced column names will be normalized according to `naming`, that is, the casefold_identifier
        NOTE: We casefold the aliases in the ModelLoadJob.
        """
        for i, select in enumerate(parsed_select.selects):
            if isinstance(select, sqlglot.exp.Alias):
                col_expr = select.this
                original_name = col_expr.name
                normalized_name = self.config.destination_capabilities.casefold_identifier(
                    original_name
                )
                if normalized_name != original_name:
                    normalized_col = col_expr.copy()
                    normalized_col.set("this", sqlglot.exp.to_identifier(normalized_name))
                    parsed_select.selects[i] = sqlglot.exp.Alias(
                        this=normalized_col,
                        alias=select.alias,
                    )
            elif isinstance(select, sqlglot.exp.Column):
                original_name = select.name
                normalized_name = self.config.destination_capabilities.casefold_identifier(
                    original_name
                )
                if normalized_name != original_name:
                    normalized_col = select.copy()
                    normalized_col.set("name", sqlglot.exp.to_identifier(normalized_name))
                    parsed_select.selects[i] = normalized_col

    def _match_schema_and_select_statement(
        self,
        parsed_select: sqlglot.exp.Select,
        root_table_name: str,
        norm_col_names: List[str],
    ) -> Optional[TSchemaUpdate]:
        """
        Ensures the SELECT statement matches the schema by:
        1. Reordering the schema to match the SELECT statement if the keys are the same but the order is different.
        2. Adding columns to the SELECT statement if they are missing but present in the schema.
        3. Removing columns from the SELECT statement if they are present in the SELECT statement but not in the schema.
        """
        schema_update: TSchemaUpdate = {}
        schema = self.schema

        selected_column_names = [select.alias_or_name for select in parsed_select.selects]

        if norm_col_names == selected_column_names:
            # Perfect match: same columns, same order
            return None

        elif selected_column_names == ["*"]:
            # Nothing to do here  because it's still a star expression
            # and wasn't replaced with explicit columns upstream
            return None

        selected_set = set(selected_column_names)
        schema_set = set(norm_col_names)

        missing_cols = [col for col in norm_col_names if col not in selected_set]
        extra_cols = [col for col in selected_column_names if col not in schema_set]

        # Step 1: Remove extra columns
        if extra_cols:
            new_selects = [
                sel for sel in parsed_select.selects if sel.alias_or_name not in extra_cols
            ]
            parsed_select.selects.clear()
            parsed_select.selects.extend(new_selects)
            selected_column_names = [sel.alias_or_name for sel in parsed_select.selects]

        # Step 2: Insert missing columns at correct positions
        for col in missing_cols:
            ordinal = norm_col_names.index(col)  # desired position
            parsed_select.selects.insert(
                ordinal,
                sqlglot.exp.Alias(
                    this=sqlglot.exp.null(),
                    alias=sqlglot.exp.to_identifier(col),
                ),
            )
            selected_column_names.insert(ordinal, col)

        # Step 3: Reorder schema to match SELECT
        reordered_columns = {
            col: schema.get_table_columns(root_table_name)[col] for col in selected_column_names
        }

        partial_table = normalize_table_identifiers(
            {
                "name": root_table_name,
                "columns": reordered_columns,
            },
            schema.naming,
        )
        schema.update_table(partial_table)
        schema_update.setdefault(root_table_name, []).append(partial_table)

        return schema_update if schema_update else None

    def _handle_star_expression(
        self,
        parsed_select: sqlglot.exp.Select,
        norm_col_names: List[str],
    ) -> Optional[sqlglot.exp.Select]:
        """
        Replaces a star (*) expression in the SELECT statement with explicit column names
        from the schema for the given table. Logs a warning if no columns are available.
        """
        if len(parsed_select.selects) == 1 and isinstance(
            parsed_select.selects[0], sqlglot.exp.Star
        ):
            logger.warning(
                f"A star expression is present in the model query {parsed_select.sql()}."
                "Replacing it with the schema columns."
            )
            parsed_select = sqlglot.exp.select(
                *[sqlglot.exp.Column(this=sqlglot.exp.to_identifier(col)) for col in norm_col_names]
            )
            return parsed_select
        return None

    def __call__(self, extracted_items_file: str, root_table_name: str) -> List[TSchemaUpdate]:
        with self.normalize_storage.extracted_packages.storage.open_file(
            extracted_items_file, "r"
        ) as f:
            select_dialect, select_statement = read_dialect_and_sql(
                file_obj=f,
                fallback_dialect=self.config.destination_capabilities.sqlglot_dialect,  # caps are available at this point
            )

        parsed_select = sqlglot.parse_one(select_statement, read=select_dialect)
        parsed_select = cast(sqlglot.exp.Select, parsed_select)
        norm_schema_column_names = [
            self.config.destination_capabilities.casefold_identifier(key)
            for key in self.schema.get_table_columns(root_table_name).keys()
        ]

        # 1. handle star expression
        star_expr_handled = self._handle_star_expression(parsed_select, norm_schema_column_names)

        # 2. normalize selected column names. If a star expression was found,
        # the columns are already normalized
        # TODO: make this better
        if not star_expr_handled:
            self._normalize_selected_columns(parsed_select)

        if star_expr_handled:
            parsed_select = star_expr_handled

        # 3. add dlt columns
        schema_updates = []
        dlt_col_update = self._adjust_with_dlt_columns(parsed_select, root_table_name)
        if dlt_col_update:
            schema_updates.append(dlt_col_update)

        # 4. normalize selected column names again
        # because the dlt columns may have been added
        # TODO: make this better
        norm_schema_column_names = [
            self.config.destination_capabilities.casefold_identifier(key)
            for key in self.schema.get_table_columns(root_table_name).keys()
        ]

        normalized_query = parsed_select.sql(dialect=select_dialect)

        # 4. match schema and select statement
        schema_match_select_update = self._match_schema_and_select_statement(
            parsed_select, root_table_name, norm_schema_column_names
        )
        if schema_match_select_update:
            schema_updates.append(schema_match_select_update)

        normalized_query = parsed_select.sql(dialect=select_dialect)
        self.item_storage.write_data_item(
            self.load_id,
            self.schema.name,
            root_table_name,
            SqlModel.from_query_string(normalized_query, select_dialect),
            {},
        )

        return schema_updates


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
                                table_name
                                if table_name in schema.tables
                                else parent_table or table_name
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
                        schema.update_table(partial_table, normalize_identifiers=False)
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
            # empty json files are when replace write disposition is used in order to truncate table(s)
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
        self, extracted_items_file: str, root_table_name: str, add_dlt_id: bool
    ) -> List[TSchemaUpdate]:
        new_columns: List[Any] = []
        schema = self.schema
        load_id = self.load_id
        schema_update: TSchemaUpdate = {}
        data_normalizer = schema.data_item_normalizer

        if add_dlt_id and isinstance(data_normalizer, RelationalNormalizer):
            partial_table = normalize_table_identifiers(
                {
                    "name": root_table_name,
                    "columns": {C_DLT_ID: dlt_id_column()},
                },
                schema.naming,
            )
            schema.update_table(partial_table, normalize_identifiers=False)
            table_updates = schema_update.setdefault(root_table_name, [])
            table_updates.append(partial_table)
            new_columns.append(
                (
                    -1,
                    pa.field(data_normalizer.c_dlt_id, pyarrow.pyarrow.string(), nullable=False),
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
                    should_normalize = pyarrow.should_normalize_arrow_schema(
                        batch.schema, columns_schema, schema.naming
                    )[0]
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
        caps = self.config.destination_capabilities
        max_precision = caps.timestamp_precision

        new_cols: TTableSchemaColumns = {}
        for key, column in table["columns"].items():
            if column.get("data_type") in ("timestamp", "time"):
                prec = column.get("precision")
                if prec is not None:
                    # apply the arrow schema precision to dlt column schema
                    try:
                        data_type = pyarrow.get_column_type_from_py_arrow(
                            arrow_schema.field(key).type,
                            caps,
                        )
                    except pyarrow.UnsupportedArrowTypeException as e:
                        e.field_name = key
                        e.table_name = root_table_name
                        raise

                    if data_type["data_type"] in ("timestamp", "time"):
                        prec = data_type["precision"]
                    # limit with destination precision
                    if prec > max_precision:
                        prec = max_precision
                    new_cols[key] = dict(column, precision=prec)  # type: ignore[assignment]
        if not new_cols:
            return []
        partial_table = normalize_table_identifiers(
            {"name": root_table_name, "columns": new_cols}, schema.naming
        )
        schema.update_table(partial_table, normalize_identifiers=False)
        return [{root_table_name: [partial_table]}]

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
        # if we need to add any columns or the file format is not parquet, we can't just import files
        must_rewrite = add_dlt_id or self.item_storage.writer_spec.file_format != "parquet"
        if not must_rewrite:
            # in rare cases normalization may be needed
            must_rewrite = pyarrow.should_normalize_arrow_schema(
                arrow_schema, self.schema.get_table_columns(root_table_name), self.schema.naming
            )[0]
        if must_rewrite:
            logger.info(
                f"Table {root_table_name} parquet file {extracted_items_file} must be rewritten:"
                f" add_dlt_id: {add_dlt_id} destination file"
                f" format: {self.item_storage.writer_spec.file_format} or due to required"
                " normalization "
            )
            schema_update = self._write_with_dlt_columns(
                extracted_items_file, root_table_name, add_dlt_id
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


class FileImportNormalizer(ItemsNormalizer):
    def __call__(self, extracted_items_file: str, root_table_name: str) -> List[TSchemaUpdate]:
        logger.info(
            f"Table {root_table_name} {self.item_storage.writer_spec.file_format} file"
            f" {extracted_items_file} will be directly imported without normalization"
        )
        completed_columns = self.schema.get_table_columns(root_table_name)
        if not completed_columns:
            logger.warning(
                f"Table {root_table_name} has no completed columns for imported file"
                f" {extracted_items_file} and will not be created! Pass column hints to the"
                " resource or with dlt.mark.with_hints or create the destination table yourself."
            )
        with self.normalize_storage.extracted_packages.storage.open_file(
            extracted_items_file, "rb"
        ) as f:
            # TODO: sniff the schema depending on a file type
            file_metrics = DataWriterMetrics(extracted_items_file, 0, f.tell(), 0, 0)
        parts = ParsedLoadJobFileName.parse(extracted_items_file)
        self.item_storage.import_items_file(
            self.load_id,
            self.schema.name,
            parts.table_name,
            self.normalize_storage.extracted_packages.storage.make_full_path(extracted_items_file),
            file_metrics,
        )
        return []
