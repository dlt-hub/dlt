from copy import copy
from typing import List, Dict, Sequence, Set, Any, Optional, Tuple
from functools import lru_cache

from dlt.common.data_types.typing import TDataType
from dlt.common.destination.capabilities import adjust_column_schema_to_capabilities
from dlt.common import logger
from dlt.common.json import json
from dlt.common.json import custom_pua_decode, may_have_pua
from dlt.common.schema import utils
from dlt.common.schema.typing import (
    TColumnSchema,
    TPartialTableSchema,
    TSchemaEvolutionMode,
    TTableSchemaColumns,
    TSchemaContractDict,
)
from dlt.common.schema.utils import has_table_seen_data, is_complete_column
from dlt.common.schema.exceptions import CannotCoerceColumnException, CannotCoerceNullException
from dlt.common.schema import TSchemaUpdate, Schema
from dlt.common.storages.load_storage import LoadStorage
from dlt.common.storages import NormalizeStorage
from dlt.common.storages.data_item_storage import DataItemStorage
from dlt.common.time import normalize_timezone
from dlt.common.typing import VARIANT_FIELD_FORMAT, DictStrAny, REPattern, StrAny, TDataItem

from dlt.normalize.configuration import NormalizeConfiguration
from dlt.normalize.items_normalizers.base import ItemsNormalizer


class JsonLItemsNormalizer(ItemsNormalizer):
    def __init__(
        self,
        item_storage: DataItemStorage,
        load_storage: LoadStorage,
        normalize_storage: NormalizeStorage,
        schema: Schema,
        load_id: str,
        config: NormalizeConfiguration,
    ) -> None:
        super().__init__(item_storage, load_storage, normalize_storage, schema, load_id, config)
        self._table_contracts: Dict[str, TSchemaContractDict] = {}
        self._filtered_tables: Set[str] = set()
        self._filtered_tables_columns: Dict[str, Dict[str, TSchemaEvolutionMode]] = {}
        # quick access to column schema for writers below
        self._column_schemas: Dict[str, TTableSchemaColumns] = {}
        self._null_only_columns: Dict[str, Set[str]] = {}
        self._shorten_fragments = lru_cache(maxsize=None)(self.schema.naming.shorten_fragments)
        # cache coercion interface from data item normalizer
        _din = self.schema.data_item_normalizer
        self._type_map = _din.py_type_to_sc_type_map
        self._can_coerce_type = _din.can_coerce_type
        self._coerce_type = _din.coerce_type
        self._py_type_to_sc_type = _din.py_type_to_sc_type

    @property
    def null_only_columns(self) -> Dict[str, Set[str]]:
        return self._null_only_columns

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
                    (table_name, parent_path, ident_path), row = row_info
                    parent_table = self._shorten_fragments(*parent_path)

                    # rows belonging to filtered out tables are skipped
                    if table_name in self._filtered_tables:
                        # stop descending into further rows
                        should_descend = False
                        continue

                    # filter row, may eliminate some or all fields
                    row = self._filter_row(table_name, row)
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
                    row, partial_table = self._coerce_row(table_name, parent_table, row)

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

        return schema_update

    def _coerce_row(
        self, table_name: str, parent_table: str, row: StrAny
    ) -> Tuple[DictStrAny, TPartialTableSchema]:
        """Fits values of fields present in `row` into a schema of `table_name`. Will coerce values into data types and infer new tables and column schemas.

        Method expects that field names in row are already normalized.
        * if table schema for `table_name` does not exist, new table is created
        * if column schema for a field in `row` does not exist, it is inferred from data
        * if incomplete column schema (no data type) exists, column is inferred from data and existing hints are applied
        * fields with None value are removed

        Returns tuple with row with coerced values and a partial table containing just the newly added columns or None if no changes were detected
        """
        # get existing or create a new table
        updated_table_partial: TPartialTableSchema = None
        table = self.schema._schema_tables.get(table_name)
        if not table:
            table = utils.new_table(table_name, parent_table)
        table_columns = table["columns"]

        new_row: DictStrAny = {}
        type_map = self._type_map
        can_coerce_type = self._can_coerce_type
        coerce_type = self._coerce_type
        for col_name, v in row.items():
            # skip None values, we should infer the types later
            if v is None:
                # just check if column is nullable if it exists
                new_col_def = self._coerce_null_value(table_columns, table_name, col_name)
                new_col_name = col_name
            else:
                # fast path: known column with matching type
                if existing_column := table_columns.get(col_name):
                    if col_type := existing_column.get("data_type"):
                        py_type = type_map.get(type(v))
                        if py_type == col_type:
                            # happy path: complete column, type matches, no coercion needed
                            new_row[col_name] = v
                            continue
                        if py_type and can_coerce_type(col_type, py_type):
                            # type mismatch but coercion is known - try fast path
                            try:
                                new_v = coerce_type(col_type, py_type, v)
                                if col_type == "timestamp":
                                    timezone = existing_column.get("timezone", True)
                                    new_v = normalize_timezone(new_v, timezone)
                                new_row[col_name] = new_v
                                continue
                            except (ValueError, SyntaxError):
                                # coercion failed - fall through to slow path for variant handling
                                pass
                # new column, incomplete column or variant
                new_col_name, new_col_def, new_v = self._coerce_non_null_value(
                    table_columns, table_name, col_name, v
                )
                new_row[new_col_name] = new_v
            if new_col_def:
                if not updated_table_partial:
                    # create partial table with only the new columns
                    updated_table_partial = copy(table)
                    updated_table_partial["columns"] = {}
                updated_table_partial["columns"][new_col_name] = new_col_def

        return new_row, updated_table_partial

    def _infer_column(
        self,
        k: str,
        v: Any,
        data_type: TDataType = None,
        is_variant: bool = False,
        table_name: str = None,
    ) -> Optional[TColumnSchema]:
        if v is None and data_type is None:
            if self.schema._infer_hint("not_null", k):
                raise CannotCoerceNullException(self.schema.name, table_name, k)
            return None
        else:
            column_schema = TColumnSchema(
                name=k,
                data_type=data_type or self._infer_column_type(v, k),
                nullable=not self.schema._infer_hint("not_null", k),
            )
        # check other preferred hints that are available
        for hint in self.schema._compiled_hints:
            # already processed
            if hint == "not_null":
                continue
            column_prop = utils.hint_to_column_prop(hint)
            hint_value = self.schema._infer_hint(hint, k)
            # set only non-default values
            if not utils.has_default_column_prop_value(column_prop, hint_value):
                column_schema[column_prop] = hint_value

        if is_variant:
            column_schema["variant"] = is_variant
        return column_schema

    def _coerce_null_value(
        self, table_columns: TTableSchemaColumns, table_name: str, col_name: str
    ) -> Optional[TColumnSchema]:
        """Raises on not-nullable columns, tracks null-only column names"""
        existing_column = table_columns.get(col_name)
        if existing_column and utils.is_complete_column(existing_column):
            if not utils.is_nullable_column(existing_column):
                raise CannotCoerceNullException(self.schema.name, table_name, col_name)
        else:
            if not existing_column and self.schema._infer_hint("not_null", col_name):
                raise CannotCoerceNullException(self.schema.name, table_name, col_name)
            self._null_only_columns.setdefault(table_name, set()).add(col_name)
        return None

    def _coerce_non_null_value(
        self,
        table_columns: TTableSchemaColumns,
        table_name: str,
        col_name: str,
        v: Any,
        is_variant: bool = False,
    ) -> Tuple[str, TColumnSchema, Any]:
        new_column: TColumnSchema = None
        existing_column = table_columns.get(col_name)
        # if column exist but is incomplete then keep it as new column
        if existing_column and not utils.is_complete_column(existing_column):
            new_column = existing_column
            existing_column = None

        # infer type or get it from existing table
        col_type = (
            existing_column["data_type"]
            if existing_column
            else self._infer_column_type(v, col_name, skip_preferred=is_variant)
        )
        # get data type of value
        py_type = self._py_type_to_sc_type(type(v))
        # and coerce type if inference changed the python type
        try:
            coerced_v = self._coerce_type(col_type, py_type, v)
        except (ValueError, SyntaxError):
            if is_variant:
                # this is final call: we cannot generate any more auto-variants
                raise CannotCoerceColumnException(
                    self.schema.name,
                    table_name,
                    col_name,
                    py_type,
                    table_columns[col_name]["data_type"],
                    v,
                )
            # otherwise we must create variant extension to the table
            # backward compatibility for complex types: if such column exists then use it
            variant_col_name = self.naming.shorten_fragments(
                col_name, VARIANT_FIELD_FORMAT % py_type
            )
            if py_type == "json":
                old_complex_col_name = self.naming.shorten_fragments(
                    col_name, VARIANT_FIELD_FORMAT % "complex"
                )
                if old_column := table_columns.get(old_complex_col_name):
                    if old_column.get("variant"):
                        variant_col_name = old_complex_col_name
            # pass final=True so no more auto-variants can be created recursively
            return self._coerce_non_null_value(
                table_columns, table_name, variant_col_name, v, is_variant=True
            )

        # if coerced value is variant, then extract variant value
        # note: checking runtime protocols with isinstance(coerced_v, SupportsVariant): is extremely slow so we check if callable as every variant is callable
        if callable(coerced_v):  # and isinstance(coerced_v, SupportsVariant):
            coerced_v = coerced_v()
            if isinstance(coerced_v, tuple):
                # variant recovered so call recursively with variant column name and variant value
                variant_col_name = self.naming.shorten_fragments(
                    col_name, VARIANT_FIELD_FORMAT % coerced_v[0]
                )
                return self._coerce_non_null_value(
                    table_columns, table_name, variant_col_name, coerced_v[1], is_variant=True
                )

        if not existing_column:
            # adjust to current capabilities, mostly removes hints that are default for a given
            # destination
            inferred_column = adjust_column_schema_to_capabilities(
                self._infer_column(col_name, v, data_type=col_type, is_variant=is_variant),
                self.config.destination_capabilities,
            )
            # if there's incomplete new_column then merge it with inferred column
            if new_column:
                # use all values present in incomplete column to override inferred column - also the defaults
                new_column = utils.merge_column(inferred_column, new_column)
            else:
                new_column = inferred_column

        # apply timestamp when column schema is known
        if col_type == "timestamp":
            timezone = (existing_column or new_column).get("timezone", True)
            coerced_v = normalize_timezone(coerced_v, timezone)

        return col_name, new_column, coerced_v

    def _infer_column_type(self, v: Any, col_name: str, skip_preferred: bool = False) -> TDataType:
        tv = type(v)
        # try to autodetect data type
        mapped_type = utils.autodetect_sc_type(self.schema._type_detections, tv, v)
        # if not try standard type mapping
        if mapped_type is None:
            mapped_type = self._py_type_to_sc_type(tv)
        # get preferred type based on column name
        preferred_type: TDataType = None
        if not skip_preferred:
            preferred_type = self.schema.get_preferred_type(col_name)
        return preferred_type or mapped_type

    def _filter_row(self, table_name: str, row: StrAny) -> StrAny:
        # TODO: remove this. move to extract stage
        # exclude row elements according to the rules in `filter` elements of the table
        # include rules have precedence and are used to make exceptions to exclude rules
        # the procedure will apply rules from the table_name and it's all parent tables up until root
        # parent tables are computed by `normalize_break_path` function so they do not need to exist in the schema
        # note: the above is not very clean. the `parent` element of each table should be used but as the rules
        #  are typically used to prevent not only table fields but whole tables from being created it is not possible

        if not (compiled_excludes := self.schema._compiled_excludes):
            # if there are no excludes in the whole schema, no modification to a row can be made
            # most of the schema do not use them
            return row

        def _exclude(
            path: str, excludes: Sequence[REPattern], includes: Sequence[REPattern]
        ) -> bool:
            is_included = False
            is_excluded = any(exclude.search(path) for exclude in excludes)
            if is_excluded:
                # we may have exception if explicitly included
                is_included = any(include.search(path) for include in includes)
            return is_excluded and not is_included

        # break table name in components
        branch = self.naming.break_path(table_name)
        compiled_includes = self.schema._compiled_includes

        # check if any of the rows is excluded by rules in any of the tables
        for i in range(len(branch), 0, -1):  # stop is exclusive in `range`
            # start at the top level table
            c_t = self.naming.make_path(*branch[:i])
            excludes = compiled_excludes.get(c_t)
            # only if there's possibility to exclude, continue
            if excludes:
                includes = compiled_includes.get(c_t) or []
                for field_name in list(row.keys()):
                    path = self.naming.make_path(*branch[i:], field_name)
                    if _exclude(path, excludes, includes):
                        # TODO: copy to new instance
                        del row[field_name]  # type: ignore
            # if row is empty, do not process further
            if not row:
                break
        return row

    def __call__(
        self,
        extracted_items_file: str,
        root_table_name: str,
    ) -> List[TSchemaUpdate]:
        self._maybe_cancel()
        schema_updates: List[TSchemaUpdate] = []
        with self.normalize_storage.extracted_packages.storage.open_file(
            extracted_items_file, "rb"
        ) as f:
            # enumerate jsonl file line by line
            line: bytes = None
            for line_no, line in enumerate(f):
                self._maybe_cancel()
                items: List[TDataItem] = json.loadb(line)
                partial_update = self._normalize_chunk(
                    root_table_name, items, may_have_pua(line), skip_write=False
                )
                schema_updates.append(partial_update)
                logger.debug(f"Processed {line_no+1} lines from file {extracted_items_file}")
            # empty json files are when replace write disposition is used in order to truncate table(s)
            if line is None and root_table_name in self.schema.tables:
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
