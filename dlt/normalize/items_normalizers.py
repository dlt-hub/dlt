from copy import copy
from typing import List, Dict, Sequence, Set, Any, Optional, Tuple
from abc import abstractmethod
from functools import lru_cache

import sqlglot

from dlt.common.data_types.type_helpers import coerce_value, py_type_to_sc_type
from dlt.common.data_types.typing import TDataType
from dlt.common.destination.capabilities import adjust_column_schema_to_capabilities
from dlt.common.libs.sqlglot import TSqlGlotDialect
from dlt.common import logger
from dlt.common.json import json
from dlt.common.data_writers.writers import ArrowToObjectAdapter
from dlt.common.json import custom_pua_decode, may_have_pua
from dlt.common.metrics import DataWriterMetrics
from dlt.common.normalizers.json.relational import DataItemNormalizer as RelationalNormalizer
from dlt.common.normalizers.json.helpers import get_root_row_id_type
from dlt.common.schema import utils
from dlt.common.schema.typing import (
    C_DLT_ID,
    C_DLT_LOAD_ID,
    TColumnSchema,
    TPartialTableSchema,
    TSchemaEvolutionMode,
    TTableSchemaColumns,
    TSchemaContractDict,
)
from dlt.common.schema.utils import (
    dlt_id_column,
    dlt_load_id_column,
    has_table_seen_data,
    is_complete_column,
    normalize_table_identifiers,
    is_nested_table,
    has_seen_null_first_hint,
)
from dlt.common.schema import utils
from dlt.common.schema.exceptions import CannotCoerceColumnException, CannotCoerceNullException
from dlt.common.storages.load_storage import LoadStorage
from dlt.common.time import normalize_timezone
from dlt.common.utils import read_dialect_and_sql
from dlt.common.storages import NormalizeStorage
from dlt.common.storages.data_item_storage import DataItemStorage
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.common.typing import VARIANT_FIELD_FORMAT, DictStrAny, REPattern, StrAny, TDataItem
from dlt.common.schema import TSchemaUpdate, Schema
from dlt.common.exceptions import MissingDependencyException
from dlt.common.normalizers.utils import generate_dlt_ids

from dlt.normalize.exceptions import NormalizeException
from dlt.normalize.configuration import NormalizeConfiguration

try:
    from dlt.common.libs import pyarrow
    from dlt.common.libs.pyarrow import pyarrow as pa
except MissingDependencyException:
    pyarrow = None
    pa = None


DLT_SUBQUERY_NAME = "_dlt_subquery"


class SqlModel:
    """
    A SqlModel is a named tuple that contains a query and a dialect.
    It is used to represent a SQL query and the dialect to use for parsing it.
    """

    __slots__ = ("_query", "_dialect")

    def __init__(self, query: str, dialect: Optional[str] = None) -> None:
        self._query = query
        self._dialect = dialect

    def to_sql(self) -> str:
        return self._query

    @property
    def query_dialect(self) -> str:
        return self._dialect

    @classmethod
    def from_query_string(cls, query: str, dialect: Optional[str] = None) -> "SqlModel":
        """
        Creates a SqlModel from a raw SQL query string using sqlglot.
        Ensures that the parsed query is an instance of sqlglot.exp.Select.

        Args:
            query (str): The raw SQL query string.
            dialect (Optional[str]): The SQL dialect to use for parsing.

        Returns:
            SqlModel: An instance of SqlModel with the normalized query and dialect.

        Raises:
            ValueError: If the parsed query is not an instance of sqlglot.exp.Select.
        """

        parsed_query = sqlglot.parse_one(query, read=dialect)

        # Ensure the parsed query is a SELECT statement
        if not isinstance(parsed_query, sqlglot.exp.Select):
            raise ValueError("Only SELECT statements are allowed to create a `SqlModel`.")

        normalized_query = parsed_query.sql(dialect=dialect)
        return cls(query=normalized_query, dialect=dialect)


class ItemsNormalizer:
    def __init__(
        self,
        item_storage: DataItemStorage,
        load_storage: LoadStorage,
        normalize_storage: NormalizeStorage,
        schema: Schema,
        load_id: str,
        config: NormalizeConfiguration,
    ) -> None:
        self.item_storage = item_storage
        self.load_storage = load_storage
        self.normalize_storage = normalize_storage
        self.schema = schema
        self.load_id = load_id
        self.config = config
        self.naming = self.schema.naming

    def _maybe_cancel(self) -> None:
        self.load_storage.new_packages.raise_if_cancelled(self.load_id)

    @abstractmethod
    def __call__(self, extracted_items_file: str, root_table_name: str) -> List[TSchemaUpdate]: ...


class ModelItemsNormalizer(ItemsNormalizer):
    def _normalize_casefold(self, ident: str) -> str:
        return self.config.destination_capabilities.casefold_identifier(
            self.schema.naming.normalize_identifier(ident)
        )

    def _uuid_expr_for_dialect(self, dialect: TSqlGlotDialect) -> sqlglot.exp.Expression:
        """
        Generates a UUID expression based on the specified dialect.

        Args:
            dialect (str): The SQL dialect for which the UUID expression needs to be generated.

        Returns:
            sqlglot.exp.Expression: A SQL expression that generates a UUID for the specified dialect.
        """

        # NOTE: redshift and sqlite don't have an in-built uuid function
        if dialect == "redshift":
            row_num = sqlglot.exp.Window(
                this=sqlglot.exp.Anonymous(this="row_number"),
                partition_by=None,
                order=None,
            )
            concat_expr = sqlglot.exp.func(
                "CONCAT",
                sqlglot.exp.Literal.string(self.load_id),
                sqlglot.exp.Literal.string("-"),
                row_num,
            )
            return sqlglot.exp.func("MD5", concat_expr)
        elif dialect == "sqlite":
            return sqlglot.exp.func(
                "lower",
                sqlglot.exp.func(
                    "hex", sqlglot.exp.func("randomblob", sqlglot.exp.Literal.number(16))
                ),
            )
        elif dialect == "clickhouse":
            return sqlglot.exp.func("generateUUIDv4")
        # NOTE: UUID in Athena creates a native UUID data type
        # which needs to be typecasted
        elif dialect == "athena":
            return sqlglot.exp.Cast(
                this=sqlglot.exp.func("UUID"), to=sqlglot.exp.DataType.build("VARCHAR")
            )
        else:
            return sqlglot.exp.func("UUID")

    def _adjust_outer_select_with_dlt_columns(
        self,
        sql_dialect: TSqlGlotDialect,
        outer_parsed_select: sqlglot.exp.Select,
        root_table_name: str,
    ) -> Optional[TSchemaUpdate]:
        """
        Adds or replaces dlt-specific columns (`_dlt_id`, `_dlt_load_id`) in the SELECT statement.

        Args:
            outer_parsed_select (sqlglot.exp.Select): The parsed outer SELECT statement.
            root_table_name (str): The name of the root table being normalized.

        Returns:
            Optional[TSchemaUpdate]: Schema updates for the added or replaced columns, or None if no updates were made.
        """
        model_config = self.config.model_normalizer
        if not (model_config.add_dlt_load_id or model_config.add_dlt_id):
            return None

        schema_update: TSchemaUpdate = {}
        schema = self.schema

        existing = {
            select.alias.lower(): idx for idx, select in enumerate(outer_parsed_select.selects)
        }

        NORM_C_DLT_LOAD_ID = self._normalize_casefold(C_DLT_LOAD_ID)
        NORM_C_DLT_ID = self._normalize_casefold(C_DLT_ID)

        # 1. Handle _dlt_load_id addition
        if model_config.add_dlt_load_id:
            # Build aliased expression
            dlt_load_id_expr = sqlglot.exp.Alias(
                this=sqlglot.exp.Literal.string(self.load_id),
                alias=sqlglot.to_identifier(NORM_C_DLT_LOAD_ID, quoted=True),
            )
            # Replace in-place if already present, otherwise append and update schema
            idx = existing.get(C_DLT_LOAD_ID)
            if idx is not None:
                outer_parsed_select.selects[idx] = dlt_load_id_expr
            else:
                outer_parsed_select.selects.append(dlt_load_id_expr)
                partial_table = normalize_table_identifiers(
                    {
                        "name": root_table_name,
                        "columns": {C_DLT_LOAD_ID: dlt_load_id_column()},
                    },
                    schema.naming,
                )
                schema.update_table(partial_table)
                table_updates = schema_update.setdefault(root_table_name, [])
                table_updates.append(partial_table)

        # 2. Handle _dlt_id addition
        if model_config.add_dlt_id:
            idx = existing.get(C_DLT_ID)
            # Do nothing if already present,
            # otherwise append and update schema only if possible
            if idx is None:
                row_id_type = get_root_row_id_type(schema, root_table_name)
                if row_id_type != "random":
                    raise NormalizeException(
                        "`add_dlt_id` was enabled in the model normalizer config, "
                        "but this is only supported when the row id type equals 'random'`. "
                        f"Received row id type: '{row_id_type}'."
                    )
                dlt_id_expr = sqlglot.exp.Alias(
                    this=self._uuid_expr_for_dialect(sql_dialect),
                    alias=sqlglot.to_identifier(NORM_C_DLT_ID, quoted=True),
                )
                outer_parsed_select.selects.append(dlt_id_expr)
                partial_table = normalize_table_identifiers(
                    {
                        "name": root_table_name,
                        "columns": {C_DLT_ID: dlt_id_column()},
                    },
                    schema.naming,
                )
                schema.update_table(partial_table)
                table_updates = schema_update.setdefault(root_table_name, [])
                table_updates.append(partial_table)

        return schema_update or None

    def _reorder_or_adjust_outer_select(
        self,
        outer_parsed_select: sqlglot.exp.Select,
        columns: TTableSchemaColumns,
        root_table_name: str,
    ) -> None:
        """
        Reorders or adjusts the SELECT statement to match the schema:
        1. Adds missing columns as NULL.
        2. Removes extra columns not in the schema.

        Args:
            outer_parsed_select (sqlglot.exp.Select): The parsed outer SELECT statement.
            columns (TTableSchemaColumns): The schema columns to match.
        """
        # Map alias name -> expression
        alias_map = {expr.alias.lower(): expr for expr in outer_parsed_select.selects}

        # Build new selects list in correct schema column order
        new_selects = []
        for col in columns:
            lower_col = col.lower()
            expr = alias_map.get(lower_col)
            if expr:
                new_selects.append(expr)
            # If there's no such column select, just put null if nullable
            else:
                if columns[col]["nullable"]:
                    new_selects.append(
                        sqlglot.exp.Alias(
                            this=sqlglot.exp.Null(),
                            alias=sqlglot.to_identifier(self._normalize_casefold(col), quoted=True),
                        )
                    )
                else:
                    exc = CannotCoerceNullException(self.schema.name, root_table_name, col)
                    exc.args = (
                        exc.args[0]
                        + " — column is missing from the SELECT clause but is non-nullable and must"
                        " be explicitly selected",
                    )
                    raise exc

        outer_parsed_select.set("expressions", new_selects)

    def _build_outer_select_statement(
        self,
        select_dialect: TSqlGlotDialect,
        parsed_select: sqlglot.exp.Select,
        columns: TTableSchemaColumns,
    ) -> Tuple[sqlglot.exp.Select, bool]:
        """
        Wraps the parsed SELECT statement in a subquery and builds an outer SELECT statement.

        Args:
            select_dialect (str): The SQL dialect to use for parsing and formatting.
            parsed_select (sqlglot.exp.Select): The parsed SELECT statement.
            columns (TTableSchemaColumns): The schema columns to match.

        Returns:
            Tuple[sqlglot.exp.Select, bool]: The outer SELECT statement and a flag indicating if reordering is needed.
        """
        # Wrap parsed select in a subquery
        subquery = parsed_select.subquery(alias=DLT_SUBQUERY_NAME)

        # Build outer SELECT list
        selected_columns = []
        outer_selects: List[sqlglot.exp.Expression] = []
        for select in parsed_select.selects:
            if isinstance(select, sqlglot.exp.Alias):
                name = select.alias

                # NOTE: for bigquery, quoted identifiers are treated as sqlglot.exp.Dot
            elif isinstance(select, sqlglot.exp.Column) or isinstance(select, sqlglot.exp.Dot):
                name = select.output_name or select.name

            else:
                raise ValueError(
                    "\n\nUnsupported SELECT expression in the model query:\n\n "
                    f" {select.sql(select_dialect)}\n\nOnly simple column selections like `column`"
                    " or `column AS alias` are currently supported.\n"
                )

            norm_casefolded = self._normalize_casefold(name)
            selected_columns.append(norm_casefolded)

            column_ref = sqlglot.exp.Dot(
                this=sqlglot.to_identifier(DLT_SUBQUERY_NAME),
                expression=sqlglot.to_identifier(name, quoted=True),
            )

            outer_selects.append(column_ref.as_(norm_casefolded, quoted=True))

        needs_reordering = selected_columns != list(columns.keys())

        # Create the outer select statement
        outer_select = sqlglot.select(*outer_selects).from_(subquery)

        return outer_select, needs_reordering

    def __call__(self, extracted_items_file: str, root_table_name: str) -> List[TSchemaUpdate]:
        self._maybe_cancel()
        with self.normalize_storage.extracted_packages.storage.open_file(
            extracted_items_file, "r"
        ) as f:
            sql_dialect, select_statement = read_dialect_and_sql(
                file_obj=f,
                fallback_dialect=self.config.destination_capabilities.sqlglot_dialect,  # caps are available at this point
            )

        # TODO the dialect here should be the "query dialect"; i.e., the transpilation input
        parsed_select = sqlglot.parse_one(select_statement, read=sql_dialect)

        # The query is ensured to be a select statement upstream,
        # but we double check here
        if not isinstance(parsed_select, sqlglot.exp.Select):
            raise ValueError("Only SELECT statements should be used as `SqlModel` queries.")

        # Star selects are not allowed
        if any(
            isinstance(expr, sqlglot.exp.Star)
            or (isinstance(expr, sqlglot.exp.Column) and isinstance(expr.this, sqlglot.exp.Star))
            for expr in parsed_select.selects
        ):
            raise ValueError(
                "\n\nA `SELECT *` was detected in the model query:\n\n"
                f"{parsed_select.sql(sql_dialect)}\n\n"
                "Model queries using a star (`*`) expression cannot be normalized. "
                "Please rewrite the query to explicitly specify the columns to be selected.\n"
            )

        outer_parsed_select, needs_reordering = self._build_outer_select_statement(
            sql_dialect, parsed_select, self.schema.get_table_columns(root_table_name)
        )

        schema_updates = []
        dlt_col_update = self._adjust_outer_select_with_dlt_columns(
            sql_dialect, outer_parsed_select, root_table_name
        )

        if dlt_col_update:
            schema_updates.append(dlt_col_update)

        if needs_reordering:
            self._reorder_or_adjust_outer_select(
                outer_parsed_select, self.schema.get_table_columns(root_table_name), root_table_name
            )

        # TODO the dialect here should be the "destination dialect"; i.e., the transpilation output
        normalized_query = outer_parsed_select.sql(dialect=sql_dialect)
        self.item_storage.write_data_item(
            self.load_id,
            self.schema.name,
            root_table_name,
            SqlModel.from_query_string(normalized_query, sql_dialect),
            {},
        )

        return schema_updates


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
        self._full_ident_path_tracker: Dict[str, Tuple[str, ...]] = {}
        self._shorten_fragments = lru_cache(maxsize=None)(self.schema.naming.shorten_fragments)
        self._check_table_exists = lru_cache(maxsize=None)(self._check_if_table_exists_impl)
        self._check_flattened_to_cols = lru_cache(maxsize=None)(self._check_if_flattened_impl)

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

                    # track full ident paths of tables
                    if table_name not in self._full_ident_path_tracker:
                        self._full_ident_path_tracker[table_name] = parent_path + ident_path

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

        self._clean_seen_null_first_hint(schema_update)
        return schema_update

    def _clean_seen_null_first_hint(self, schema_update: TSchemaUpdate) -> None:
        """
        Performs schema and schema update cleanup related to `seen-null-first` hints by
        removing entire columns with `seen-null-first` hints from parent tables
        when those columns have been converted to nested tables.

        NOTE: The `seen-null-first` hint is used during schema inference to track columns
        that were first encountered with null values. In cases where subsequent
        non-null values create a nested table, the entire
        column with the `seen-null-first` hint in parent table becomes obsolete.

        Args:
            schema_update (TSchemaUpdate): Dictionary mapping table names to their table updates.
        """
        schema_update_copy = schema_update.copy()
        for table_name, table_updates in schema_update_copy.items():
            last_ident_path = self._full_ident_path_tracker.get(table_name)[-1]

            for table_update in table_updates:
                # Remove the entire column with hint from parent table if it was created as a nested table
                if is_nested_table(table_update):
                    parent_name = table_update.get("parent")
                    parent_col_schemas = self.schema.get_table_columns(
                        parent_name, include_incomplete=True
                    )
                    parent_col_schema = parent_col_schemas.get(last_ident_path)

                    # remove only incomplete columns: both None, simple and complex values may be received by a column
                    # in any order
                    if (
                        parent_col_schema
                        and has_seen_null_first_hint(parent_col_schema)
                        and not is_complete_column(parent_col_schema)
                    ):
                        parent_col_schemas.pop(last_ident_path)
                        parent_updates = schema_update.get(parent_name, [])
                        for j, parent_update in enumerate(parent_updates):
                            if last_ident_path in parent_update["columns"]:
                                schema_update[parent_name][j]["columns"].pop(last_ident_path)

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
        for col_name, v in row.items():
            # skip None values, we should infer the types later
            if v is None:
                # just check if column is nullable if it exists
                new_col_def = self._coerce_null_value(table_columns, table_name, col_name)
                new_col_name = col_name
            else:
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
    ) -> TColumnSchema:
        # return unbounded table
        if v is None and data_type is None:
            if self.schema._infer_hint("not_null", k):
                raise CannotCoerceNullException(self.schema.name, table_name, k)
            column_schema = TColumnSchema(
                name=k,
                nullable=True,
            )
            column_schema["x-normalizer"] = {"seen-null-first": True}
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

    def _check_if_table_exists_impl(self, ident_path: Tuple[str, ...], col_name: str) -> bool:
        """Check if the combination of ident_path and col_name represents an existing table.

        This method performs the expensive operations of:
        1. Calling shorten_fragments to compute the possible table name
        2. Looking up the table in schema._schema_tables

        Results are cached via _check_table_exists to avoid repeated computation
        for the same ident_path + col_name combinations during normalization.

        Args:
            ident_path (Tuple[str, ...]): Tuple of normalized path fragments leading to the column
            col_name (str): Name of the column to check

        Returns:
            bool: True if a table exists for this path combination, False otherwise
        """
        possible_table_name = self._shorten_fragments(*ident_path, col_name)
        return possible_table_name in self.schema._schema_tables

    def _check_if_flattened_impl(self, table_name: str, col_name: str) -> bool:
        """Check if col_name was flattened into compound columns in the table.

        This method performs the expensive operations of:
        1. Iterating through all columns to check for compound column prefixes

        Results are cached via _check_flattened_to_cols to avoid repeated computation
        for the same table_name + col_name combinations during normalization.

        Note: This check doesn't properly handle the edge case where very long column names
        are shortened during normalization. When compound columns are created from a shortened
        name, they use the shortened prefix, but this method checks against the current col_name
        which may differ. The normalizer doesn't maintain a mapping between original and
        shortened column names, so it can't match them correctly.

        Args:
            table_name (str): Name of the table to check
            col_name (str): Name of the column to check if it was flattened

        Returns:
            bool: True if compound columns exist with col_name as prefix, False otherwise
        """
        table_columns = self.schema.get_table_columns(table_name, include_incomplete=True)
        prefix = col_name + self.naming.PATH_SEPARATOR
        return any(col.startswith(prefix) for col in table_columns)

    def _coerce_null_value(
        self, table_columns: TTableSchemaColumns, table_name: str, col_name: str
    ) -> Optional[TColumnSchema]:
        """Raises when column is explicitly not nullable or creates unbounded column"""
        existing_column = table_columns.get(col_name)
        # If it exists as a direct child table or compound column(s) in the schema, don't infer
        if not existing_column:
            # Use cached checks to avoid expensive repeated lookups
            full_ident_path = self._full_ident_path_tracker.get(table_name)
            if full_ident_path and self._check_table_exists(full_ident_path, col_name):
                return None
            if table_columns and self._check_flattened_to_cols(table_name, col_name):
                return None
        if existing_column and utils.is_complete_column(existing_column):
            if not utils.is_nullable_column(existing_column):
                raise CannotCoerceNullException(self.schema.name, table_name, col_name)
        else:
            # generate unbounded column only if it does not exist or it does not
            # contain seen null
            if not existing_column or not existing_column.get("x-normalizer", {}).get(
                "seen-null-first"
            ):
                inferred_unbounded_col = self._infer_column(
                    k=col_name, v=None, data_type=None, table_name=table_name
                )
                return inferred_unbounded_col
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
        py_type = py_type_to_sc_type(type(v))
        # and coerce type if inference changed the python type
        try:
            coerced_v = coerce_value(col_type, py_type, v)
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
            mapped_type = py_type_to_sc_type(tv)
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


class ArrowItemsNormalizer(ItemsNormalizer):
    REWRITE_ROW_GROUPS = 1

    def _write_with_dlt_columns(
        self,
        extracted_items_file: str,
        root_table_name: str,
        add_dlt_id: bool,
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
            # TODO: use get_root_row_id_type to get row id type and generate deterministic
            #  row ids as well (using pandas helper function prepared for scd2)
            #  we could also generate random columns with pandas or duckdb if present
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
        self._maybe_cancel()
        with self.normalize_storage.extracted_packages.storage.open_file(
            extracted_items_file, "rb"
        ) as f:
            for batch in pyarrow.pq_stream_with_new_columns(
                f, new_columns, row_groups_per_read=self.REWRITE_ROW_GROUPS
            ):
                self._maybe_cancel()
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

    def __call__(self, extracted_items_file: str, root_table_name: str) -> List[TSchemaUpdate]:
        self._maybe_cancel()
        # read schema and counts from file metadata
        from dlt.common.libs.pyarrow import get_parquet_metadata

        with self.normalize_storage.extracted_packages.storage.open_file(
            extracted_items_file, "rb"
        ) as f:
            num_rows, arrow_schema = get_parquet_metadata(f)
            file_metrics = DataWriterMetrics(extracted_items_file, num_rows, f.tell(), 0, 0)

        add_dlt_id = self.config.parquet_normalizer.add_dlt_id
        # TODO: add dlt id only if not present in table
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
            return schema_update

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

        return []


class FileImportNormalizer(ItemsNormalizer):
    def __call__(self, extracted_items_file: str, root_table_name: str) -> List[TSchemaUpdate]:
        self._maybe_cancel()
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
