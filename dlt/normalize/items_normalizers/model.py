from typing import List, Optional

import sqlglot

from dlt.common.libs.sqlglot import (
    TSqlGlotDialect,
    SqlModel,
    build_outer_select_statement,
    reorder_or_adjust_outer_select,
    uuid_expr_for_dialect,
    validate_no_star_select,
)
from dlt.common.normalizers.json.helpers import get_root_row_id_type
from dlt.common.schema.typing import C_DLT_ID, C_DLT_LOAD_ID
from dlt.common.schema.utils import (
    dlt_id_column,
    dlt_load_id_column,
    normalize_table_identifiers,
)
from dlt.common.schema import TSchemaUpdate
from dlt.common.utils import read_dialect_and_sql

from dlt.normalize.exceptions import NormalizeException
from dlt.normalize.items_normalizers.base import ItemsNormalizer


class ModelItemsNormalizer(ItemsNormalizer):
    def _normalize_casefold(self, identifier: str) -> str:
        # use normalize_path() to preserve __ separators in already-normalized names
        normalized = self.schema.naming.normalize_path(identifier)
        return self.config.destination_capabilities.casefold_identifier(normalized)

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
                    this=uuid_expr_for_dialect(sql_dialect, self.load_id),
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

        validate_no_star_select(parsed_select, sql_dialect)

        outer_parsed_select, needs_reordering = build_outer_select_statement(
            sql_dialect,
            parsed_select,
            self.schema.get_table_columns(root_table_name),
            self._normalize_casefold,
        )

        schema_updates = []
        dlt_col_update = self._adjust_outer_select_with_dlt_columns(
            sql_dialect, outer_parsed_select, root_table_name
        )

        if dlt_col_update:
            schema_updates.append(dlt_col_update)

        if needs_reordering:
            reorder_or_adjust_outer_select(
                outer_parsed_select,
                self.schema.get_table_columns(root_table_name),
                self._normalize_casefold,
                self.schema.name,
                root_table_name,
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
