from typing import Any, Dict, List, Sequence, Tuple, cast, Optional, Callable, Union

import yaml
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.destination import PreparedTableSchema
from dlt.common.destination.utils import resolve_merge_strategy
from dlt.common.typing import TypedDict

from dlt.common.schema.typing import (
    TSortOrder,
    TColumnProp,
)
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    get_first_column_name_with_prop,
    get_dedup_sort_tuple,
    get_validity_column_names,
    get_active_record_timestamp,
    is_nested_table,
)
from dlt.common.storages.load_storage import ParsedLoadJobFileName
from dlt.common.storages.load_package import load_package_state as current_load_package
from dlt.common.utils import uniq_id
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.destinations.exceptions import MergeDispositionException
from dlt.destinations.job_impl import FollowupJobRequestImpl
from dlt.destinations.sql_client import SqlClientBase
from dlt.common.destination.exceptions import DestinationTransientException


class SqlJobCreationException(DestinationTransientException):
    def __init__(
        self, original_exception: Exception, table_chain: Sequence[PreparedTableSchema]
    ) -> None:
        tables_str = yaml.dump(
            table_chain, allow_unicode=True, default_flow_style=False, sort_keys=False
        )
        super().__init__(
            f"Could not create SQLFollowupJob with exception {str(original_exception)}. Table"
            f" chain: {tables_str}"
        )


class SqlFollowupJob(FollowupJobRequestImpl):
    """Sql base job for jobs that rely on the whole table chain"""

    @classmethod
    def from_table_chain(
        cls,
        table_chain: Sequence[PreparedTableSchema],
        sql_client: SqlClientBase[Any],
    ) -> FollowupJobRequestImpl:
        """Generates a list of sql statements, that will be executed by the sql client when the job is executed in the loader.

        The `table_chain` contains a list of schemas of nested tables, ordered by the ancestry (the root of the tree is first on the list).
        """
        root_table = table_chain[0]
        file_info = ParsedLoadJobFileName(
            root_table["name"], ParsedLoadJobFileName.new_file_id(), 0, "sql"
        )

        try:
            # Remove line breaks from multiline statements and write one SQL statement per line in output file
            # to support clients that need to execute one statement at a time (i.e. snowflake)
            sql = [
                " ".join(stmt.splitlines()) for stmt in cls.generate_sql(table_chain, sql_client)
            ]
            job = cls(file_info.file_name())
            job._save_text_file("\n".join(sql))
        except Exception as e:
            raise SqlJobCreationException(e, table_chain) from e

        return job

    @classmethod
    def generate_sql(
        cls,
        table_chain: Sequence[PreparedTableSchema],
        sql_client: SqlClientBase[Any],
    ) -> List[str]:
        pass


class SqlStagingFollowupJob(SqlFollowupJob):
    """Generates a list of sql statements that copy the data from staging dataset into destination dataset."""

    @classmethod
    def _generate_insert_sql(
        cls,
        table_chain: Sequence[PreparedTableSchema],
        sql_client: SqlClientBase[Any],
        truncate_first: bool,
    ) -> List[str]:
        sql: List[str] = []
        for table in table_chain:
            with sql_client.with_staging_dataset():
                staging_table_name = sql_client.make_qualified_table_name(table["name"])
            table_name = sql_client.make_qualified_table_name(table["name"])
            columns = ", ".join(
                map(
                    sql_client.escape_column_name,
                    get_columns_names_with_prop(table, "name"),
                )
            )
            if truncate_first:
                sql.append(sql_client._truncate_table_sql(table_name))
            sql.append(
                f"INSERT INTO {table_name}({columns}) SELECT {columns} FROM {staging_table_name}"
            )
        return sql


class SqlStagingReplaceFollowupJob(SqlStagingFollowupJob):
    """Generates a list of sql statements that replace the data from staging dataset into destination dataset."""

    @classmethod
    def _generate_clone_sql(
        cls,
        table_chain: Sequence[PreparedTableSchema],
        sql_client: SqlClientBase[Any],
    ) -> List[str]:
        """Drop and clone the table for supported destinations"""
        sql: List[str] = []
        for table in table_chain:
            with sql_client.with_staging_dataset():
                staging_table_name = sql_client.make_qualified_table_name(table["name"])
            table_name = sql_client.make_qualified_table_name(table["name"])
            sql.append(f"DROP TABLE IF EXISTS {table_name}")
            # recreate destination table with data cloned from staging table
            sql.append(f"CREATE TABLE {table_name} CLONE {staging_table_name}")
        return sql

    @classmethod
    def generate_sql(
        cls,
        table_chain: Sequence[PreparedTableSchema],
        sql_client: SqlClientBase[Any],
    ) -> List[str]:
        root_table = table_chain[0]
        if (
            root_table["x-replace-strategy"] == "staging-optimized"  # type: ignore[typeddict-item]
            and sql_client.capabilities.supports_clone_table
        ):
            return cls._generate_clone_sql(table_chain, sql_client)

        return cls._generate_insert_sql(table_chain, sql_client, truncate_first=True)


class SqlStagingCopyFollowupJob(SqlStagingFollowupJob):
    """Generates a list of sql statements that copy the data from staging dataset into destination dataset."""

    @classmethod
    def generate_sql(
        cls,
        table_chain: Sequence[PreparedTableSchema],
        sql_client: SqlClientBase[Any],
    ) -> List[str]:
        return cls._generate_insert_sql(table_chain, sql_client, truncate_first=False)


class SqlMergeFollowupJob(SqlFollowupJob):
    """
    Generates a list of sql statements that merge the data from staging dataset into destination dataset.
    If no merge keys are discovered, falls back to append.
    """

    @classmethod
    def generate_sql(
        cls,
        table_chain: Sequence[PreparedTableSchema],
        sql_client: SqlClientBase[Any],
    ) -> List[str]:
        # resolve only root table
        root_table = table_chain[0]
        merge_strategy = resolve_merge_strategy(
            {root_table["name"]: root_table}, root_table, sql_client.capabilities
        )

        merge_sql = None
        if merge_strategy == "delete-insert":
            merge_sql = cls.gen_merge_sql(table_chain, sql_client)
        elif merge_strategy == "upsert":
            merge_sql = cls.gen_upsert_sql(table_chain, sql_client)
        elif merge_strategy == "scd2":
            merge_sql = cls.gen_scd2_sql(table_chain, sql_client)

        # prepend setup code
        return cls._gen_table_setup_clauses(table_chain, sql_client) + merge_sql

    @classmethod
    def _gen_table_setup_clauses(
        cls, table_chain: Sequence[PreparedTableSchema], sql_client: SqlClientBase[Any]
    ) -> List[str]:
        """Subclasses may override this method to generate additional sql statements to run before the merge"""
        return []

    @classmethod
    def _gen_key_table_clauses(
        cls, primary_keys: Sequence[str], merge_keys: Sequence[str]
    ) -> List[str]:
        """Generate sql clauses to select rows to delete via merge and primary key. Return select all clause if no keys defined."""
        clauses: List[str] = []
        if primary_keys or merge_keys:
            if primary_keys:
                clauses.append(
                    " AND ".join(["%s.%s = %s.%s" % ("{d}", c, "{s}", c) for c in primary_keys])
                )
            if merge_keys:
                clauses.append(
                    " AND ".join(["%s.%s = %s.%s" % ("{d}", c, "{s}", c) for c in merge_keys])
                )
        return clauses or ["1=1"]

    @classmethod
    def gen_key_table_clauses(
        cls,
        root_table_name: str,
        staging_root_table_name: str,
        key_clauses: Sequence[str],
        for_delete: bool,
    ) -> List[str]:
        """Generate sql clauses that may be used to select or delete rows in root table of destination dataset

        A list of clauses may be returned for engines that do not support OR in subqueries. Like BigQuery
        """
        return [
            f"FROM {root_table_name} as d WHERE EXISTS (SELECT 1 FROM {staging_root_table_name} as"
            f" s WHERE {' OR '.join([c.format(d='d',s='s') for c in key_clauses])})"
        ]

    @classmethod
    def gen_delete_temp_table_sql(
        cls,
        table_name: str,
        unique_column: str,
        key_table_clauses: Sequence[str],
        sql_client: SqlClientBase[Any],
    ) -> Tuple[List[str], str]:
        """Generate sql that creates delete temp table and inserts `unique_column` from root table for all records to delete. May return several statements.

        Returns temp table name for cases where special names are required like SQLServer.
        """
        sql: List[str] = []
        temp_table_name = cls._new_temp_table_name(table_name, "delete", sql_client)
        select_statement = f"SELECT d.{unique_column} {key_table_clauses[0]}"
        sql.append(cls._to_temp_table(select_statement, temp_table_name, unique_column))
        for clause in key_table_clauses[1:]:
            sql.append(f"INSERT INTO {temp_table_name} SELECT {unique_column} {clause}")
        return sql, temp_table_name

    @classmethod
    def gen_select_from_dedup_sql(
        cls,
        table_name: str,
        primary_keys: Sequence[str],
        columns: Sequence[str],
        dedup_sort: Tuple[str, TSortOrder] = None,
        condition: str = None,
        condition_columns: Sequence[str] = None,
        skip_dedup: bool = False,
    ) -> str:
        """Returns SELECT FROM SQL statement.

        The FROM clause in the SQL statement represents a deduplicated version
        of the `table_name` table.

        Expects column names provided in arguments to be escaped identifiers.

        Args:
            table_name: Name of the table that is selected from.
            primary_keys: A sequence of column names representing the primary
              key of the table. Is used to deduplicate the table.
            columns: Sequence of column names that will be selected from
              the table.
            dedup_sort: Name of a column and sort order tuple to sort the records by within a
              primary key. Values in the column are sorted in descending order,
              so the record with the highest value in `dedup_sort` remains
              after deduplication. No sorting is done if a None value is provided,
              leading to arbitrary deduplication.
            condition: String used as a WHERE clause in the SQL statement to
              filter records. The name of any column that is used in the
              condition but is not part of `columns` must be provided in the
              `condition_columns` argument. No filtering is done (aside from the
              deduplication) if a None value is provided.
            condition_columns: Sequence of names of columns used in the `condition`
              argument. These column names will be selected in the inner subquery
              to make them accessible to the outer WHERE clause. This argument
              should only be used in combination with the `condition` argument.
            skip_dedup: Skips deduplication if data declared deduplicated

        Returns:
            A string representing a SELECT FROM SQL statement where the FROM
            clause represents a deduplicated version of the `table_name` table.

            The returned value is used in two ways:
            1) To select the values for an INSERT INTO statement.
            2) To select the values for a temporary table used for inserts.
        """
        if condition is None:
            condition = "1 = 1"
        col_str = ", ".join(columns)
        inner_col_str = col_str
        if condition_columns is not None:
            inner_col_str += ", " + ", ".join(condition_columns)
        if skip_dedup:
            return f"SELECT {col_str} FROM {table_name} WHERE {condition}"
        else:
            order_by = cls.default_order_by()
            if dedup_sort is not None:
                order_by = f"{dedup_sort[0]} {dedup_sort[1].upper()}"
            return f"""
                SELECT {col_str}
                    FROM (
                        SELECT ROW_NUMBER() OVER (partition BY {", ".join(primary_keys)} ORDER BY {order_by}) AS _dlt_dedup_rn, {inner_col_str}
                        FROM {table_name}
                    ) AS _dlt_dedup_numbered WHERE _dlt_dedup_rn = 1 AND ({condition})

        """

    @classmethod
    def default_order_by(cls) -> str:
        return "(SELECT NULL)"

    @classmethod
    def gen_insert_temp_table_sql(
        cls,
        table_name: str,
        staging_root_table_name: str,
        sql_client: SqlClientBase[Any],
        primary_keys: Sequence[str],
        unique_column: str,
        dedup_sort: Tuple[str, TSortOrder] = None,
        condition: str = None,
        condition_columns: Sequence[str] = None,
        skip_dedup: bool = False,
    ) -> Tuple[List[str], str]:
        temp_table_name = cls._new_temp_table_name(table_name, "insert", sql_client)
        if len(primary_keys) > 0:
            # deduplicate
            select_sql = cls.gen_select_from_dedup_sql(
                staging_root_table_name,
                primary_keys,
                [unique_column],
                dedup_sort,
                condition,
                condition_columns,
                skip_dedup,
            )
        else:
            # don't deduplicate
            select_sql = f"SELECT {unique_column} FROM {staging_root_table_name} WHERE {condition}"
        return [cls._to_temp_table(select_sql, temp_table_name, unique_column)], temp_table_name

    @classmethod
    def gen_delete_from_sql(
        cls,
        table_name: str,
        unique_column: str,
        delete_temp_table_name: str,
        temp_table_column: str,
    ) -> str:
        """Generate DELETE FROM statement deleting the records found in the deletes temp table."""
        return f"""DELETE FROM {table_name}
            WHERE {unique_column} IN (
                SELECT * FROM {delete_temp_table_name}
            );
        """

    @classmethod
    def gen_concat_sql(cls, columns: Sequence[str]) -> str:
        return f"CONCAT({', '.join(columns)})"

    @classmethod
    def _shorten_table_name(cls, ident: str, sql_client: SqlClientBase[Any]) -> str:
        """Trims identifier to max length supported by sql_client. Used for dynamically constructed table names"""
        from dlt.common.normalizers.naming import NamingConvention

        return NamingConvention.shorten_identifier(
            ident, ident, sql_client.capabilities.max_identifier_length
        )

    @classmethod
    def _new_temp_table_name(cls, table_name: str, op: str, sql_client: SqlClientBase[Any]) -> str:
        return cls._shorten_table_name(f"{table_name}_{op}_{uniq_id()}", sql_client)

    @classmethod
    def _to_temp_table(cls, select_sql: str, temp_table_name: str, unique_column: str) -> str:
        """Generate sql that creates temp table from select statement. May return several statements.

        Args:
            select_sql: select statement to create temp table from
            temp_table_name: name of the temp table (unqualified)
            unique_column: column in the select list that is unique. used by Clickhouse only

        Returns:
            sql statement that inserts data from selects into temp table
        """
        return f"CREATE TEMPORARY TABLE {temp_table_name} AS {select_sql}"

    @classmethod
    def gen_update_table_prefix(cls, table_name: str) -> str:
        return f"UPDATE {table_name} SET"

    @classmethod
    def requires_temp_table_for_delete(cls) -> bool:
        """Whether a temporary table is required to delete records.

        Must be `True` for destinations that don't support correlated subqueries.
        """
        return False

    @classmethod
    def _escape_list(cls, list_: List[str], escape_id: Callable[[str], str]) -> List[str]:
        return list(map(escape_id, list_))

    @classmethod
    def _get_hard_delete_col_and_cond(
        cls,
        table: PreparedTableSchema,
        escape_id: Callable[[str], str],
        escape_lit: Callable[[Any], Any],
        invert: bool = False,
    ) -> Tuple[Optional[str], Optional[str]]:
        """Returns tuple of hard delete column name and SQL condition statement.

        Returns tuple of `None` values if no column has `hard_delete` hint.
        Condition statement can be used to filter deleted records.
        Set `invert=True` to filter non-deleted records instead.
        """

        col = get_first_column_name_with_prop(table, "hard_delete")
        if col is None:
            return (None, None)
        cond = f"{escape_id(col)} IS NOT NULL"
        if invert:
            cond = f"{escape_id(col)} IS NULL"
        if table["columns"][col]["data_type"] == "bool":
            if invert:
                cond += f" OR {escape_id(col)} = {escape_lit(False)}"
            else:
                cond = f"{escape_id(col)} = {escape_lit(True)}"
        return (col, cond)

    @classmethod
    def get_row_key_col(
        cls,
        table_chain: Sequence[PreparedTableSchema],
        table: PreparedTableSchema,
        dataset_name: str,
        staging_dataset_name: str,
    ) -> str:
        """Returns name of first column in `table` with `row_key` property. If not found first `unique` hint will be used.
        If no `unique` columns exist, will attempt to use a single primary key column.

        Returns:
            str: Name of the column to be used as row key

        Raises:
            MergeDispositionException: If no suitable column is found based on the search criteria
        """
        col = get_first_column_name_with_prop(table, "row_key")
        if col is not None:
            return col

        col = get_first_column_name_with_prop(table, "unique")
        if col is not None:
            return col

        # Try to use a single primary key column as a fallback
        primary_key_cols = get_columns_names_with_prop(table, "primary_key")
        if len(primary_key_cols) == 1:
            return primary_key_cols[0]
        elif len(primary_key_cols) > 1:
            raise MergeDispositionException(
                dataset_name,
                staging_dataset_name,
                [t["name"] for t in table_chain],
                f"Multiple primary key columns found in table `{table['name']}`. "
                "Cannot use as `row_key`.",
            )

        raise MergeDispositionException(
            dataset_name,
            staging_dataset_name,
            [t["name"] for t in table_chain],
            "No `row_key`, `unique`, or single primary key column (e.g. `_dlt_id`) "
            f"in table `{table['name']}`.",
        )

    @classmethod
    def get_root_key_col(
        cls,
        table_chain: Sequence[PreparedTableSchema],
        table: PreparedTableSchema,
        dataset_name: str,
        staging_dataset_name: str,
    ) -> str:
        """Returns name of first column in `table` with `root_key` property.

        Raises `MergeDispositionException` if no such column exists.
        """
        return cls._get_prop_col_or_raise(
            table,
            "root_key",
            MergeDispositionException(
                dataset_name,
                staging_dataset_name,
                [t["name"] for t in table_chain],
                f"No `root_key` column (e.g. `_dlt_root_id`) in table `{table['name']}`.",
            ),
        )

    @classmethod
    def _get_prop_col_or_raise(
        cls, table: PreparedTableSchema, prop: Union[TColumnProp, str], exception: Exception
    ) -> str:
        """Returns name of first column in `table` with `prop` property.

        Raises `exception` if no such column exists.
        """
        col = get_first_column_name_with_prop(table, prop)
        if col is None:
            raise exception
        return col

    @classmethod
    def gen_merge_sql(
        cls, table_chain: Sequence[PreparedTableSchema], sql_client: SqlClientBase[Any]
    ) -> List[str]:
        """Generates a list of sql statements that merge the data in staging dataset with the data in destination dataset.

        The `table_chain` contains a list schemas of a tables with row_key - parent_key nested reference, ordered by the ancestry (the root of the tree is first on the list).
        The root table is merged using primary_key and merge_key hints which can be compound and be both specified. In that case the OR clause is generated.
        The nested tables are merged based on propagated `root_key` which is a type of foreign key but always leading to a root table.

        First we store the root_keys of root table elements to be deleted in the temp table. Then we use the temp table to delete records from root and all netsed tables in the destination dataset.
        At the end we copy the data from the staging dataset into destination dataset.

        If a hard_delete column is specified, records flagged as deleted will be excluded from the copy into the destination dataset.
        If a dedup_sort column is specified in conjunction with a primary key, records will be sorted before deduplication, so the "latest" record remains.
        """
        sql: List[str] = []
        root_table = table_chain[0]

        escape_column_id = sql_client.escape_column_name
        escape_lit = sql_client.capabilities.escape_literal
        if escape_lit is None:
            escape_lit = DestinationCapabilitiesContext.generic_capabilities().escape_literal

        # get top level table full identifiers
        root_table_name, staging_root_table_name = sql_client.get_qualified_table_names(
            root_table["name"]
        )

        # get merge and primary keys from top level
        primary_keys = cls._escape_list(
            get_columns_names_with_prop(root_table, "primary_key"),
            escape_column_id,
        )
        merge_keys = cls._escape_list(
            get_columns_names_with_prop(root_table, "merge_key"),
            escape_column_id,
        )

        # if we do not have any merge keys to select from, we will fall back to a staged append, i.E.
        # just skip the delete part
        append_fallback = (len(primary_keys) + len(merge_keys)) == 0

        if not append_fallback:
            key_clauses = cls._gen_key_table_clauses(primary_keys, merge_keys)

            row_key_column: str = None
            root_key_column: str = None

            if len(table_chain) == 1 and not cls.requires_temp_table_for_delete():
                key_table_clauses = cls.gen_key_table_clauses(
                    root_table_name, staging_root_table_name, key_clauses, for_delete=True
                )
                # if no nested tables, just delete data from root table
                for clause in key_table_clauses:
                    sql.append(f"DELETE {clause}")
            else:
                key_table_clauses = cls.gen_key_table_clauses(
                    root_table_name, staging_root_table_name, key_clauses, for_delete=False
                )
                # use row_key or unique hint to create temp table with all identifiers to delete
                row_key_column = escape_column_id(
                    cls.get_row_key_col(
                        table_chain,
                        root_table,
                        sql_client.fully_qualified_dataset_name(),
                        sql_client.fully_qualified_dataset_name(staging=True),
                    )
                )
                create_delete_temp_table_sql, delete_temp_table_name = (
                    cls.gen_delete_temp_table_sql(
                        root_table["name"], row_key_column, key_table_clauses, sql_client
                    )
                )
                sql.extend(create_delete_temp_table_sql)

                # delete from nested tables first. This is important for databricks which does not support temporary tables,
                # but uses temporary views instead
                for table in table_chain[1:]:
                    table_name = sql_client.make_qualified_table_name(table["name"])
                    root_key_column = escape_column_id(
                        cls.get_root_key_col(
                            table_chain,
                            table,
                            sql_client.fully_qualified_dataset_name(),
                            sql_client.fully_qualified_dataset_name(staging=True),
                        )
                    )
                    sql.append(
                        cls.gen_delete_from_sql(
                            table_name, root_key_column, delete_temp_table_name, row_key_column
                        )
                    )

                # delete from root table now that nested tables have been processed
                sql.append(
                    cls.gen_delete_from_sql(
                        root_table_name, row_key_column, delete_temp_table_name, row_key_column
                    )
                )

        # get hard delete information
        hard_delete_col, not_deleted_cond = cls._get_hard_delete_col_and_cond(
            root_table,
            escape_column_id,
            escape_lit,
            invert=True,
        )

        # get dedup sort information
        dedup_sort = get_dedup_sort_tuple(root_table)
        skip_dedup: bool = root_table.get("x-stage-data-deduplicated", False)  # type: ignore[assignment]

        insert_temp_table_name: str = None
        if len(table_chain) > 1:
            if len(primary_keys) > 0 or hard_delete_col is not None:
                # condition_columns = [hard_delete_col] if not_deleted_cond is not None else None
                condition_columns = None if hard_delete_col is None else [hard_delete_col]
                (
                    create_insert_temp_table_sql,
                    insert_temp_table_name,
                ) = cls.gen_insert_temp_table_sql(
                    root_table["name"],
                    staging_root_table_name,
                    sql_client,
                    primary_keys,
                    row_key_column,
                    dedup_sort,
                    not_deleted_cond,
                    condition_columns,
                    skip_dedup=skip_dedup,
                )
                sql.extend(create_insert_temp_table_sql)

        # insert from staging to dataset
        for table in table_chain:
            table_name, staging_table_name = sql_client.get_qualified_table_names(table["name"])

            insert_cond = not_deleted_cond if hard_delete_col is not None else "1 = 1"
            if (len(primary_keys) > 0 and len(table_chain) > 1) or (
                len(primary_keys) == 0
                and is_nested_table(table)  # nested table
                and hard_delete_col is not None
            ):
                uniq_column = root_key_column if is_nested_table(table) else row_key_column
                insert_cond = f"{uniq_column} IN (SELECT * FROM {insert_temp_table_name})"

            columns = list(map(escape_column_id, get_columns_names_with_prop(table, "name")))
            col_str = ", ".join(columns)
            select_sql = f"SELECT {col_str} FROM {staging_table_name} WHERE {insert_cond}"
            if len(primary_keys) > 0 and len(table_chain) == 1:
                # without nested tables we deduplicate inside the query instead of using a temp table
                select_sql = cls.gen_select_from_dedup_sql(
                    staging_table_name,
                    primary_keys,
                    columns,
                    dedup_sort,
                    insert_cond,
                    skip_dedup=skip_dedup,
                )

            sql.append(f"INSERT INTO {table_name}({col_str}) {select_sql}")
        return sql

    @classmethod
    def gen_upsert_sql(
        cls, table_chain: Sequence[PreparedTableSchema], sql_client: SqlClientBase[Any]
    ) -> List[str]:
        sql: List[str] = []
        root_table = table_chain[0]
        root_table_name, staging_root_table_name = sql_client.get_qualified_table_names(
            root_table["name"]
        )
        escape_column_id = sql_client.escape_column_name
        escape_lit = sql_client.capabilities.escape_literal
        if escape_lit is None:
            escape_lit = DestinationCapabilitiesContext.generic_capabilities().escape_literal

        # process table hints
        primary_keys = cls._escape_list(
            get_columns_names_with_prop(root_table, "primary_key"),
            escape_column_id,
        )
        hard_delete_col, deleted_cond = cls._get_hard_delete_col_and_cond(
            root_table,
            escape_column_id,
            escape_lit,
        )

        # generate merge statement for root table
        on_str = " AND ".join([f"d.{c} = s.{c}" for c in primary_keys])
        root_table_column_names = list(map(escape_column_id, root_table["columns"]))
        update_str = ", ".join([c + " = " + "s." + c for c in root_table_column_names])
        col_str = ", ".join(["{alias}" + c for c in root_table_column_names])
        delete_str = (
            "" if hard_delete_col is None else f"WHEN MATCHED AND s.{deleted_cond} THEN DELETE"
        )

        sql.append(f"""
            MERGE INTO {root_table_name} d USING {staging_root_table_name} s
            ON {on_str}
            {delete_str}
            WHEN MATCHED
                THEN UPDATE SET {update_str}
            WHEN NOT MATCHED
                THEN INSERT ({col_str.format(alias="")}) VALUES ({col_str.format(alias="s.")});
        """)

        # generate statements for nested tables if they exist
        nested_tables = table_chain[1:]
        if nested_tables:
            root_row_key_column = escape_column_id(
                cls.get_row_key_col(
                    table_chain,
                    root_table,
                    sql_client.fully_qualified_dataset_name(),
                    sql_client.fully_qualified_dataset_name(staging=True),
                )
            )
            for table in nested_tables:
                nested_row_key_column = escape_column_id(
                    cls.get_row_key_col(
                        table_chain,
                        table,
                        sql_client.fully_qualified_dataset_name(),
                        sql_client.fully_qualified_dataset_name(staging=True),
                    )
                )
                root_key_column = escape_column_id(
                    cls.get_root_key_col(
                        table_chain,
                        table,
                        sql_client.fully_qualified_dataset_name(),
                        sql_client.fully_qualified_dataset_name(staging=True),
                    )
                )
                table_name, staging_table_name = sql_client.get_qualified_table_names(table["name"])

                # delete records for elements no longer in the list
                sql.append(f"""
                    DELETE FROM {table_name}
                    WHERE {root_key_column} IN (SELECT {root_row_key_column} FROM {staging_root_table_name})
                    AND {nested_row_key_column} NOT IN (SELECT {nested_row_key_column} FROM {staging_table_name});
                """)

                # insert records for new elements in the list
                table_column_names = list(map(escape_column_id, table["columns"]))
                update_str = ", ".join([c + " = " + "s." + c for c in table_column_names])
                col_str = ", ".join(["{alias}" + c for c in table_column_names])
                sql.append(f"""
                    MERGE INTO {table_name} d USING {staging_table_name} s
                    ON d.{nested_row_key_column} = s.{nested_row_key_column}
                    WHEN MATCHED
                        THEN UPDATE SET {update_str}
                    WHEN NOT MATCHED
                        THEN INSERT ({col_str.format(alias="")}) VALUES ({col_str.format(alias="s.")});
                """)

                # delete hard-deleted records
                if hard_delete_col is not None:
                    sql.append(f"""
                        DELETE FROM {table_name}
                        WHERE {root_key_column} IN (
                            SELECT {root_row_key_column}
                            FROM {staging_root_table_name}
                            WHERE {deleted_cond}
                        );
                    """)
        return sql

    @classmethod
    def gen_scd2_sql(
        cls, table_chain: Sequence[PreparedTableSchema], sql_client: SqlClientBase[Any]
    ) -> List[str]:
        """Generates SQL statements for the `scd2` merge strategy.

        The root table can be inserted into and updated.
        Updates only take place when a record retires (because there is a new version
        or it is deleted) and only affect the "valid to" column.
        Nested tables are insert-only.
        """
        sql: List[str] = []
        root_table = table_chain[0]
        root_table_name, staging_root_table_name = sql_client.get_qualified_table_names(
            root_table["name"]
        )

        # get column names
        caps = sql_client.capabilities
        escape_column_id = sql_client.escape_column_name
        from_, to = list(
            map(escape_column_id, get_validity_column_names(root_table))
        )  # validity columns
        hash_ = escape_column_id(
            get_first_column_name_with_prop(root_table, "x-row-version")
        )  # row hash column

        # define values for validity columns
        format_datetime_literal = caps.format_datetime_literal
        if format_datetime_literal is None:
            format_datetime_literal = (
                DestinationCapabilitiesContext.generic_capabilities().format_datetime_literal
            )

        boundary_ts = ensure_pendulum_datetime(
            root_table.get(  # type: ignore[arg-type]
                "x-boundary-timestamp",
                current_load_package()["state"]["created_at"],
            )
        )
        boundary_literal = format_datetime_literal(
            boundary_ts,
            caps.timestamp_precision,
        )

        active_record_timestamp = get_active_record_timestamp(root_table)
        if active_record_timestamp is None:
            active_record_literal = "NULL"
            is_active = f"{to} IS NULL"
        else:  # it's a datetime
            active_record_literal = format_datetime_literal(
                active_record_timestamp, caps.timestamp_precision
            )
            is_active = f"{to} = {active_record_literal}"

        # retire records:
        # - no `merge_key`: retire all absent records
        # - yes `merge_key`: retire those absent records whose `merge_key`
        # is present in staging data
        retire_sql = f"""
            {cls.gen_update_table_prefix(root_table_name)} {to} = {boundary_literal}
            WHERE {is_active}
            AND {hash_} NOT IN (SELECT {hash_} FROM {staging_root_table_name});
        """
        merge_keys = cls._escape_list(
            get_columns_names_with_prop(root_table, "merge_key"),
            escape_column_id,
        )
        if len(merge_keys) > 0:
            if len(merge_keys) == 1:
                key = merge_keys[0]
            else:
                key = cls.gen_concat_sql(merge_keys)  # compound key
            key_present = f"{key} IN (SELECT {key} FROM {staging_root_table_name})"
            retire_sql = retire_sql.rstrip()[:-1]  # remove semicolon
            retire_sql += f" AND {key_present}"
        sql.append(retire_sql)

        # insert new active records in root table
        columns = map(escape_column_id, list(root_table["columns"].keys()))
        col_str = ", ".join([c for c in columns if c not in (from_, to)])
        sql.append(f"""
            INSERT INTO {root_table_name} ({col_str}, {from_}, {to})
            SELECT {col_str}, {boundary_literal} AS {from_}, {active_record_literal} AS {to}
            FROM {staging_root_table_name} AS s
            WHERE {hash_} NOT IN (SELECT {hash_} FROM {root_table_name} WHERE {is_active});
        """)

        # insert list elements for new active records in nested tables
        nested_tables = table_chain[1:]
        if nested_tables:
            # TODO: - based on deterministic nested hashes (OK)
            # - if row hash changes all is right
            # - if it does not we only capture new records, while we should replace existing with those in stage
            # - this write disposition is way more similar to regular merge (how root tables are handled is different, other tables handled same)
            for table in nested_tables:
                row_key_column = escape_column_id(
                    cls.get_row_key_col(
                        table_chain,
                        table,
                        sql_client.fully_qualified_dataset_name(),
                        sql_client.fully_qualified_dataset_name(staging=True),
                    )
                )
                table_name, staging_table_name = sql_client.get_qualified_table_names(table["name"])
                sql.append(f"""
                    INSERT INTO {table_name}
                    SELECT *
                    FROM {staging_table_name}
                    WHERE {row_key_column} NOT IN (SELECT {row_key_column} FROM {table_name});
                """)

        return sql
