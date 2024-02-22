from typing import Any, Callable, List, Sequence, Tuple, cast, TypedDict, Optional

import yaml
from dlt.common.runtime.logger import pretty_format_exception

from dlt.common.schema.typing import TTableSchema, TSortOrder
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    get_first_column_name_with_prop,
    get_dedup_sort_tuple,
)
from dlt.common.storages.load_storage import ParsedLoadJobFileName
from dlt.common.utils import uniq_id
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.destinations.exceptions import MergeDispositionException
from dlt.destinations.job_impl import NewLoadJobImpl
from dlt.destinations.sql_client import SqlClientBase


class SqlJobParams(TypedDict):
    replace: Optional[bool]


DEFAULTS: SqlJobParams = {"replace": False}


class SqlBaseJob(NewLoadJobImpl):
    """Sql base job for jobs that rely on the whole tablechain"""

    failed_text: str = ""

    @classmethod
    def from_table_chain(
        cls,
        table_chain: Sequence[TTableSchema],
        sql_client: SqlClientBase[Any],
        params: Optional[SqlJobParams] = None,
    ) -> NewLoadJobImpl:
        """Generates a list of sql statements, that will be executed by the sql client when the job is executed in the loader.

        The `table_chain` contains a list schemas of a tables with parent-child relationship, ordered by the ancestry (the root of the tree is first on the list).
        """
        params = cast(SqlJobParams, {**DEFAULTS, **(params or {})})  # type: ignore
        top_table = table_chain[0]
        file_info = ParsedLoadJobFileName(
            top_table["name"], ParsedLoadJobFileName.new_file_id(), 0, "sql"
        )
        try:
            # Remove line breaks from multiline statements and write one SQL statement per line in output file
            # to support clients that need to execute one statement at a time (i.e. snowflake)
            sql = [
                " ".join(stmt.splitlines())
                for stmt in cls.generate_sql(table_chain, sql_client, params)
            ]
            job = cls(file_info.file_name(), "running")
            job._save_text_file("\n".join(sql))
        except Exception:
            # return failed job
            tables_str = yaml.dump(
                table_chain, allow_unicode=True, default_flow_style=False, sort_keys=False
            )
            job = cls(file_info.file_name(), "failed", pretty_format_exception())
            job._save_text_file("\n".join([cls.failed_text, tables_str]))
        return job

    @classmethod
    def generate_sql(
        cls,
        table_chain: Sequence[TTableSchema],
        sql_client: SqlClientBase[Any],
        params: Optional[SqlJobParams] = None,
    ) -> List[str]:
        pass


class SqlStagingCopyJob(SqlBaseJob):
    """Generates a list of sql statements that copy the data from staging dataset into destination dataset."""

    failed_text: str = "Tried to generate a staging copy sql job for the following tables:"

    @classmethod
    def _generate_clone_sql(
        cls,
        table_chain: Sequence[TTableSchema],
        sql_client: SqlClientBase[Any],
    ) -> List[str]:
        """Drop and clone the table for supported destinations"""
        sql: List[str] = []
        for table in table_chain:
            with sql_client.with_staging_dataset(staging=True):
                staging_table_name = sql_client.make_qualified_table_name(table["name"])
            table_name = sql_client.make_qualified_table_name(table["name"])
            sql.append(f"DROP TABLE IF EXISTS {table_name};")
            # recreate destination table with data cloned from staging table
            sql.append(f"CREATE TABLE {table_name} CLONE {staging_table_name};")
        return sql

    @classmethod
    def _generate_insert_sql(
        cls,
        table_chain: Sequence[TTableSchema],
        sql_client: SqlClientBase[Any],
        params: SqlJobParams = None,
    ) -> List[str]:
        sql: List[str] = []
        for table in table_chain:
            with sql_client.with_staging_dataset(staging=True):
                staging_table_name = sql_client.make_qualified_table_name(table["name"])
            table_name = sql_client.make_qualified_table_name(table["name"])
            columns = ", ".join(
                map(
                    sql_client.capabilities.escape_identifier,
                    get_columns_names_with_prop(table, "name"),
                )
            )
            if params["replace"]:
                sql.append(sql_client._truncate_table_sql(table_name))
            sql.append(
                f"INSERT INTO {table_name}({columns}) SELECT {columns} FROM {staging_table_name};"
            )
        return sql

    @classmethod
    def generate_sql(
        cls,
        table_chain: Sequence[TTableSchema],
        sql_client: SqlClientBase[Any],
        params: SqlJobParams = None,
    ) -> List[str]:
        if params["replace"] and sql_client.capabilities.supports_clone_table:
            return cls._generate_clone_sql(table_chain, sql_client)
        return cls._generate_insert_sql(table_chain, sql_client, params)


class SqlMergeJob(SqlBaseJob):
    """Generates a list of sql statements that merge the data from staging dataset into destination dataset."""

    failed_text: str = "Tried to generate a merge sql job for the following tables:"

    @classmethod
    def generate_sql(
        cls,
        table_chain: Sequence[TTableSchema],
        sql_client: SqlClientBase[Any],
        params: Optional[SqlJobParams] = None,
    ) -> List[str]:
        """Generates a list of sql statements that merge the data in staging dataset with the data in destination dataset.

        The `table_chain` contains a list schemas of a tables with parent-child relationship, ordered by the ancestry (the root of the tree is first on the list).
        The root table is merged using primary_key and merge_key hints which can be compound and be both specified. In that case the OR clause is generated.
        The child tables are merged based on propagated `root_key` which is a type of foreign key but always leading to a root table.

        First we store the root_keys of root table elements to be deleted in the temp table. Then we use the temp table to delete records from root and all child tables in the destination dataset.
        At the end we copy the data from the staging dataset into destination dataset.

        If a hard_delete column is specified, records flagged as deleted will be excluded from the copy into the destination dataset.
        If a dedup_sort column is specified in conjunction with a primary key, records will be sorted before deduplication, so the "latest" record remains.
        """
        return cls.gen_merge_sql(table_chain, sql_client)

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
        cls, unique_column: str, key_table_clauses: Sequence[str]
    ) -> Tuple[List[str], str]:
        """Generate sql that creates delete temp table and inserts `unique_column` from root table for all records to delete. May return several statements.

        Returns temp table name for cases where special names are required like SQLServer.
        """
        sql: List[str] = []
        temp_table_name = cls._new_temp_table_name("delete")
        select_statement = f"SELECT d.{unique_column} {key_table_clauses[0]}"
        sql.append(cls._to_temp_table(select_statement, temp_table_name))
        for clause in key_table_clauses[1:]:
            sql.append(f"INSERT INTO {temp_table_name} SELECT {unique_column} {clause};")
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
            sort_column: Name of a column to sort the records by within a
              primary key. Values in the column are sorted in descending order,
              so the record with the highest value in `sort_column` remains
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

        Returns:
            A string representing a SELECT FROM SQL statement where the FROM
            clause represents a deduplicated version of the `table_name` table.

            The returned value is used in two ways:
            1) To select the values for an INSERT INTO statement.
            2) To select the values for a temporary table used for inserts.
        """
        order_by = "(SELECT NULL)"
        if dedup_sort is not None:
            order_by = f"{dedup_sort[0]} {dedup_sort[1].upper()}"
        if condition is None:
            condition = "1 = 1"
        col_str = ", ".join(columns)
        inner_col_str = col_str
        if condition_columns is not None:
            inner_col_str += ", " + ", ".join(condition_columns)
        return f"""
            SELECT {col_str}
                FROM (
                    SELECT ROW_NUMBER() OVER (partition BY {", ".join(primary_keys)} ORDER BY {order_by}) AS _dlt_dedup_rn, {inner_col_str}
                    FROM {table_name}
                ) AS _dlt_dedup_numbered WHERE _dlt_dedup_rn = 1 AND ({condition})
        """

    @classmethod
    def gen_insert_temp_table_sql(
        cls,
        staging_root_table_name: str,
        primary_keys: Sequence[str],
        unique_column: str,
        dedup_sort: Tuple[str, TSortOrder] = None,
        condition: str = None,
        condition_columns: Sequence[str] = None,
    ) -> Tuple[List[str], str]:
        temp_table_name = cls._new_temp_table_name("insert")
        if len(primary_keys) > 0:
            # deduplicate
            select_sql = cls.gen_select_from_dedup_sql(
                staging_root_table_name,
                primary_keys,
                [unique_column],
                dedup_sort,
                condition,
                condition_columns,
            )
        else:
            # don't deduplicate
            select_sql = f"SELECT {unique_column} FROM {staging_root_table_name} WHERE {condition}"
        return [cls._to_temp_table(select_sql, temp_table_name)], temp_table_name

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
    def _new_temp_table_name(cls, name_prefix: str) -> str:
        return f"{name_prefix}_{uniq_id()}"

    @classmethod
    def _to_temp_table(cls, select_sql: str, temp_table_name: str) -> str:
        """Generate sql that creates temp table from select statement. May return several statements.

        Args:
            select_sql: select statement to create temp table from
            temp_table_name: name of the temp table (unqualified)

        Returns:
            sql statement that inserts data from selects into temp table
        """
        return f"CREATE TEMP TABLE {temp_table_name} AS {select_sql};"

    @classmethod
    def gen_merge_sql(
        cls, table_chain: Sequence[TTableSchema], sql_client: SqlClientBase[Any]
    ) -> List[str]:
        sql: List[str] = []
        root_table = table_chain[0]

        escape_id = sql_client.capabilities.escape_identifier
        escape_lit = sql_client.capabilities.escape_literal
        if escape_id is None:
            escape_id = DestinationCapabilitiesContext.generic_capabilities().escape_identifier
        if escape_lit is None:
            escape_lit = DestinationCapabilitiesContext.generic_capabilities().escape_literal

        # get top level table full identifiers
        root_table_name = sql_client.make_qualified_table_name(root_table["name"])
        with sql_client.with_staging_dataset(staging=True):
            staging_root_table_name = sql_client.make_qualified_table_name(root_table["name"])
        # get merge and primary keys from top level
        primary_keys = list(
            map(
                escape_id,
                get_columns_names_with_prop(root_table, "primary_key"),
            )
        )
        merge_keys = list(
            map(
                escape_id,
                get_columns_names_with_prop(root_table, "merge_key"),
            )
        )
        key_clauses = cls._gen_key_table_clauses(primary_keys, merge_keys)

        unique_column: str = None
        root_key_column: str = None

        if len(table_chain) == 1:
            key_table_clauses = cls.gen_key_table_clauses(
                root_table_name, staging_root_table_name, key_clauses, for_delete=True
            )
            # if no child tables, just delete data from top table
            for clause in key_table_clauses:
                sql.append(f"DELETE {clause};")
        else:
            key_table_clauses = cls.gen_key_table_clauses(
                root_table_name, staging_root_table_name, key_clauses, for_delete=False
            )
            # use unique hint to create temp table with all identifiers to delete
            unique_columns = get_columns_names_with_prop(root_table, "unique")
            if not unique_columns:
                raise MergeDispositionException(
                    sql_client.fully_qualified_dataset_name(),
                    staging_root_table_name,
                    [t["name"] for t in table_chain],
                    f"There is no unique column (ie _dlt_id) in top table {root_table['name']} so"
                    " it is not possible to link child tables to it.",
                )
            # get first unique column
            unique_column = escape_id(unique_columns[0])
            # create temp table with unique identifier
            create_delete_temp_table_sql, delete_temp_table_name = cls.gen_delete_temp_table_sql(
                unique_column, key_table_clauses
            )
            sql.extend(create_delete_temp_table_sql)

            # delete from child tables first. This is important for databricks which does not support temporary tables,
            # but uses temporary views instead
            for table in table_chain[1:]:
                table_name = sql_client.make_qualified_table_name(table["name"])
                root_key_columns = get_columns_names_with_prop(table, "root_key")
                if not root_key_columns:
                    raise MergeDispositionException(
                        sql_client.fully_qualified_dataset_name(),
                        staging_root_table_name,
                        [t["name"] for t in table_chain],
                        "There is no root foreign key (ie _dlt_root_id) in child table"
                        f" {table['name']} so it is not possible to refer to top level table"
                        f" {root_table['name']} unique column {unique_column}",
                    )
                root_key_column = escape_id(root_key_columns[0])
                sql.append(
                    cls.gen_delete_from_sql(
                        table_name, root_key_column, delete_temp_table_name, unique_column
                    )
                )

            # delete from top table now that child tables have been prcessed
            sql.append(
                cls.gen_delete_from_sql(
                    root_table_name, unique_column, delete_temp_table_name, unique_column
                )
            )

        # get name of column with hard_delete hint, if specified
        not_deleted_cond: str = None
        hard_delete_col = get_first_column_name_with_prop(root_table, "hard_delete")
        if hard_delete_col is not None:
            # any value indicates a delete for non-boolean columns
            not_deleted_cond = f"{escape_id(hard_delete_col)} IS NULL"
            if root_table["columns"][hard_delete_col]["data_type"] == "bool":
                # only True values indicate a delete for boolean columns
                not_deleted_cond += f" OR {escape_id(hard_delete_col)} = {escape_lit(False)}"

        # get dedup sort information
        dedup_sort = get_dedup_sort_tuple(root_table)

        insert_temp_table_name: str = None
        if len(table_chain) > 1:
            if len(primary_keys) > 0 or hard_delete_col is not None:
                condition_columns = [hard_delete_col] if not_deleted_cond is not None else None
                (
                    create_insert_temp_table_sql,
                    insert_temp_table_name,
                ) = cls.gen_insert_temp_table_sql(
                    staging_root_table_name,
                    primary_keys,
                    unique_column,
                    dedup_sort,
                    not_deleted_cond,
                    condition_columns,
                )
                sql.extend(create_insert_temp_table_sql)

        # insert from staging to dataset
        for table in table_chain:
            table_name = sql_client.make_qualified_table_name(table["name"])
            with sql_client.with_staging_dataset(staging=True):
                staging_table_name = sql_client.make_qualified_table_name(table["name"])

            insert_cond = not_deleted_cond if hard_delete_col is not None else "1 = 1"
            if (len(primary_keys) > 0 and len(table_chain) > 1) or (
                len(primary_keys) == 0
                and table.get("parent") is not None  # child table
                and hard_delete_col is not None
            ):
                uniq_column = unique_column if table.get("parent") is None else root_key_column
                insert_cond = f"{uniq_column} IN (SELECT * FROM {insert_temp_table_name})"

            columns = list(map(escape_id, get_columns_names_with_prop(table, "name")))
            col_str = ", ".join(columns)
            select_sql = f"SELECT {col_str} FROM {staging_table_name} WHERE {insert_cond}"
            if len(primary_keys) > 0 and len(table_chain) == 1:
                # without child tables we deduplicate inside the query instead of using a temp table
                select_sql = cls.gen_select_from_dedup_sql(
                    staging_table_name, primary_keys, columns, dedup_sort, insert_cond
                )

            sql.append(f"INSERT INTO {table_name}({col_str}) {select_sql};")
        return sql
