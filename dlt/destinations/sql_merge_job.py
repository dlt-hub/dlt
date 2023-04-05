from typing import Any, Callable, List, Sequence, Tuple, cast

import yaml
from dlt.common.runtime.logger import pretty_format_exception

from dlt.common.schema.typing import TTableSchema
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.common.storages.load_storage import ParsedLoadJobFileName
from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import MergeDispositionException
from dlt.destinations.job_impl import NewLoadJobImpl
from dlt.destinations.sql_client import SqlClientBase



class SqlMergeJob(NewLoadJobImpl):

    @classmethod
    def from_table_chain(cls, table_chain: Sequence[TTableSchema], sql_client: SqlClientBase[Any]) -> NewLoadJobImpl:
        """Generates a list of sql statements that merge the data in staging dataset with the data in destination dataset.

        The `table_chain` contains a list schemas of a tables with parent-child relationship, ordered by the ancestry (the root of the tree is first on the list).
        The root table is merged using primary_key and merge_key hints which can be compound and be both specified. In that case the OR clause is generated.
        The child tables are merged based on propagated `root_key` which is a type of foreign key but always leading to a root table.

        First we store the root_keys of root table elements to be deleted in the temp table. Then we use the temp table to delete records from root and all child tables in the destination dataset.
        At the end we copy the data from the staging dataset into destination dataset.
        """
        top_table = table_chain[0]
        file_info = ParsedLoadJobFileName(top_table["name"], uniq_id()[:10], 0, "sql")
        try:
            sql = cls.gen_merge_sql(table_chain, sql_client)
            job = cls(file_info.job_id(), "running")
            job._save_text_file("\n".join(sql))
        except Exception:
            # return failed job
            failed_text = "Tried to generate a merge sql job for the following tables:"
            tables_str = cast(str, yaml.dump(table_chain, allow_unicode=True, default_flow_style=False, sort_keys=False))
            job = cls(file_info.job_id(), "failed", pretty_format_exception())
            job._save_text_file("\n".join([failed_text, tables_str]))
        return job

    @classmethod
    def _gen_key_table_clauses(cls, primary_keys: Sequence[str], merge_keys: Sequence[str], escape_identifier: Callable[[str], str])-> List[str]:
        """Generate sql clauses to select rows to delete via merge and primary key. Return select all clause if no keys defined."""
        clauses: List[str] = []
        if primary_keys or merge_keys:
            if primary_keys:
                clauses.append(" AND ".join([f"data.{c} = staging.{c}" for c in map(escape_identifier, primary_keys)]))
            if merge_keys:
                clauses.append(" AND ".join([f"data.{c} = staging.{c}" for c in map(escape_identifier, merge_keys)]))
        return clauses or ["1=1"]

    @classmethod
    def gen_key_table_clauses(cls, root_table_name: str, staging_root_table_name: str, key_clauses: Sequence[str]) -> List[str]:
        """Generate sql clauses that may be used to select or delete rows in root table of destination dataset

            A list of clauses may be returned for engines that do not support OR in subqueries. Like BigQuery
        """
        return [f"FROM {root_table_name} AS data WHERE EXISTS (SELECT 1 FROM {staging_root_table_name} AS staging WHERE {' OR '.join(key_clauses)})"]

    @classmethod
    def gen_temp_table_sql(cls, unique_column: str, key_table_clauses: Sequence[str]) -> Tuple[List[str], str]:
        """Generate sql that creates the temp table and inserts `unique_column` from root table for all records to delete. May return several statements.

           Returns temp table name for cases where special names are required like SQLServer.
        """
        sql: List[str] = []
        temp_table_name = f"test_{uniq_id()}"
        sql.append(f"CREATE TEMP TABLE {temp_table_name} AS SELECT {unique_column} {key_table_clauses[0]};")
        for clause in key_table_clauses[1:]:
            sql.append(f"INSERT INTO {temp_table_name} SELECT {unique_column} {clause};")
        return sql, temp_table_name

    @classmethod
    def gen_merge_sql(cls, table_chain: Sequence[TTableSchema], sql_client: SqlClientBase[Any]) -> List[str]:
        sql: List[str] = []
        root_table = table_chain[0]

        # get top level table full identifiers
        root_table_name = sql_client.make_qualified_table_name(root_table["name"])
        with sql_client.with_staging_dataset(staging=True):
            staging_root_table_name = sql_client.make_qualified_table_name(root_table["name"])
        # get merge and primary keys from top level
        primary_keys = get_columns_names_with_prop(root_table, "primary_key")
        merge_keys = get_columns_names_with_prop(root_table, "merge_key")
        key_clauses = cls._gen_key_table_clauses(primary_keys, merge_keys, sql_client.capabilities.escape_identifier)
        key_table_clauses = cls.gen_key_table_clauses(root_table_name, staging_root_table_name, key_clauses)
        # select_overlapped =
        if len(table_chain) == 1:
            # if no child tables, just delete data from top table
            for clause in key_table_clauses:
                sql.append(f"DELETE {clause};")
        else:
            # use unique hint to create temp table with all identifiers to delete
            unique_columns = get_columns_names_with_prop(root_table, "unique")
            if not unique_columns:
                raise MergeDispositionException(
                    sql_client.fully_qualified_dataset_name(),
                    staging_root_table_name,
                    [t["name"] for t in table_chain],
                    f"There is no unique column (ie _dlt_id) in top table {root_table['name']} so it is not possible to link child tables to it."
                )
            # get first unique column
            unique_column = sql_client.capabilities.escape_identifier(unique_columns[0])
            # create temp table with unique identifier
            create_table_sql, temp_table_name = cls.gen_temp_table_sql(unique_column, key_table_clauses)
            sql.extend(create_table_sql)
            # delete top table
            sql.append(f"DELETE FROM {root_table_name} WHERE {unique_column} IN (SELECT * FROM {temp_table_name});")
            # delete other tables
            for table in table_chain[1:]:
                table_name = sql_client.make_qualified_table_name(table["name"])
                root_key_columns = get_columns_names_with_prop(table, "root_key")
                if not root_key_columns:
                    raise MergeDispositionException(
                        sql_client.fully_qualified_dataset_name(),
                        staging_root_table_name,
                        [t["name"] for t in table_chain],
                        f"There is no root foreign key (ie _dlt_root_id) in child table {table['name']} so it is not possible to refer to top level table {root_table['name']} unique column {unique_column}"
                    )
                root_key_column = sql_client.capabilities.escape_identifier(root_key_columns[0])
                sql.append(f"DELETE FROM {table_name} WHERE {root_key_column} IN (SELECT * FROM {temp_table_name});")

        # insert from staging to dataset, truncate staging table
        for table in table_chain:
            table_name = sql_client.make_qualified_table_name(table["name"])
            with sql_client.with_staging_dataset(staging=True):
                staging_table_name = sql_client.make_qualified_table_name(table["name"])
            columns = ", ".join(map(sql_client.capabilities.escape_identifier, table["columns"].keys()))
            sql.append(
                f"""INSERT INTO {table_name}({columns})
                SELECT {columns} FROM {staging_table_name};
                """)
            # -- DELETE FROM {staging_table_name} WHERE 1=1;

        return sql
