from typing import Any, List, Sequence, cast

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
        top_table = table_chain[0]
        file_info = ParsedLoadJobFileName(top_table["name"], uniq_id()[:10], 0, "sql")
        try:
            sql = SqlMergeJob._gen_sql(table_chain, sql_client)
            job = cls(file_info.job_id(), "running")
            print("\n".join(sql))
            job._save_text_file("\n".join(sql))
        except Exception:
            # return failed job
            failed_text = "Tried to generate a merge sql job for the following tables:"
            tables_str = cast(str, yaml.dump(table_chain, allow_unicode=True, default_flow_style=False, sort_keys=False))
            job = cls(file_info.job_id(), "failed", pretty_format_exception())
            job._save_text_file("\n".join([failed_text, tables_str]))
        return job

    @staticmethod
    def _gen_sql(table_chain: Sequence[TTableSchema], sql_client: SqlClientBase[Any]) -> List[str]:
        sql: List[str] = []
        top_table = table_chain[0]

        # get top level table full identifiers
        top_table_name = sql_client.make_qualified_table_name(top_table["name"])
        temp_table_name = f"test_{uniq_id()}"
        with sql_client.with_staging_dataset(staging=True):
            staging_top_table_name = sql_client.make_qualified_table_name(top_table["name"])
        # get merge and primary keys from top level
        primary_keys = get_columns_names_with_prop(top_table, "primary_key")
        merge_keys = get_columns_names_with_prop(top_table, "merge_key")
        overlapped_clause = ""
        if primary_keys or merge_keys:
            if primary_keys:
                overlapped_clause = "WHERE " + " AND ".join([f"data.{c} = staging.{c}" for c in map(sql_client.capabilities.escape_identifier, primary_keys)])
            if merge_keys:
                if not overlapped_clause:
                    overlapped_clause = "WHERE "
                else:
                    overlapped_clause += " OR "
                overlapped_clause += " AND ".join([f"data.{c} = staging.{c}" for c in map(sql_client.capabilities.escape_identifier, merge_keys)])
        select_overlapped = f"FROM {top_table_name} AS data WHERE EXISTS (SELECT 1 FROM {staging_top_table_name} AS staging {overlapped_clause})"
        if len(table_chain) == 1:
            # if no child tables, just delete data from top table
            sql.append(f"DELETE {select_overlapped};")
        else:
            # use unique hint to create temp table with all identifiers to delete
            unique_columns = get_columns_names_with_prop(top_table, "unique")
            if not unique_columns:
                raise MergeDispositionException(
                    sql_client.fully_qualified_dataset_name(),
                    staging_top_table_name,
                    [t["name"] for t in table_chain],
                    f"There is no unique column (ie _dlt_id) in top table {top_table['name']} so it is not possible to link child tables to it."
                )
            # get first unique column
            unique_column = sql_client.capabilities.escape_identifier(unique_columns[0])
            # create temp table with unique identifier
            # sql.append(f"SELECT {unique_column} {select_overlapped};")
            sql.append(f"CREATE TEMP TABLE {temp_table_name} AS SELECT {unique_column} {select_overlapped};")
            # delete top table
            sql.append(f"DELETE FROM {top_table_name} WHERE {unique_column} IN (SELECT * FROM {temp_table_name});")
            # delete other tables
            for table in table_chain[1:]:
                table_name = sql_client.make_qualified_table_name(table["name"])
                root_key_columns = get_columns_names_with_prop(table, "root_key")
                if not root_key_columns:
                    raise MergeDispositionException(
                        sql_client.fully_qualified_dataset_name(),
                        staging_top_table_name,
                        [t["name"] for t in table_chain],
                        f"There is no root foreign key (ie _dlt_root_id) in child table {table['name']} so it is not possible to refer to top level table {top_table['name']} unique column {unique_column}"
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
