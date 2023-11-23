import os
import abc
from typing import Any, Iterator, List

from dlt.common.destination.reference import LoadJob, FollowupJob, TLoadJobState
from dlt.common.schema.typing import TTableSchema
from dlt.common.storages import FileStorage
from dlt.common.utils import chunks

from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.job_client_impl import SqlJobClientWithStaging


class InsertValuesLoadJob(LoadJob, FollowupJob):
    def __init__(self, table_name: str, file_path: str, sql_client: SqlClientBase[Any]) -> None:
        super().__init__(FileStorage.get_file_name_from_file_path(file_path))
        self._sql_client = sql_client
        # insert file content immediately
        with self._sql_client.begin_transaction():
            for fragments in self._insert(
                sql_client.make_qualified_table_name(table_name), file_path
            ):
                self._sql_client.execute_fragments(fragments)

    def state(self) -> TLoadJobState:
        # this job is always done
        return "completed"

    def exception(self) -> str:
        # this part of code should be never reached
        raise NotImplementedError()

    def _insert(self, qualified_table_name: str, file_path: str) -> Iterator[List[str]]:
        # WARNING: maximum redshift statement is 16MB https://docs.aws.amazon.com/redshift/latest/dg/c_redshift-sql.html
        # the procedure below will split the inserts into max_query_length // 2 packs
        with FileStorage.open_zipsafe_ro(file_path, "r", encoding="utf-8") as f:
            header = f.readline()
            values_mark = f.readline()
            # properly formatted file has a values marker at the beginning
            assert values_mark == "VALUES\n"

            max_rows = self._sql_client.capabilities.max_rows_per_insert

            insert_sql = []
            while content := f.read(self._sql_client.capabilities.max_query_length // 2):
                # read one more line in order to
                # 1. complete the content which ends at "random" position, not an end line
                # 2. to modify its ending without a need to re-allocating the 8MB of "content"
                until_nl = f.readline()
                # if until next line contains just '\n' try to take another line so we can finish content properly
                # TODO: write test for this case (content ends with ",")
                if until_nl == "\n":
                    until_nl = f.readline()
                until_nl = until_nl.strip("\n")
                # if there was anything left, until_nl contains the last line
                is_eof = len(until_nl) == 0 or until_nl[-1] == ";"
                if not is_eof:
                    # print(f'replace the "," with " {until_nl} {len(insert_sql)}')
                    until_nl = until_nl[:-1] + ";"

                if max_rows is not None:
                    # mssql has a limit of 1000 rows per INSERT, so we need to split into separate statements
                    values_rows = content.splitlines(keepends=True)
                    len_rows = len(values_rows)
                    processed = 0
                    # Chunk by max_rows - 1 for simplicity because one more row may be added
                    for chunk in chunks(values_rows, max_rows - 1):
                        processed += len(chunk)
                        insert_sql.extend([header.format(qualified_table_name), values_mark])
                        if processed == len_rows:
                            # On the last chunk we need to add the extra row read
                            insert_sql.append("".join(chunk) + until_nl)
                        else:
                            # Replace the , with ;
                            insert_sql.append("".join(chunk).strip()[:-1] + ";\n")
                else:
                    # otherwise write all content in a single INSERT INTO
                    insert_sql.extend([header.format(qualified_table_name), values_mark, content])

                    if until_nl:
                        insert_sql.append(until_nl)

                # actually this may be empty if we were able to read a full file into content
                if not is_eof:
                    # execute chunk of insert
                    yield insert_sql
                    insert_sql = []

        if insert_sql:
            yield insert_sql


class InsertValuesJobClient(SqlJobClientWithStaging):
    def restore_file_load(self, file_path: str) -> LoadJob:
        """Returns a completed SqlLoadJob or InsertValuesJob

        Returns completed jobs as SqlLoadJob and InsertValuesJob executed atomically in start_file_load so any jobs that should be recreated are already completed.
        Obviously the case of asking for jobs that were never created will not be handled. With correctly implemented loader that cannot happen.

        Args:
            file_path (str): a path to a job file

        Returns:
            LoadJob: Always a restored job completed
        """
        job = super().restore_file_load(file_path)
        if not job:
            job = EmptyLoadJob.from_file_path(file_path, "completed")
        return job

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        job = super().start_file_load(table, file_path, load_id)
        if not job:
            # this is using sql_client internally and will raise a right exception
            if file_path.endswith("insert_values"):
                job = InsertValuesLoadJob(table["name"], file_path, self.sql_client)
        return job

    # # TODO: implement indexes and primary keys for postgres
    # def _get_in_table_constraints_sql(self, t: TTableSchema) -> str:
    #     # get primary key
    #     pass

    # def _get_out_table_constrains_sql(self, t: TTableSchema) -> str:
    #     # set non unique indexes
    #     pass
