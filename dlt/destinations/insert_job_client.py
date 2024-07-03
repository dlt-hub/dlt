import os
import abc
from typing import Any, Iterator, List

from dlt.common.destination.reference import LoadJob, HasFollowupJobs
from dlt.common.schema.typing import TTableSchema
from dlt.common.storages import FileStorage
from dlt.common.utils import chunks

from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.job_impl import EmptyLoadJobWithFollowupJobs
from dlt.destinations.job_client_impl import SqlJobClientWithStaging, SqlJobClientBase


class InsertValuesLoadJob(LoadJob, HasFollowupJobs):
    def __init__(self, job_client: SqlJobClientBase, table_name: str, file_path: str) -> None:
        super().__init__(job_client, file_path)
        self._sql_client = job_client.sql_client
        self.table_name = table_name

    def run(self) -> None:
        # insert file content immediately
        with self._sql_client.begin_transaction():
            for fragments in self._insert(
                self._sql_client.make_qualified_table_name(self.table_name), self._file_path
            ):
                self._sql_client.execute_fragments(fragments)

    def _insert(self, qualified_table_name: str, file_path: str) -> Iterator[List[str]]:
        # WARNING: maximum redshift statement is 16MB https://docs.aws.amazon.com/redshift/latest/dg/c_redshift-sql.html
        # the procedure below will split the inserts into max_query_length // 2 packs
        with FileStorage.open_zipsafe_ro(file_path, "r", encoding="utf-8") as f:
            header = f.readline()
            # format and casefold header
            header = self._sql_client.capabilities.casefold_identifier(header).format(
                qualified_table_name
            )
            writer_type = self._sql_client.capabilities.insert_values_writer_type
            if writer_type == "default":
                sep = ","
                # properly formatted file has a values marker at the beginning
                values_mark = f.readline()
                assert values_mark == "VALUES\n"
            elif writer_type == "select_union":
                sep = " UNION ALL"

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
                    until_nl = until_nl[: -len(sep)] + ";"  # replace the separator with ";"
                if max_rows is not None:
                    # mssql has a limit of 1000 rows per INSERT, so we need to split into separate statements
                    values_rows = content.splitlines(keepends=True)
                    len_rows = len(values_rows)
                    processed = 0
                    # Chunk by max_rows - 1 for simplicity because one more row may be added
                    for chunk in chunks(values_rows, max_rows - 1):
                        processed += len(chunk)
                        insert_sql.append(header)
                        if writer_type == "default":
                            insert_sql.append(values_mark)
                        if processed == len_rows:
                            # On the last chunk we need to add the extra row read
                            insert_sql.append("".join(chunk) + until_nl)
                        else:
                            # Replace the , with ;
                            insert_sql.append("".join(chunk).strip()[: -len(sep)] + ";\n")
                else:
                    # otherwise write all content in a single INSERT INTO
                    if writer_type == "default":
                        insert_sql.extend([header, values_mark, content + until_nl])
                    elif writer_type == "select_union":
                        insert_sql.extend([header, content + until_nl])

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

        Returns completed jobs as SqlLoadJob and InsertValuesJob executed atomically in get_load_job so any jobs that should be recreated are already completed.
        Obviously the case of asking for jobs that were never created will not be handled. With correctly implemented loader that cannot happen.

        Args:
            file_path (str): a path to a job file

        Returns:
            LoadJob: Always a restored job completed
        """
        job = super().restore_file_load(file_path)
        if not job:
            job = EmptyLoadJobWithFollowupJobs.from_file_path(file_path, "completed")
        return job

    def get_load_job(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        job = super().get_load_job(table, file_path, load_id)
        if not job:
            # this is using sql_client internally and will raise a right exception
            if file_path.endswith("insert_values"):
                job = InsertValuesLoadJob(self, table["name"], file_path)
        return job
