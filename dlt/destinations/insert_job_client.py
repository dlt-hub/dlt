import os
from typing import Any, Iterator, List

from dlt.common.destination.reference import LoadJob, FollowupJob, TLoadJobState
from dlt.common.schema.typing import TTableSchema, TWriteDisposition
from dlt.common.storages import FileStorage

from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.job_client_impl import SqlJobClientBase


class InsertValuesLoadJob(LoadJob, FollowupJob):
    def __init__(self, table_name: str, write_disposition: TWriteDisposition, file_path: str, sql_client: SqlClientBase[Any]) -> None:
        super().__init__(FileStorage.get_file_name_from_file_path(file_path))
        self._sql_client = sql_client
        # insert file content immediately
        with self._sql_client.with_staging_dataset(write_disposition=="merge"):
            with self._sql_client.begin_transaction():
                for fragments in self._insert(sql_client.make_qualified_table_name(table_name), write_disposition, file_path):
                    self._sql_client.execute_fragments(fragments)

    def state(self) -> TLoadJobState:
        # this job is always done
        return "completed"

    def exception(self) -> str:
        # this part of code should be never reached
        raise NotImplementedError()

    def _insert(self, qualified_table_name: str, write_disposition: TWriteDisposition, file_path: str) -> Iterator[List[str]]:
        # WARNING: maximum redshift statement is 16MB https://docs.aws.amazon.com/redshift/latest/dg/c_redshift-sql.html
        # the procedure below will split the inserts into max_query_length // 2 packs
        with open(file_path, "r", encoding="utf-8") as f:
            header = f.readline()
            values_mark = f.readline()
            # properly formatted file has a values marker at the beginning
            assert values_mark == "VALUES\n"

            insert_sql = []
            if write_disposition == "replace":
                insert_sql.append("DELETE FROM {};".format(qualified_table_name))
            while content := f.read(self._sql_client.capabilities.max_query_length // 2):
                # write INSERT
                insert_sql.extend([header.format(qualified_table_name), values_mark, content])
                # read one more line in order to
                # 1. complete the content which ends at "random" position, not an end line
                # 2. to modify it's ending without a need to re-allocating the 8MB of "content"
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
                # actually this may be empty if we were able to read a full file into content
                if until_nl:
                    insert_sql.append(until_nl)
                if not is_eof:
                    # execute chunk of insert
                    yield insert_sql
                    insert_sql = []
        if insert_sql:
            yield insert_sql


class InsertValuesJobClient(SqlJobClientBase):

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

    def start_file_load(self, table: TTableSchema, file_path: str) -> LoadJob:
        job = super().start_file_load(table, file_path)
        if not job:
            # this is using sql_client internally and will raise a right exception
            job = InsertValuesLoadJob(table["name"], table["write_disposition"], file_path, self.sql_client)
        return job

    # TODO: implement indexes and primary keys for postgres
    def _get_in_table_constraints_sql(self, t: TTableSchema) -> str:
        # get primary key
        pass

    def _get_out_table_constrains_sql(self, t: TTableSchema) -> str:
        # set non unique indexes
        pass
