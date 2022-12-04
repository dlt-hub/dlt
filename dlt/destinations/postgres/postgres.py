from abc import abstractmethod
import platform

from dlt.common.wei import EVM_DECIMAL_PRECISION
if platform.python_implementation() == "PyPy":
    import psycopg2cffi as psycopg2
    from psycopg2cffi.sql import SQL, Composed
else:
    import psycopg2
    from psycopg2.sql import SQL, Composed


from typing import Any, Dict, List, Optional, Sequence, Tuple, Type

from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.destination import DestinationCapabilitiesContext, DestinationClientDwhConfiguration, LoadJob, TLoadJobStatus
from dlt.common.schema import COLUMN_HINTS, TColumnSchema, TColumnSchemaBase, TDataType, TColumnHint, Schema, TTableSchemaColumns, add_missing_hints
from dlt.common.schema.typing import TTableSchema, TWriteDisposition
from dlt.common.storages.file_storage import FileStorage

from dlt.destinations.exceptions import DestinationSchemaWillNotUpdate
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.job_client_impl import SqlJobClientBase, LoadEmptyJob

from dlt.destinations.postgres import capabilities
from dlt.destinations.postgres.sql_client import Psycopg2SqlClient
from dlt.destinations.postgres.configuration import PostgresClientConfiguration


SCT_TO_PGT: Dict[TDataType, str] = {
    "complex": "varchar",
    "text": "varchar",
    "double": "double precision",
    "bool": "boolean",
    "timestamp": "timestamp with time zone",
    "bigint": "bigint",
    "binary": "bytea",
    "decimal": f"numeric({DEFAULT_NUMERIC_PRECISION},{DEFAULT_NUMERIC_SCALE})"
}

PGT_TO_SCT: Dict[str, TDataType] = {
    "varchar": "text",
    "double precision": "double",
    "boolean": "bool",
    "timestamp with time zone": "timestamp",
    "bigint": "bigint",
    "bytea": "binary",
    "numeric": "decimal"
}

HINT_TO_POSTGRES_ATTR: Dict[TColumnHint, str] = {
    "unique": "UNIQUE"
}


class InsertValuesLoadJob(LoadJob):
    def __init__(self, table_name: str, write_disposition: TWriteDisposition, file_path: str, sql_client: SqlClientBase["psycopg2.connection"]) -> None:
        super().__init__(FileStorage.get_file_name_from_file_path(file_path))
        self._sql_client = sql_client
        # insert file content immediately
        self._insert(sql_client.make_qualified_table_name(table_name), write_disposition, file_path)

    def status(self) -> TLoadJobStatus:
        # this job is always done
        return "completed"

    def file_name(self) -> str:
        return self._file_name

    def exception(self) -> str:
        # this part of code should be never reached
        raise NotImplementedError()

    def _insert(self, qualified_table_name: str, write_disposition: TWriteDisposition, file_path: str) -> None:
        # TODO: implement tracking of jobs in storage, both completed and failed
        # WARNING: maximum redshift statement is 16MB https://docs.aws.amazon.com/redshift/latest/dg/c_redshift-sql.html
        # the procedure below will split the inserts into 4MB packs
        with open(file_path, "r", encoding="utf-8") as f:
            header = f.readline()
            values_mark = f.readline()
            # properly formatted file has a values marker at the beginning
            assert values_mark == "VALUES\n"
            # begin the transaction
            insert_sql = [SQL("BEGIN TRANSACTION;")]
            if write_disposition == "replace":
                insert_sql.append(SQL("DELETE FROM {};").format(SQL(qualified_table_name)))
            # is_eof = False
            while content := f.read(PostgresClient.capabilities()["max_query_length"] // 2):
                # read one more line in order to
                # 1. complete the content which ends at "random" position, not an end line
                # 2. to modify it's ending without a need to re-allocating the 8MB of "content"
                until_nl = f.readline().strip("\n")
                # write INSERT
                insert_sql.extend(
                    [SQL(header).format(SQL(qualified_table_name)),
                    SQL(values_mark),
                    SQL(content)]
                )
                # are we at end of file
                is_eof = len(until_nl) == 0 or until_nl[-1] == ";"
                if not is_eof:
                    # replace the "," with ";"
                    until_nl = until_nl[:-1] + ";\n"
                # actually this may be empty if we were able to read a full file into content
                if until_nl:
                    insert_sql.append(SQL(until_nl))
                if not is_eof:
                    # execute chunk of insert
                    self._sql_client.execute_sql(Composed(insert_sql))
                    insert_sql.clear()

        # on EOF add COMMIT TX and execute
        insert_sql.append(SQL("COMMIT TRANSACTION;"))
        self._sql_client.execute_sql(Composed(insert_sql))


class PostgresClientBase(SqlJobClientBase):

    def restore_file_load(self, file_path: str) -> LoadJob:
        # always returns completed jobs as InsertValuesLoadJob is executed
        # atomically in start_file_load so any jobs that should be recreated are already completed
        # in case of bugs in loader (asking for jobs that were never created) we are not able to detect that
        return LoadEmptyJob.from_file_path(file_path, "completed")

    def start_file_load(self, table: TTableSchema, file_path: str) -> LoadJob:
        # this is using sql_client internally and will raise a right exception
        return InsertValuesLoadJob(table["name"], table["write_disposition"], file_path, self.sql_client)

    def _get_table_update_sql(self, table_name: str, new_columns: Sequence[TColumnSchema], exists: bool) -> str:
        # build sql
        canonical_name = self.sql_client.make_qualified_table_name(table_name)
        sql = "BEGIN TRANSACTION;\n"
        if not exists:
            # build CREATE
            sql += f"CREATE TABLE {canonical_name} (\n"
            sql += ",\n".join([self._get_column_def_sql(c) for c in new_columns])
            sql += ");"
        else:
            # build ALTER as separate statement for each column (redshift limitation)
            sql += "\n".join([f"ALTER TABLE {canonical_name}\nADD COLUMN {self._get_column_def_sql(c)};" for c in new_columns])
        # scan columns to get hints
        if exists:
            # no hints may be specified on added columns
            for hint in COLUMN_HINTS:
                if any(c.get(hint, False) is True for c in new_columns):
                    hint_columns = [c["name"] for c in new_columns if c.get(hint, False)]
                    raise DestinationSchemaWillNotUpdate(canonical_name, hint_columns, f"{hint} requested after table was created")
        # TODO: add FK relations
        sql += "\nCOMMIT TRANSACTION;"
        return sql

    # TODO: implement indexes and primary keys for postgres
    def _get_in_table_constraints_sql(self, t: TTableSchema) -> str:
        # get primary key
        pass

    def _get_out_table_constrains_sql(self, t: TTableSchema) -> str:
        # set non unique indexes
        pass

    @abstractmethod
    def _get_column_def_sql(self, c: TColumnSchema) -> str:
        pass

    def get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:
        schema_table: TTableSchemaColumns = {}
        query = f"""
                SELECT column_name, data_type, is_nullable, numeric_precision, numeric_scale
                    FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_schema = '{self.sql_client.fully_qualified_dataset_name()}' AND table_name = '{table_name}'
                ORDER BY ordinal_position;
                """
        rows = self.sql_client.execute_sql(query)
        # if no rows we assume that table does not exist
        if len(rows) == 0:
            # TODO: additionally check if table exists
            return False, schema_table
        # TODO: pull more data to infer indexes, PK and uniques attributes/constraints
        for c in rows:
            schema_c: TColumnSchemaBase = {
                "name": c[0],
                "nullable": self._null_to_bool(c[2]),
                "data_type": self._pq_t_to_sc_t(c[1], c[3], c[4]),
            }
            schema_table[c[0]] = add_missing_hints(schema_c)
        return True, schema_table

    @staticmethod
    def _null_to_bool(v: str) -> bool:
        if v == "NO":
            return False
        elif v == "YES":
            return True
        raise ValueError(v)

    @staticmethod
    def _gen_not_null(v: bool) -> str:
        return "NOT NULL" if not v else ""

    @staticmethod
    @abstractmethod
    def _sc_t_to_pq_t(sc_t: TDataType) -> str:
        pass

    @staticmethod
    @abstractmethod
    def _pq_t_to_sc_t(pq_t: str, precision: Optional[int], scale: Optional[int]) -> TDataType:
        pass


class PostgresClient(PostgresClientBase):

    def __init__(self, schema: Schema, config: PostgresClientConfiguration) -> None:
        sql_client = Psycopg2SqlClient(
            schema.normalize_make_dataset_name(config.dataset_name, config.default_schema_name, schema.name),
            config.credentials
        )
        super().__init__(schema, config, sql_client)
        self.config: PostgresClientConfiguration = config
        self.sql_client = sql_client
        self.caps = self.capabilities()

    def _get_column_def_sql(self, c: TColumnSchema) -> str:
        hints_str = " ".join(HINT_TO_POSTGRES_ATTR.get(h, "") for h in HINT_TO_POSTGRES_ATTR.keys() if c.get(h, False) is True)
        column_name = self.caps.escape_identifier(c["name"])
        return f"{column_name} {self._sc_t_to_pq_t(c['data_type'])} {hints_str} {self._gen_not_null(c['nullable'])}"

    @staticmethod
    def _sc_t_to_pq_t(sc_t: TDataType) -> str:
        if sc_t == "wei":
            return f"numeric({2*EVM_DECIMAL_PRECISION}, {EVM_DECIMAL_PRECISION})"
        return SCT_TO_PGT[sc_t]

    @staticmethod
    def _pq_t_to_sc_t(pq_t: str, precision: Optional[int], scale: Optional[int]) -> TDataType:
        if pq_t == "numeric":
            if precision == 2*EVM_DECIMAL_PRECISION and scale == EVM_DECIMAL_PRECISION:
                return "wei"
        return PGT_TO_SCT.get(pq_t, "text")

    @classmethod
    def capabilities(cls) -> DestinationCapabilitiesContext:
        return capabilities()
