import datetime  # noqa: I251
import re

from clickhouse_driver import dbapi as clickhouse_dbapi  # type: ignore[import-untyped]
import clickhouse_driver
import clickhouse_driver.errors  # type: ignore[import-untyped]
from clickhouse_driver.dbapi import OperationalError  # type: ignore[import-untyped]
from clickhouse_driver.dbapi.extras import DictCursor  # type: ignore[import-untyped]
import clickhouse_connect
from clickhouse_connect.driver.tools import insert_file as clk_insert_file
from clickhouse_connect.driver.summary import QuerySummary

from contextlib import contextmanager
from typing import (
    Iterator,
    AnyStr,
    Any,
    List,
    Optional,
    Sequence,
    ClassVar,
    Literal,
    Tuple,
    cast,
)

from pendulum import DateTime
import sqlglot
from sqlglot import exp
from .sqlglot_helpers import _add_properties, _has_expression

from dlt.common import logger
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.typing import DictStrAny
from dlt.common.utils import removeprefix

from dlt.destinations.exceptions import (
    DatabaseUndefinedRelation,
    DatabaseTransientException,
    DatabaseTerminalException,
)
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseCredentials,
    ClickHouseClientConfiguration,
)
from dlt.destinations.impl.clickhouse.typing import (
    TTableEngineType,
    TABLE_ENGINE_TYPE_TO_CLICKHOUSE_ATTR,
)
from dlt.destinations.sql_client import (
    DBApiCursorImpl,
    SqlClientBase,
    raise_database_error,
    raise_open_connection_error,
)
from dlt.destinations.typing import DBTransaction, DBApi
from dlt.destinations.utils import _convert_to_old_pyformat


TDeployment = Literal["ClickHouseOSS", "ClickHouseCloud"]
TRANSACTIONS_UNSUPPORTED_WARNING_MESSAGE = (
    "ClickHouse does not support transactions! Each statement is auto-committed separately."
)


class ClickHouseDBApiCursorImpl(DBApiCursorImpl):
    native_cursor: DictCursor

    # TODO: implement arrow reading


class ClickHouseSqlClient(
    SqlClientBase[clickhouse_driver.dbapi.connection.Connection], DBTransaction
):
    dbapi: ClassVar[DBApi] = clickhouse_dbapi

    def __init__(
        self,
        dataset_name: Optional[str],
        staging_dataset_name: str,
        known_table_names: List[str],
        credentials: ClickHouseCredentials,
        capabilities: DestinationCapabilitiesContext,
        config: ClickHouseClientConfiguration,
    ) -> None:
        super().__init__(credentials.database, dataset_name, staging_dataset_name, capabilities)
        self._conn: clickhouse_driver.dbapi.connection = None
        self.known_table_names = known_table_names
        self.credentials = credentials
        self.database_name = credentials.database
        self.config = config

    def has_dataset(self) -> bool:
        # we do not need to normalize dataset_sentinel_table_name.
        sentinel_table = self.config.dataset_sentinel_table_name
        all_ds_tables = self._list_tables()
        if self.dataset_name:
            prefix = self.dataset_name + self.config.dataset_table_separator
            return sentinel_table in [removeprefix(t, prefix) for t in all_ds_tables]
        else:
            # if no dataset specified we look for sentinel table
            return sentinel_table in all_ds_tables

    def open_connection(self) -> clickhouse_driver.dbapi.connection.Connection:
        self._conn = clickhouse_driver.connect(dsn=self.credentials.to_native_representation())
        return self._conn

    @raise_open_connection_error
    def close_connection(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @contextmanager
    @raise_database_error
    def begin_transaction(self) -> Iterator[DBTransaction]:
        yield self

    @raise_database_error
    def commit_transaction(self) -> None:
        self._conn.commit()

    @raise_database_error
    def rollback_transaction(self) -> None:
        self._conn.rollback()

    @property
    def native_connection(self) -> clickhouse_driver.dbapi.connection.Connection:
        return self._conn

    def execute_sql(
        self, sql: AnyStr, *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
        with self.execute_query(sql, *args, **kwargs) as curr:
            return None if curr.description is None else curr.fetchall()

    def create_dataset(self) -> None:
        # We create a sentinel table which defines whether we consider the dataset created.
        sentinel_table_name = self.make_qualified_table_name(
            self.config.dataset_sentinel_table_name
        )
        sentinel_table_type = cast(TTableEngineType, self.config.table_engine_type)
        cluster = self.config.cluster
        distributed_tables = self.config.distributed_tables
        if not (cluster and distributed_tables is True):        
            self.execute_sql(f"""
                CREATE TABLE {sentinel_table_name}
                (_dlt_id String NOT NULL)
                ENGINE={TABLE_ENGINE_TYPE_TO_CLICKHOUSE_ATTR.get(sentinel_table_type)}
                PRIMARY KEY _dlt_id
                COMMENT 'internal dlt sentinel table'""")
        else:
            config_database = self.credentials.database
            base_table_database = self.config.base_table_database_prefix + config_database
            sentinel_base_table_name = self.make_qualified_table_name(
                self.config.dataset_sentinel_table_name + self.config.base_table_name_postfix
            ).replace(config_database, base_table_database)
            # with the current implementation base table doesn't get dropped and we get an error on the next run
            # it seem like drop_dataset is never called, I've added logging there and nothing is printed
            self.execute_sql(f"""
                CREATE TABLE IF NOT EXISTS {sentinel_base_table_name}
                (_dlt_id String NOT NULL)
                ENGINE={TABLE_ENGINE_TYPE_TO_CLICKHOUSE_ATTR.get(sentinel_table_type)}
                PRIMARY KEY _dlt_id
                COMMENT 'internal dlt sentinel table'""")
            self.execute_sql(f"""
                CREATE TABLE IF NOT EXISTS {sentinel_table_name}
                (_dlt_id String NOT NULL)
                ENGINE=Distributed('{cluster}', '{base_table_database}', '{sentinel_base_table_name}', rand())
                COMMENT 'internal dlt sentinel table'""")

    def drop_dataset(self) -> None:
        # always try to drop the sentinel table.
        sentinel_table_name = self.make_qualified_table_name(
            self.config.dataset_sentinel_table_name
        )

        all_ds_tables = self._list_tables()

        if self.dataset_name:
            # Since ClickHouse doesn't have schemas, we need to drop all tables in our virtual schema,
            # or collection of tables, that has the `dataset_name` as a prefix.
            to_drop_results = all_ds_tables
        else:
            # drop only tables known in logical (dlt) schema
            to_drop_results = [
                table_name for table_name in self.known_table_names if table_name in all_ds_tables
            ]

        # catalog_name = self.catalog_name()
        # drop a sentinel table only when dataset name was empty (was not included in the schema)
        if not self.dataset_name:
            cluster = self.config.cluster
            distributed_tables = self.config.distributed_tables

            if not (cluster and distributed_tables is True):
                self.execute_sql(f"DROP TABLE {sentinel_table_name} SYNC")
            else:
                logger.debug(f"**** DROP TABLE : {sentinel_table_name}")
                self.execute_sql(f"DROP TABLE {sentinel_table_name} ON CLUSTER {cluster} SYNC")
                config_database = self.credentials.database
                base_table_database = self.config.base_table_database_prefix + config_database
                sg_table = sqlglot.to_table(sentinel_table_name, quoted=False, dialect="clickhouse")
                logger.debug(f"**** DROP TABLE : {base_table_database}.{sg_table.name + self.config.base_table_name_postfix}")
                self.execute_sql(f"DROP TABLE {base_table_database}.{sg_table.name + self.config.base_table_name_postfix} ON CLUSTER {cluster} SYNC")
            logger.warning(
                "Dataset without name (tables without prefix) got dropped. Only tables known in the"
                " current dlt schema and sentinel tables were removed."
            )
        else:
            sentinel_table_name = self.make_qualified_table_name_path(
                self.config.dataset_sentinel_table_name, escape=False
            )[-1]
            if sentinel_table_name not in all_ds_tables:
                # no sentinel table, dataset does not exist
                self.execute_sql(f"SELECT 1 FROM {sentinel_table_name}")
                raise AssertionError(f"{sentinel_table_name} must not exist")
        self.drop_tables(*to_drop_results)
        # for table in to_drop_results:
        #     # The "DROP TABLE" clause is discarded if we allow clickhouse_driver to handle parameter substitution.
        #     # This is because the driver incorrectly substitutes the entire query string, causing the "DROP TABLE" keyword to be omitted.
        #     # To resolve this, we are forced to provide the full query string here.
        #     self.execute_sql(
        #         f"DROP TABLE {catalog_name}.{self.capabilities.escape_identifier(table)} SYNC"
        #     )

    def drop_tables(self, *tables: str) -> None:
        """Drops a set of tables if they exist"""
        if not tables:
            return
        cluster = self.config.cluster
        distributed_tables = self.config.distributed_tables
        if not (cluster and distributed_tables is True):
            statements = [
                f"DROP TABLE IF EXISTS {self.make_qualified_table_name(table)} SYNC;"
                for table in tables
            ]
            self.execute_many(statements)
        else:            
            statements = [
                f"DROP TABLE IF EXISTS {self.make_qualified_table_name(table)} ON CLUSTER {self.config.cluster} SYNC;"
                for table in tables
            ]
            config_database = self.credentials.database
            base_table_database = self.config.base_table_database_prefix + config_database

            for table in tables:
                base_table_name = self.make_qualified_table_name(table + self.config.base_table_name_postfix).replace(config_database, base_table_database)
                statements.append(
                    f"DROP TABLE IF EXISTS {base_table_name} ON CLUSTER {self.config.cluster} SYNC;"
                )

            logger.debug(f"**** DROP TABLE STATEMENTS:\n{statements}")
            self.execute_many(statements)

    def insert_file(
        self, file_path: str, table_name: str, file_format: str, compression: str
    ) -> QuerySummary:
        with clickhouse_connect.create_client(
            host=self.credentials.host,
            port=self.credentials.http_port,
            database=self.credentials.database,
            user_name=self.credentials.username,
            password=self.credentials.password,
            secure=bool(self.credentials.secure),
        ) as clickhouse_connect_client:
            return clk_insert_file(
                clickhouse_connect_client,
                self.make_qualified_table_name(table_name),
                file_path,
                fmt=file_format,
                settings={
                    "allow_experimental_lightweight_delete": 1,
                    "enable_http_compression": 1,
                    "date_time_input_format": "best_effort",
                },
                compression=compression,
            )

    def _list_tables(self) -> List[str]:
        catalog_name, table_name = self.make_qualified_table_name_path("%", escape=False)
        rows = self.execute_sql(
            """
            SELECT name
            FROM system.tables
            WHERE database = %s
            AND name LIKE %s
            """,
            catalog_name,
            table_name,
        )
        return [row[0] for row in rows]

    @staticmethod
    def _sanitise_dbargs(db_args: DictStrAny) -> DictStrAny:
        """For ClickHouse OSS, the DBapi driver doesn't parse datetime types.
        We remove timezone specifications in this case."""
        for key, value in db_args.items():
            if isinstance(value, (DateTime, datetime.datetime)):
                db_args[key] = str(value.replace(microsecond=0, tzinfo=None))
        return db_args

    def extract_alter_table_name(self, query: str) -> Optional[str]:
        if self.contains_string(query, "ALTER TABLE"):
            pattern = r'(?i)\bALTER TABLE\s+([`"]?[\w]+[`"]?\.[`"]?[\w]+[`"]?)'
            group_num = 1
        else:
            # If no known command is found, return None
            logger.warning(f"Unknown command in query: {query}")
            return None

        match = re.search(pattern, query, re.DOTALL)
        if match:
            return match.group(group_num)  # Return the captured table name
        return None  # Return None if no match is found

    def add_on_cluster(self, query: str, cluster_name: str = None) -> str:
        # Define regex patterns for CREATE, DROP, and ALTER TABLE commands
        if cluster_name is None:
            return query
        patterns = [
            r'(?i)\bCREATE (TEMPORARY )*TABLE( IF NOT EXISTS)*\s+([`"]?[\w]+[`"]?\.[`"]?[\w]+[`"]?)',  # Matches "CREATE TABLE `schema`.`table`"
            r'(?i)\bDROP (TEMPORARY )*TABLE( IF EXISTS)*\s+([`"]?[\w]+[`"]?\.[`"]?[\w]+[`"]?)',    # Matches "DROP TABLE `schema`.`table`"
            r'(?i)\bALTER TABLE\s+([`"]?[\w]+[`"]?\.[`"]?[\w]+[`"]?)',    # Matches "ALTER TABLE `schema`.`table`"
            r'(?i)\bDELETE FROM\s+([`"]?[\w]+[`"]?\.[`"]?[\w]+[`"]?)',    # Matches "DELETE FROM `schema`.`table`"
            r'(?i)\bTRUNCATE TABLE\s+([`"]?[\w]+[`"]?\.[`"]?[\w]+[`"]?)',  # Matches "TRUNCATE TABLE `schema`.`table`"
        ]

        # Check each pattern
        for pattern in patterns:
            match = re.search(pattern, query)
            if match:
                # Construct the modified query
                modified_query = re.sub(pattern, f'\\g<0> ON CLUSTER {cluster_name}', query)
                return modified_query

        # Return the original query if no modifications were made
        return query

    def contains_string(self, query: str, what: str) -> bool:
        return bool(re.search(fr'\b{what}\b', query, re.IGNORECASE))


    def adjust_query_for_distributed_setup(self, query: str) -> str:
        # Adjust the query for distributed setup
        # To have all the data on all replicas we need a base table and a distributed table on top of it
        # dlt generates create table statements for standard case where a single create statements is enough
        # hence we need to modify create statements and all other DDL and DML statements
        cluster = self.config.cluster
        distributed_tables = self.config.distributed_tables
        if cluster and distributed_tables is True:            
            oncluster = exp.OnCluster(this=exp.var(cluster))
            mod_queries = []
            # query can contain mutliple statements of different types (CREATE, DELETE, INSERT ...) separated by ;
            for qry in query.split(";"):
                # db name for the base table
                if qry == "" or qry is None:
                    continue # skip or sqlglot will throw an error
                base_db = self.config.base_table_database_prefix + self.credentials.database
                if not(self.contains_string(qry, "DROP TABLE") or self.contains_string(qry, "TRUNCATE TABLE")):
                    # DROP and TRUNCATE do not have joins and are not fully supported by sqlglot for clickhouse
                    stmt = sqlglot.parse_one(qry, dialect='clickhouse')
                    # if there are joins in the query we need to add global
                    # stmt = stmt.transform(_add_global)
                    # because of the special case for the DELETE FROM we cannot
                    # convert sqlglot ast after the if statement so we convert it here
                    # to adjust joins in a single place
                    qry = stmt.sql(dialect="clickhouse", pretty=True, identify=False)
                
                if (self.contains_string(qry, "CREATE") and self.contains_string(qry, "Engine\\s*=\\s*Memory")):
                    # Possibly not the best way to catch the CTAS for the temporary table, but it works for now
                    # _to_temp_table function doesn't have access to the config object so this is the best place to modify the query

                    # According to CH docs Memory engine is in many cases not much better then using MergeTree
                    # also the Memory engine tables exist on a single replica and we need it on all replicas
                    stmt.args["properties"].find(exp.EngineProperty).replace(exp.EngineProperty(this=exp.var("ReplicatedMergeTree")))
                    if not _has_expression(stmt.args["properties"], exp.OnCluster):
                        stmt.args["properties"] = _add_properties(properties=stmt.args["properties"], new_properties=[oncluster])
                    primary_key = exp.PrimaryKey(expressions=[(exp.Ordered(this="tuple()", nulls_first=False))]) # needs to be defined for the ReplicatedMergeTree engine
                    stmt.args["properties"] = _add_properties(properties=stmt.args["properties"], new_properties=[primary_key])
                    qry = stmt.sql(dialect="clickhouse", pretty=True, identify=False)
                if self.contains_string(qry, "DELETE FROM"):
                    # we need to delete from the base table
                    base_table = exp.to_table(sql_path= base_db + "." + stmt.this.name + self.config.base_table_name_postfix, quoted=False, dialect="clickhouse")
                    stmt.this.replace(base_table)
                    qry = stmt.sql(dialect="clickhouse", pretty=True, identify=False)
                    # sqlglot doesn't support adding ON CLUSTER clause to DELETE statement [2025-09-05]
                    qry = self.add_on_cluster(qry, cluster)
                   
                    # sqlglot cannot parse the settings part of the query so I guess it can't add it as well [2025-09-05]
                    qry = qry + " SETTINGS allow_nondeterministic_mutations=1"
                elif (self.contains_string(qry, "TRUNCATE TABLE") or self.contains_string(qry, "DROP TABLE") or self.contains_string(qry, "ALTER")) and not self.contains_string(qry, "ON CLUSTER"):
                    # sqlglot doesn't support adding ON CLUSTER clause to TRUNCATE TABLE, DROP TABLE or ALTER TABLE statement
                    # and it aslo raises an error for the SYNC keyword  [2025-09-05]
                    if self.contains_string(qry, "TRUNCATE TABLE"):
                        # we need to truncate the base table or the data will not be deleted
                        # eventhough according to the docs and github discussion it should be
                        # https://github.com/ClickHouse/ClickHouse/issues/50447
                        stmt = sqlglot.parse_one(qry, dialect='clickhouse')
                        table = stmt.find(sqlglot.expressions.Table)
                        table.args["db"] = exp.Identifier(this=base_db, quoted=True)
                        table.args["this"] = exp.Identifier(this=table.name + self.config.base_table_name_postfix, quoted=True)
                        qry = stmt.sql(dialect="clickhouse")
                    elif self.contains_string(qry, "UPDATE"):
                        # hadnling ALTER TABLE table_name UPDATE
                        # We need to update the base table not the distributed one, it's not supported by ClickHouse
                        # And sqlglot doesn't support this statement, as in, it doesn't parse the table name
                        # We use sqlglot to parse the table name and then use replace to adjust the query
                        logger.debug(f"**** ALTER TABLE UPDATE query:\n{qry}")
                        table_name = self.extract_alter_table_name(qry)
                        if table_name:
                            # we need to update the base table
                            table = exp.to_table(sql_path=table_name,dialect="clickhouse")
                            qry = qry.replace(table.db, base_db, 1).replace(table.name, table.name + self.config.base_table_name_postfix, 1)
                            qry = qry + " SETTINGS allow_nondeterministic_mutations = 1"
                            logger.debug(f"**** ALTER TABLE UPDATE query after replace:\n{qry}")


                    qry = self.add_on_cluster(qry, cluster)

                # we want to ensure that any IN or JOIN works with the distributed table
                # even if GLOBAL was not explicitly specified
                if self.contains_string(qry, "JOIN ") or self.contains_string(qry, "IN "):
                    # if
                    if self.contains_string(qry, "SETTINGS"):
                        qry = qry + ", distributed_product_mode = 'global'"
                    else:
                        qry = qry + "\nSETTINGS distributed_product_mode = 'global'"

                # unfortunately sqlglot is converting rand() to randCanonical() for clikchouse
                # rand() returns an integer value and randCanonical() returns a float
                # Float values are not supported for sharding key values
                if self.contains_string(qry, "Distributed") and self.contains_string(qry, "randCanonical()"):
                    qry = qry.replace("randCanonical()", "rand()")
                mod_queries.append(qry)
            query = ";\n".join(mod_queries)
            logger.debug(f"**** queries after joining:\n{query}")
        return query

    @contextmanager
    @raise_database_error
    def execute_query(
        self, query: AnyStr, *args: Any, **kwargs: Any
    ) -> Iterator[ClickHouseDBApiCursorImpl]:
        assert isinstance(query, str), "Query must be a string."

        # sqlglot doesn't like %s in the query string and throws an error, hence the workaround
        # and we do this before the _convert_to_old_pyformat function to avoid dealing with the
        # positional numbers in the placeholders
        dummy_identifier = 'dummy____845_identifier___2165'
        query = query.replace("%s", dummy_identifier)
        logger.debug(f"**** Executing queries after replace:\n{query}")
        query = self.adjust_query_for_distributed_setup(query)
        query = query.replace(dummy_identifier, "%s")

        db_args: DictStrAny = kwargs.copy()

        if args:
            query, db_args = _convert_to_old_pyformat(query, args, OperationalError)
            db_args.update(kwargs)

        db_args = self._sanitise_dbargs(db_args)


        with self._conn.cursor() as cursor:
            if self.config.distributed_tables is True:
                # With distributed_product_mode setting we ensure that all joins and IN clauses work with the distributed table
                # even if GLOBAL was not explicitly specified
                # Also, UPDATEs and DELETEs will work with the distributed table due to allow_nondeterministic_mutations=1.
                # This is easier than modifying each query
                cursor.set_settings(settings={
                    "distributed_product_mode": "global",
                    "allow_nondeterministic_mutations": 1
                })
            for query_line in query.split(";"):
                if query_line := query_line.strip():
                    try:
                        cursor.execute(query_line, db_args)
                    except KeyError as e:
                        raise DatabaseTransientException(OperationalError()) from e

            yield ClickHouseDBApiCursorImpl(cursor)  # type: ignore[abstract]

    def catalog_name(self, escape: bool = True) -> Optional[str]:
        database_name = self.capabilities.casefold_identifier(self.database_name)
        if escape:
            database_name = self.capabilities.escape_identifier(database_name)
        return database_name

    def make_qualified_table_name_path(
        self, table_name: Optional[str], escape: bool = True
    ) -> List[str]:
        # get catalog and dataset
        path = super().make_qualified_table_name_path(None, escape=escape)
        if table_name:
            # table name combines dataset name and table name
            if self.dataset_name:
                table_name = self.capabilities.casefold_identifier(
                    f"{self.dataset_name}{self.config.dataset_table_separator}{table_name}"
                )
            else:
                # without dataset just use the table name
                table_name = self.capabilities.casefold_identifier(table_name)
            if escape:
                table_name = self.capabilities.escape_identifier(table_name)
            # we have only two path components
            path[1] = table_name
        return path

    def _get_information_schema_components(self, *tables: str) -> Tuple[str, str, List[str]]:
        components = super()._get_information_schema_components(*tables)
        # clickhouse has a catalogue and no schema but uses catalogue as a schema to query the information schema ­🤷
        # so we must disable catalogue search. also note that table name is prefixed with logical "dataset_name"
        return (None, components[0], components[2])

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if isinstance(ex, clickhouse_driver.dbapi.errors.OperationalError):
            if "Code: 57." in str(ex) or "Code: 82." in str(ex) or "Code: 47." in str(ex):
                return DatabaseTerminalException(ex)
            elif "Code: 60." in str(ex) or "Code: 81." in str(ex):
                return DatabaseUndefinedRelation(ex)
            else:
                return DatabaseTransientException(ex)
        elif isinstance(
            ex,
            (
                clickhouse_driver.dbapi.errors.OperationalError,
                clickhouse_driver.dbapi.errors.InternalError,
            ),
        ):
            return DatabaseTransientException(ex)
        elif isinstance(
            ex,
            (
                clickhouse_driver.dbapi.errors.DataError,
                clickhouse_driver.dbapi.errors.ProgrammingError,
                clickhouse_driver.dbapi.errors.IntegrityError,
            ),
        ):
            return DatabaseTerminalException(ex)
        elif cls.is_dbapi_exception(ex):
            return DatabaseTransientException(ex)
        else:
            return ex

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, clickhouse_driver.dbapi.Error)
