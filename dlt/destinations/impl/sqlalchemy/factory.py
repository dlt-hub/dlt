from typing import Any, Dict, Type, Union, TYPE_CHECKING, Optional

from dlt.common import pendulum
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.destination.capabilities import DataTypeMapper
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.normalizers import NamingConvention

from dlt.destinations.impl.sqlalchemy.configuration import (
    SqlalchemyCredentials,
    SqlalchemyClientConfiguration,
)
from dlt.common.data_writers.escape import format_datetime_literal

if TYPE_CHECKING:
    # from dlt.destinations.impl.sqlalchemy.sqlalchemy_client import SqlalchemyJobClient
    from dlt.destinations.impl.sqlalchemy.sqlalchemy_job_client import SqlalchemyJobClient
    from sqlalchemy.engine import Engine
else:
    Engine = Any


def _format_mysql_datetime_literal(
    v: pendulum.DateTime, precision: int = 6, no_tz: bool = False
) -> str:
    # Format without timezone to prevent tz conversion in SELECT
    return format_datetime_literal(v, precision, no_tz=True)


class sqlalchemy(Destination[SqlalchemyClientConfiguration, "SqlalchemyJobClient"]):
    spec = SqlalchemyClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        # lazy import to avoid sqlalchemy dep
        SqlalchemyTypeMapper: Type[DataTypeMapper]

        try:
            from dlt.destinations.impl.sqlalchemy.type_mapper import SqlalchemyTypeMapper
        except ModuleNotFoundError:
            # assign mock type mapper if no sqlalchemy
            from dlt.common.destination.capabilities import (
                UnsupportedTypeMapper as SqlalchemyTypeMapper,
            )

        # https://www.sqlalchemyql.org/docs/current/limits.html
        caps = DestinationCapabilitiesContext.generic_capabilities()
        caps.preferred_loader_file_format = "typed-jsonl"
        caps.supported_loader_file_formats = ["typed-jsonl", "parquet", "model"]
        caps.preferred_staging_file_format = None
        caps.supported_staging_file_formats = []
        caps.has_case_sensitive_identifiers = True
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
        caps.max_identifier_length = 63
        caps.max_column_identifier_length = 63
        caps.max_query_length = 32 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 1024 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = True
        caps.max_query_parameters = 20_0000
        caps.max_rows_per_insert = 10_000  # Set a default to avoid OOM on large datasets
        # Multiple concatenated statements are not supported by all engines, so leave them off by default
        caps.supports_multiple_statements = False
        caps.type_mapper = SqlalchemyTypeMapper
        caps.supported_replace_strategies = ["truncate-and-insert", "insert-from-staging"]
        caps.supported_merge_strategies = ["delete-insert", "scd2"]

        return caps

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: SqlalchemyClientConfiguration,
        naming: Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        # lazy import to avoid sqlalchemy dep
        MssqlVariantTypeMapper: Type[DataTypeMapper]
        MysqlVariantTypeMapper: Type[DataTypeMapper]
        TrinoVariantTypeMapper: Type[DataTypeMapper]

        try:
            from dlt.destinations.impl.sqlalchemy.type_mapper import (
                MssqlVariantTypeMapper,
                MysqlVariantTypeMapper,
                TrinoVariantTypeMapper,
            )
        except ModuleNotFoundError:
            # assign mock type mapper if no sqlalchemy
            from dlt.common.destination.capabilities import (
                UnsupportedTypeMapper as MssqlVariantTypeMapper,
                UnsupportedTypeMapper as MysqlVariantTypeMapper,
                UnsupportedTypeMapper as TrinoVariantTypeMapper,
            )

        dialect = config.get_dialect()
        if dialect is not None:
            backend_name = config.get_backend_name()

            caps.max_identifier_length = dialect.max_identifier_length
            caps.max_column_identifier_length = dialect.max_identifier_length
            caps.supports_native_boolean = dialect.supports_native_boolean

            if dialect.name == "mysql" or backend_name in ("mysql", "mariadb"):
                # correct max identifier length
                # dialect uses 255 (max length for aliases) instead of 64 (max length of identifiers)
                caps.max_identifier_length = 64
                caps.max_column_identifier_length = 64
                caps.format_datetime_literal = _format_mysql_datetime_literal
                caps.enforces_nulls_on_alter = False
                caps.sqlglot_dialect = "mysql"
                caps.type_mapper = MysqlVariantTypeMapper
            elif dialect.name == "trino":
                caps.sqlglot_dialect = "trino"
                caps.timestamp_precision = 3
                caps.type_mapper = TrinoVariantTypeMapper

            elif backend_name in [
                "oracle",
                "redshift",
                "drill",
                "druid",
                "presto",
                "hive",
                "trino",
                "clickhouse",
                "databricks",
                "bigquery",
                "snowflake",
                "doris",
                "risingwave",
                "starrocks",
                "sqlite",
            ]:
                caps.sqlglot_dialect = backend_name  #  type: ignore

            elif backend_name == "postgresql":
                caps.sqlglot_dialect = "postgres"
            elif backend_name == "awsathena":
                caps.sqlglot_dialect = "athena"
            elif backend_name == "mssql":
                caps.sqlglot_dialect = "tsql"
                caps.type_mapper = MssqlVariantTypeMapper
            elif backend_name == "teradatasql":
                caps.sqlglot_dialect = "teradata"

            if dialect.requires_name_normalize:  # type: ignore[attr-defined]
                caps.has_case_sensitive_identifiers = False
                caps.casefold_identifier = str.lower

        return super(sqlalchemy, cls).adjust_capabilities(caps, config, naming)

    @property
    def client_class(self) -> Type["SqlalchemyJobClient"]:
        from dlt.destinations.impl.sqlalchemy.sqlalchemy_job_client import SqlalchemyJobClient

        return SqlalchemyJobClient

    def __init__(
        self,
        credentials: Union[SqlalchemyCredentials, Dict[str, Any], str, Engine] = None,
        create_unique_indexes: bool = False,
        create_primary_keys: bool = False,
        destination_name: str = None,
        environment: str = None,
        engine_args: Dict[str, Any] = None,
        **kwargs: Any,
    ) -> None:
        """Configure the Sqlalchemy destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials (Union[SqlalchemyCredentials, Dict[str, Any], str, Engine], optional): Credentials to connect to the sqlalchemy database. Can be an instance of
                `SqlalchemyCredentials` or a connection string in the format `mysql://user:password@host:port/database`. Defaults to None.
            create_unique_indexes (bool, optional): Whether UNIQUE constraints should be created. Defaults to False.
            create_primary_keys (bool, optional): Whether PRIMARY KEY constraints should be created. Defaults to False.
            destination_name (str, optional): The name of the destination. Defaults to None.
            environment (str, optional): The environment to use. Defaults to None.
            engine_args (Dict[str, Any], optional): Additional arguments to pass to the SQLAlchemy engine. Defaults to None.
            **kwargs (Any): Additional arguments passed to the destination.
        """
        super().__init__(
            credentials=credentials,
            create_unique_indexes=create_unique_indexes,
            create_primary_keys=create_primary_keys,
            destination_name=destination_name,
            environment=environment,
            engine_args=engine_args,
            **kwargs,
        )


sqlalchemy.register()
