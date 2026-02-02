from typing import Any, Dict, Type, Union, TYPE_CHECKING, Optional

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.destination.capabilities import DataTypeMapper
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.normalizers import NamingConvention

from dlt.destinations.impl.sqlalchemy.configuration import (
    SqlalchemyCredentials,
    SqlalchemyClientConfiguration,
)

if TYPE_CHECKING:
    from dlt.destinations.impl.sqlalchemy.sqlalchemy_job_client import SqlalchemyJobClient
    from sqlalchemy.engine import Engine
else:
    Engine = Any


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
        dialect_type = config.get_dialect()
        if dialect_type is not None:
            # instantiate dialect to access properties and pass to capabilities
            dialect = dialect_type()
            backend_name = config.get_backend_name()

            # generic dialect properties
            caps.max_identifier_length = dialect.max_identifier_length
            caps.max_column_identifier_length = dialect.max_identifier_length
            caps.supports_native_boolean = dialect.supports_native_boolean

            # look up registered dialect capabilities
            from dlt.destinations.impl.sqlalchemy.dialect import (
                get_dialect_capabilities,
                DialectCapabilities,
            )

            # try backend_name first, then dialect.name (they may differ)
            dialect_caps = (
                get_dialect_capabilities(backend_name)
                or get_dialect_capabilities(dialect.name)
                or DialectCapabilities(backend_name)
            )

            # set sqlglot dialect from the property (driven by backend name)
            caps.sqlglot_dialect = dialect_caps.sqlglot_dialect  # type: ignore[assignment]
            caps.type_mapper = dialect_caps.type_mapper_class()
            caps.dialect_capabilities = dialect_caps
            dialect_caps.adjust_capabilities(caps, dialect)

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
        engine_kwargs: Dict[str, Any] = None,
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
            engine_kwargs (Dict[str, Any], optional): Additional keyword arguments passed to `sqlalchemy.create_engine`. Defaults to None.
            engine_args (Dict[str, Any], optional): Deprecated. Use engine_kwargs instead. Defaults to None.
            **kwargs (Any): Additional arguments passed to the destination.
        """
        super().__init__(
            credentials=credentials,
            create_unique_indexes=create_unique_indexes,
            create_primary_keys=create_primary_keys,
            destination_name=destination_name,
            environment=environment,
            engine_kwargs=engine_kwargs,
            engine_args=engine_args,
            **kwargs,
        )


sqlalchemy.register()
