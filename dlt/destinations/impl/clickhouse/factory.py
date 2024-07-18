import sys
import typing as t

from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.data_writers.escape import (
    escape_clickhouse_identifier,
    escape_clickhouse_literal,
    format_clickhouse_datetime_literal,
)
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseClientConfiguration,
    ClickHouseCredentials,
)


if t.TYPE_CHECKING:
    from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseClient
    from clickhouse_driver.dbapi import Connection  # type: ignore[import-untyped]


class clickhouse(Destination[ClickHouseClientConfiguration, "ClickHouseClient"]):
    spec = ClickHouseClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "jsonl"
        caps.supported_loader_file_formats = ["parquet", "jsonl"]
        caps.preferred_staging_file_format = "jsonl"
        caps.supported_staging_file_formats = ["parquet", "jsonl"]

        caps.format_datetime_literal = format_clickhouse_datetime_literal
        caps.escape_identifier = escape_clickhouse_identifier
        caps.escape_literal = escape_clickhouse_literal
        # docs are very unclear https://clickhouse.com/docs/en/sql-reference/syntax
        # taking into account other sources: identifiers are case sensitive
        caps.has_case_sensitive_identifiers = True
        # and store as is in the information schema
        caps.casefold_identifier = str

        # https://stackoverflow.com/questions/68358686/what-is-the-maximum-length-of-a-column-in-clickhouse-can-it-be-modified
        caps.max_identifier_length = 255
        caps.max_column_identifier_length = 255

        # ClickHouse has no max `String` type length.
        caps.max_text_data_type_length = sys.maxsize

        caps.schema_supports_numeric_precision = True
        # Use 'Decimal128' with these defaults.
        # https://clickhouse.com/docs/en/sql-reference/data-types/decimal
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        # Use 'Decimal256' with these defaults.
        caps.wei_precision = (76, 0)
        caps.timestamp_precision = 6

        # https://clickhouse.com/docs/en/operations/settings/settings#max_query_size
        caps.is_max_query_length_in_bytes = True
        caps.max_query_length = 262144

        # ClickHouse has limited support for transactional semantics, especially for `ReplicatedMergeTree`,
        # the default ClickHouse Cloud engine. It does, however, provide atomicity for individual DDL operations like `ALTER TABLE`.
        # https://clickhouse-driver.readthedocs.io/en/latest/dbapi.html#clickhouse_driver.dbapi.connection.Connection.commit
        # https://clickhouse.com/docs/en/guides/developer/transactional#transactions-commit-and-rollback
        caps.supports_transactions = False
        caps.supports_ddl_transactions = False

        caps.supports_truncate_command = True

        caps.supported_merge_strategies = ["delete-insert", "scd2"]

        return caps

    @property
    def client_class(self) -> t.Type["ClickHouseClient"]:
        from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseClient

        return ClickHouseClient

    def __init__(
        self,
        credentials: t.Union[
            ClickHouseCredentials, str, t.Dict[str, t.Any], t.Type["Connection"]
        ] = None,
        destination_name: str = None,
        environment: str = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the ClickHouse destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment
        variables and dlt config files.

        Args:
            credentials: Credentials to connect to the clickhouse database.
                Can be an instance of `ClickHouseCredentials`, or a connection string
                in the format `clickhouse://user:password@host:port/database`.
            **kwargs: Additional arguments passed to the destination config.
        """
        super().__init__(
            credentials=credentials,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
