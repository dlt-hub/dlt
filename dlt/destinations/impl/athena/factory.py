import typing as t

from dlt.common.data_types.typing import TDataType
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.configuration.specs import AwsCredentials
from dlt.common.data_writers.escape import (
    escape_athena_identifier,
    format_bigquery_datetime_literal,
)
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema.typing import TColumnSchema, TColumnType, TLoaderMergeStrategy, TTableSchema
from dlt.common.typing import TLoaderFileFormat
from dlt.common.utils import without_none

from dlt.destinations.type_mapping import TypeMapperImpl
from dlt.destinations.impl.athena.configuration import AthenaClientConfiguration

if t.TYPE_CHECKING:
    from dlt.destinations.impl.athena.athena import AthenaClient


def athena_merge_strategies_selector(
    supported_merge_strategies: t.Sequence[TLoaderMergeStrategy],
    /,
    *,
    table_schema: TTableSchema,
) -> t.Sequence[TLoaderMergeStrategy]:
    if table_schema.get("table_format") == "iceberg":
        return supported_merge_strategies
    else:
        return []


class AthenaTypeMapper(TypeMapperImpl):
    sct_to_unbound_dbt = {
        "json": "string",
        "text": "string",
        "double": "double",
        "bool": "boolean",
        "date": "date",
        "timestamp": "timestamp",
        "bigint": "bigint",
        "binary": "binary",
        "time": "string",
    }

    sct_to_dbt = {"decimal": "decimal(%i,%i)", "wei": "decimal(%i,%i)"}

    dbt_to_sct = {
        "varchar": "text",
        "double": "double",
        "boolean": "bool",
        "date": "date",
        "timestamp": "timestamp",
        "bigint": "bigint",
        "binary": "binary",
        "varbinary": "binary",
        "decimal": "decimal",
        "tinyint": "bigint",
        "smallint": "bigint",
        "int": "bigint",
    }

    def ensure_supported_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema,
        loader_file_format: TLoaderFileFormat,
    ) -> None:
        # TIME is not supported for parquet on Athena
        if loader_file_format == "parquet" and column["data_type"] == "time":
            raise TerminalValueError(
                "Please convert `datetime.time` objects in your data to `str` or"
                " `datetime.datetime`.",
                "time",
            )

    def to_db_integer_type(self, column: TColumnSchema, table: PreparedTableSchema = None) -> str:
        precision = column.get("precision")
        table_format = table.get("table_format")
        if precision is None:
            return "bigint"
        if precision <= 8:
            return "int" if table_format == "iceberg" else "tinyint"
        elif precision <= 16:
            return "int" if table_format == "iceberg" else "smallint"
        elif precision <= 32:
            return "int"
        elif precision <= 64:
            return "bigint"
        raise TerminalValueError(
            f"bigint with {precision} bits precision cannot be mapped into athena integer type"
        )

    def from_destination_type(
        self, db_type: str, precision: t.Optional[int], scale: t.Optional[int]
    ) -> TColumnType:
        for key, val in self.dbt_to_sct.items():
            if db_type.startswith(key):
                return without_none(dict(data_type=val, precision=precision, scale=scale))  # type: ignore[return-value]
        return dict(data_type=None)


class athena(Destination[AthenaClientConfiguration, "AthenaClient"]):
    spec = AthenaClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        # athena only supports loading from staged files on s3 for now
        caps.preferred_loader_file_format = None
        caps.supported_loader_file_formats = []
        caps.supported_table_formats = ["iceberg", "hive"]
        caps.preferred_staging_file_format = "parquet"
        caps.supported_staging_file_formats = ["parquet"]
        caps.type_mapper = AthenaTypeMapper

        # athena is storing all identifiers in lower case and is case insensitive
        # it also uses lower case in all the queries
        # https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html
        caps.escape_identifier = escape_athena_identifier
        caps.casefold_identifier = str.lower
        caps.has_case_sensitive_identifiers = False
        caps.format_datetime_literal = format_bigquery_datetime_literal
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
        caps.max_identifier_length = 255
        caps.max_column_identifier_length = 255
        caps.max_query_length = 16 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 262144
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = False
        caps.supports_transactions = False
        caps.alter_add_multi_column = True
        caps.schema_supports_numeric_precision = False
        caps.timestamp_precision = 3
        caps.supports_truncate_command = False
        caps.supported_merge_strategies = ["delete-insert", "upsert", "scd2"]
        caps.supported_replace_strategies = ["truncate-and-insert", "insert-from-staging"]
        caps.merge_strategies_selector = athena_merge_strategies_selector
        return caps

    @property
    def client_class(self) -> t.Type["AthenaClient"]:
        from dlt.destinations.impl.athena.athena import AthenaClient

        return AthenaClient

    def __init__(
        self,
        query_result_bucket: t.Optional[str] = None,
        credentials: t.Union[AwsCredentials, t.Dict[str, t.Any], t.Any] = None,
        athena_work_group: t.Optional[str] = None,
        aws_data_catalog: t.Optional[str] = "awsdatacatalog",
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Athena destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            query_result_bucket: S3 bucket to store query results in
            credentials: AWS credentials to connect to the Athena database.
            athena_work_group: Athena work group to use
            aws_data_catalog: Athena data catalog to use
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            query_result_bucket=query_result_bucket,
            credentials=credentials,
            athena_work_group=athena_work_group,
            aws_data_catalog=aws_data_catalog,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
