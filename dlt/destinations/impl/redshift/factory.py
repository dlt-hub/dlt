import typing as t

from dlt.common.data_types.typing import TDataType
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.data_writers.escape import escape_redshift_identifier, escape_redshift_literal
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE

from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.exceptions import TerminalValueError
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.schema.typing import TColumnSchema, TColumnType
from dlt.common.typing import TLoaderFileFormat

from dlt.destinations.type_mapping import TypeMapperImpl
from dlt.destinations.impl.redshift.configuration import (
    RedshiftCredentials,
    RedshiftClientConfiguration,
)

if t.TYPE_CHECKING:
    from dlt.destinations.impl.redshift.redshift import RedshiftClient


class RedshiftTypeMapper(TypeMapperImpl):
    sct_to_unbound_dbt = {
        "json": "super",
        "text": "varchar(max)",
        "double": "double precision",
        "bool": "boolean",
        "date": "date",
        "timestamp": "timestamp with time zone",
        "bigint": "bigint",
        "binary": "varbinary",
        "time": "time without time zone",
    }

    sct_to_dbt = {
        "decimal": "numeric(%i,%i)",
        "wei": "numeric(%i,%i)",
        "text": "varchar(%i)",
        "binary": "varbinary(%i)",
    }

    dbt_to_sct = {
        "super": "json",
        "varchar(max)": "text",
        "double precision": "double",
        "boolean": "bool",
        "date": "date",
        "timestamp with time zone": "timestamp",
        "bigint": "bigint",
        "binary varying": "binary",
        "numeric": "decimal",
        "time without time zone": "time",
        "varchar": "text",
        "smallint": "bigint",
        "integer": "bigint",
    }

    def ensure_supported_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema,
        loader_file_format: TLoaderFileFormat,
    ) -> None:
        if loader_file_format == "insert_values":
            return
        # time not supported on staging file formats
        if column["data_type"] == "time":
            raise TerminalValueError(
                "Please convert `datetime.time` objects in your data to `str` or"
                " `datetime.datetime`.",
                "time",
            )
        if loader_file_format == "jsonl":
            if column["data_type"] == "binary":
                raise TerminalValueError("", "binary")
        if loader_file_format == "parquet":
            # binary not supported on parquet if precision is set
            if column.get("precision") and column["data_type"] == "binary":
                raise TerminalValueError(
                    "Redshift cannot load fixed width VARBYTE columns from parquet files. Switch"
                    " to other file format or use binary columns without precision.",
                    "binary",
                )

    def to_db_integer_type(self, column: TColumnSchema, table: PreparedTableSchema = None) -> str:
        precision = column.get("precision")
        if precision is None:
            return "bigint"
        if precision <= 16:
            return "smallint"
        elif precision <= 32:
            return "integer"
        elif precision <= 64:
            return "bigint"
        raise TerminalValueError(
            f"bigint with {precision} bits precision cannot be mapped into postgres integer type"
        )

    def from_destination_type(
        self, db_type: str, precision: t.Optional[int], scale: t.Optional[int]
    ) -> TColumnType:
        if db_type == "numeric":
            if (precision, scale) == self.capabilities.wei_precision:
                return dict(data_type="wei")
        return super().from_destination_type(db_type, precision, scale)


class redshift(Destination[RedshiftClientConfiguration, "RedshiftClient"]):
    spec = RedshiftClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "insert_values"
        caps.supported_loader_file_formats = ["insert_values"]
        caps.preferred_staging_file_format = "jsonl"
        caps.supported_staging_file_formats = ["jsonl", "parquet"]
        caps.type_mapper = RedshiftTypeMapper
        # redshift is case insensitive and will lower case identifiers when stored
        # you can enable case sensitivity https://docs.aws.amazon.com/redshift/latest/dg/r_enable_case_sensitive_identifier.html
        # then redshift behaves like postgres
        caps.escape_identifier = escape_redshift_identifier
        caps.escape_literal = escape_redshift_literal
        caps.casefold_identifier = str.lower
        caps.has_case_sensitive_identifiers = False
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
        caps.max_identifier_length = 127
        caps.max_column_identifier_length = 127
        caps.max_query_length = 16 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 65535
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = True
        caps.alter_add_multi_column = False
        caps.supported_merge_strategies = ["delete-insert", "scd2"]
        caps.supported_replace_strategies = ["truncate-and-insert", "insert-from-staging"]

        return caps

    @property
    def client_class(self) -> t.Type["RedshiftClient"]:
        from dlt.destinations.impl.redshift.redshift import RedshiftClient

        return RedshiftClient

    def __init__(
        self,
        credentials: t.Union[RedshiftCredentials, t.Dict[str, t.Any], str] = None,
        staging_iam_role: t.Optional[str] = None,
        has_case_sensitive_identifiers: bool = False,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Redshift destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the redshift database. Can be an instance of `RedshiftCredentials` or
                a connection string in the format `redshift://user:password@host:port/database`
            staging_iam_role: IAM role to use for staging data in S3
            has_case_sensitive_identifiers: Are case sensitive identifiers enabled for a database
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            staging_iam_role=staging_iam_role,
            has_case_sensitive_identifiers=has_case_sensitive_identifiers,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: RedshiftClientConfiguration,
        naming: t.Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        # modify the caps if case sensitive identifiers are requested
        if config.has_case_sensitive_identifiers:
            caps.has_case_sensitive_identifiers = True
            caps.casefold_identifier = str
        return super().adjust_capabilities(caps, config, naming)
