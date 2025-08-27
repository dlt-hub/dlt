from typing import Any, Dict, Type, Union, TYPE_CHECKING, Optional, cast

from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.exceptions import TerminalValueError
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.configuration.specs import GcpServiceAccountCredentials
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.data_writers.escape import escape_hive_identifier, format_bigquery_datetime_literal
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.schema.typing import TColumnSchema, TColumnType
from dlt.common.typing import TLoaderFileFormat

from dlt.destinations.type_mapping import TypeMapperImpl
from dlt.destinations.impl.bigquery.bigquery_adapter import should_autodetect_schema
from dlt.destinations.impl.bigquery.configuration import BigQueryClientConfiguration
from dlt.destinations.utils import parse_db_data_type_str_with_precision


if TYPE_CHECKING:
    from dlt.destinations.impl.bigquery.bigquery import BigQueryClient


class BigQueryTypeMapper(TypeMapperImpl):
    sct_to_unbound_dbt = {
        "json": "JSON",
        "text": "STRING",
        "double": "FLOAT64",
        "bool": "BOOL",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "bigint": "INT64",
        "binary": "BYTES",
        "wei": "BIGNUMERIC",  # non-parametrized should hold wei values
        "time": "TIME",
    }

    sct_to_dbt = {
        "text": "STRING(%i)",
        "binary": "BYTES(%i)",
    }

    dbt_to_sct = {
        "STRING": "text",
        "FLOAT64": "double",
        "BOOL": "bool",
        "DATE": "date",
        "TIMESTAMP": "timestamp",
        "INT64": "bigint",
        "BYTES": "binary",
        "NUMERIC": "decimal",
        "BIGNUMERIC": "decimal",
        "JSON": "json",
        "TIME": "time",
    }

    def ensure_supported_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema,
        loader_file_format: TLoaderFileFormat,
    ) -> None:
        # if table contains json types, we cannot load with parquet
        if (
            loader_file_format == "parquet"
            and column["data_type"] == "json"
            and not should_autodetect_schema(table)
        ):
            raise TerminalValueError(
                "Enable `autodetect_schema` in config or via BigQuery adapter", column["data_type"]
            )

    def to_db_decimal_type(self, column: TColumnSchema) -> str:
        # Use BigQuery's BIGNUMERIC for large precision decimals
        precision, scale = self.decimal_precision(column.get("precision"), column.get("scale"))

        # BigQuery NUMERIC constraints:
        # - When scale = 0: precision must be 1-29
        # - When scale > 0: precision can be up to 38
        # - scale must be <= 9
        if scale == 0 and precision > 29:
            return "BIGNUMERIC(%i,%i)" % (precision, scale)
        elif precision > 38 or scale > 9:
            return "BIGNUMERIC(%i,%i)" % (precision, scale)
        return "NUMERIC(%i,%i)" % (precision, scale)

    # noinspection PyTypeChecker,PydanticTypeChecker
    def from_destination_type(
        self, db_type: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        # precision is present in the type name
        if db_type == "BIGNUMERIC":
            return dict(data_type="wei")
        return super().from_destination_type(*parse_db_data_type_str_with_precision(db_type))


# noinspection PyPep8Naming
class bigquery(Destination[BigQueryClientConfiguration, "BigQueryClient"]):
    spec = BigQueryClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "jsonl"
        caps.supported_loader_file_formats = ["jsonl", "parquet", "model"]
        caps.preferred_staging_file_format = "parquet"
        caps.supported_staging_file_formats = ["parquet", "jsonl"]
        caps.type_mapper = BigQueryTypeMapper
        # BigQuery is by default case sensitive but that cannot be turned off for a dataset
        # https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#case_sensitivity
        caps.escape_identifier = escape_hive_identifier
        caps.escape_literal = None
        caps.has_case_sensitive_identifiers = True
        caps.casefold_identifier = str
        # BQ limit is 4GB but leave a large headroom since buffered writer does not preemptively check size
        caps.recommended_file_size = int(1024 * 1024 * 1024)
        caps.format_datetime_literal = format_bigquery_datetime_literal
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (76, 38)
        caps.max_identifier_length = 1024
        caps.max_column_identifier_length = 300
        caps.max_query_length = 1024 * 1024
        caps.is_max_query_length_in_bytes = False
        caps.max_text_data_type_length = 10 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = False
        caps.supports_clone_table = True
        caps.schema_supports_numeric_precision = False  # no precision information in BigQuery
        caps.supported_merge_strategies = ["delete-insert", "upsert", "scd2"]
        caps.supported_replace_strategies = [
            "truncate-and-insert",
            "insert-from-staging",
            "staging-optimized",
        ]
        caps.sqlglot_dialect = "bigquery"

        return caps

    @property
    def client_class(self) -> Type["BigQueryClient"]:
        from dlt.destinations.impl.bigquery.bigquery import BigQueryClient

        return BigQueryClient

    def __init__(
        self,
        credentials: GcpServiceAccountCredentials = None,
        location: str = None,
        has_case_sensitive_identifiers: bool = None,
        destination_name: str = None,
        environment: str = None,
        **kwargs: Any,
    ) -> None:
        """Configure the MsSql destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials (GcpServiceAccountCredentials, optional): Credentials to connect to the BigQuery database. Can be an instance of `GcpServiceAccountCredentials` or
                a dict or string with service accounts credentials as used in the Google Cloud
            location (str, optional): A location where the datasets will be created, eg. "EU". The default is "US"
            has_case_sensitive_identifiers (bool, optional): Is the dataset case-sensitive, defaults to True
            destination_name (str, optional): Name of the destination, can be used in config section to differentiate between multiple of the same type
            environment (str, optional): Environment of the destination
            **kwargs (Any): Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            location=location,
            has_case_sensitive_identifiers=has_case_sensitive_identifiers,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: BigQueryClientConfiguration,
        naming: Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        # modify the caps if case sensitive identifiers are requested
        if config.should_set_case_sensitivity_on_new_dataset:
            caps.has_case_sensitive_identifiers = config.has_case_sensitive_identifiers
        return super().adjust_capabilities(caps, config, naming)


bigquery.register()
