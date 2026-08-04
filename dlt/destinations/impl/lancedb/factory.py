from typing import Any, Dict, Optional, Sequence, Type, Union, TYPE_CHECKING

from dlt.common.data_writers.escape import escape_lancedb_literal, escape_postgres_identifier
from dlt.common.destination.configuration import ParquetFormatConfiguration
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.destination.capabilities import DataTypeMapper
from dlt.common.exceptions import MissingDependencyException
from dlt.destinations.impl.lance.configuration import LanceEmbeddingsConfiguration
from dlt.destinations.impl.lancedb.configuration import (
    LanceDBCredentials,
    LanceDBClientConfiguration,
)


def _get_type_mapper() -> Type[DataTypeMapper]:
    # lancedb type mapper cannot be used without pyarrow installed; load on demand
    try:
        from dlt.destinations.impl.lancedb.type_mapper import LanceDBTypeMapper

        return LanceDBTypeMapper
    except MissingDependencyException:
        from dlt.common.destination.capabilities import UnsupportedTypeMapper

        return UnsupportedTypeMapper


if TYPE_CHECKING:
    from dlt.common.libs.ibis import BaseBackend
    from dlt.common.schema import Schema
    from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient
    from lancedb.remote.db import RemoteDBConnection


class lancedb(Destination[LanceDBClientConfiguration, "LanceDBClient"]):
    spec = LanceDBClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "parquet"
        caps.supported_loader_file_formats = ["parquet", "reference"]
        caps.type_mapper = _get_type_mapper()

        caps.max_identifier_length = 200
        caps.max_column_identifier_length = 1024
        caps.max_query_length = 8 * 1024 * 1024
        caps.is_max_query_length_in_bytes = False
        caps.max_text_data_type_length = 8 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = False

        # the SQL endpoint reads only, one statement per request
        caps.supports_transactions = False
        caps.supports_ddl_transactions = False
        caps.supports_multiple_statements = False
        caps.sqlglot_dialect = "postgres"
        caps.escape_identifier = escape_postgres_identifier
        caps.escape_literal = escape_lancedb_literal
        # arrow field names are stored and matched verbatim
        caps.has_case_sensitive_identifiers = True

        caps.decimal_precision = (38, 18)
        caps.wei_precision = (38, 0)
        caps.timestamp_precision = 6
        caps.supported_replace_strategies = ["truncate-and-insert"]

        caps.recommended_file_size = 128_000_000

        caps.supported_merge_strategies = ["upsert", "insert-only"]

        # enable creation of nested types to support own vectors
        caps.supports_nested_types = True

        # must store arrow-compatible nested types, not parquet default - otherwise schema checker in lance fails
        caps.parquet_format = ParquetFormatConfiguration(use_compliant_nested_type=False)

        return caps

    @property
    def client_class(self) -> Type["LanceDBClient"]:
        from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient

        return LanceDBClient

    def create_ibis_backend(
        self, client: "LanceDBClient", read_only: bool = False, schemas: Sequence["Schema"] = ()
    ) -> "BaseBackend":
        """Creates the dlt ibis backend, which runs expressions over the Arrow Flight SQL endpoint."""
        from dlt.common.libs.ibis import _DltBackend
        from dlt.destinations.dataset import dataset

        # ibis has no LanceDB backend, so the dlt backend compiles expressions and lets the dlt sql
        # client execute them
        return _DltBackend.from_dataset(
            dataset(self, client.dataset_name, schema=list(schemas) or client.schema)
        )

    def __init__(
        self,
        credentials: Union["RemoteDBConnection", LanceDBCredentials, Dict[str, Any]] = None,
        commit_tag: Optional[str] = None,
        embeddings: Union[LanceEmbeddingsConfiguration, Dict[str, Any]] = None,
        destination_name: str = None,
        environment: str = None,
        **kwargs: Any,
    ) -> None:
        """Configure the LanceDB destination to use in a pipeline.

        Connects to a managed LanceDB Enterprise or Cloud cluster. Each dataset is a database of the
        cluster and reads go through its Arrow Flight SQL endpoint.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials (Union["RemoteDBConnection", LanceDBCredentials, Dict[str, Any]]): Credentials to connect to the
                managed cluster. Can be an instance of `LanceDBCredentials` or
                an already connected managed LanceDB client or
                a dictionary with the credentials parameters.
            commit_tag (Optional[str]): Tag applied to every table of the dataset after a successful
                load, so the dataset can be read back as one tagged version.
            embeddings (Union[LanceEmbeddingsConfiguration, Dict[str, Any]], optional): Embedding provider,
                model, and credentials. If not provided, no vector column is added.
            destination_name (str, optional): Name of the destination, can be used in config section to differentiate between multiple of the same type
            environment (str, optional): Environment of the destination
            **kwargs (Any, optional): Additional arguments forwarded to the destination config
        """
        super().__init__(
            credentials=credentials,
            commit_tag=commit_tag,
            embeddings=embeddings,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


lancedb.register()
