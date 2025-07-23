from typing import Any, Optional, Type, Union, Dict, TYPE_CHECKING, Sequence, Tuple

from dlt.common.destination import Destination, DestinationCapabilitiesContext, TLoaderFileFormat
from dlt.common.destination.client import DEFAULT_FILE_LAYOUT
from dlt.common.schema.typing import TLoaderMergeStrategy, TLoaderReplaceStrategy, TTableSchema
from dlt.common.storages.configuration import FileSystemCredentials

from dlt.destinations.impl.filesystem.configuration import FilesystemDestinationClientConfiguration
from dlt.destinations.impl.filesystem.typing import TCurrentDateTime, TExtraPlaceholders

if TYPE_CHECKING:
    from dlt.destinations.impl.filesystem.filesystem import FilesystemClient


def filesystem_loader_file_format_selector(
    preferred_loader_file_format: TLoaderFileFormat,
    supported_loader_file_formats: Sequence[TLoaderFileFormat],
    /,
    *,
    table_schema: TTableSchema,
) -> Tuple[TLoaderFileFormat, Sequence[TLoaderFileFormat]]:
    if table_schema.get("table_format") in ("delta", "iceberg"):
        return ("parquet", ["parquet"])
    return (preferred_loader_file_format, supported_loader_file_formats)


def filesystem_merge_strategies_selector(
    supported_merge_strategies: Sequence[TLoaderMergeStrategy],
    /,
    *,
    table_schema: TTableSchema,
) -> Sequence[TLoaderMergeStrategy]:
    if table_schema.get("table_format") in ["delta", "iceberg"]:
        return supported_merge_strategies
    else:
        return []


def filesystem_replace_strategies_selector(
    supported_replace_strategies: Sequence[TLoaderReplaceStrategy],
    /,
    *,
    table_schema: TTableSchema,
) -> Sequence[TLoaderReplaceStrategy]:
    if table_schema.get("table_format") in ("iceberg", "delta"):
        # always from staging table
        return ["insert-from-staging"]
    else:
        # only truncate and insert for regular tables
        return ["truncate-and-insert"]


class filesystem(Destination[FilesystemDestinationClientConfiguration, "FilesystemClient"]):
    spec = FilesystemDestinationClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext.generic_capabilities(
            preferred_loader_file_format="jsonl",
            loader_file_format_selector=filesystem_loader_file_format_selector,
            supported_table_formats=["delta", "iceberg"],
            supported_merge_strategies=["upsert"],
            merge_strategies_selector=filesystem_merge_strategies_selector,
        )
        caps.supported_loader_file_formats = list(caps.supported_loader_file_formats) + [
            "reference",
        ]
        caps.has_case_sensitive_identifiers = True
        # for delta and iceberg this is copy from staging, use replace strategy selector
        caps.supported_replace_strategies = ["truncate-and-insert", "insert-from-staging"]
        caps.replace_strategies_selector = filesystem_replace_strategies_selector
        caps.enforces_nulls_on_alter = False
        caps.sqlglot_dialect = "duckdb"
        caps.supports_nested_types = True

        return caps

    @property
    def client_class(self) -> Type["FilesystemClient"]:
        from dlt.destinations.impl.filesystem.filesystem import FilesystemClient

        return FilesystemClient

    def __init__(
        self,
        bucket_url: str = None,
        credentials: Union[FileSystemCredentials, Dict[str, Any], Any] = None,
        layout: str = DEFAULT_FILE_LAYOUT,
        extra_placeholders: Optional[TExtraPlaceholders] = None,
        current_datetime: Optional[TCurrentDateTime] = None,
        always_refresh_views: bool = None,
        destination_name: str = None,
        environment: str = None,
        **kwargs: Any,
    ) -> None:
        """Configure the filesystem destination to use in a pipeline and load data to local or remote filesystem.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        The `bucket_url` determines the protocol to be used:

        - Local folder: `file:///path/to/directory`
        - AWS S3 (and S3 compatible storages): `s3://bucket-name
        - Azure Blob Storage: `az://container-name
        - Google Cloud Storage: `gs://bucket-name
        - Memory fs: `memory://m`

        Args:
            bucket_url (str, optional): The fsspec compatible bucket url to use for the destination.
            credentials (Union[FileSystemCredentials, Dict[str, Any], Any], optional): Credentials to connect to the filesystem. The type of credentials should correspond to
                the bucket protocol. For example, for AWS S3, the credentials should be an instance of `AwsCredentials`.
                A dictionary with the credentials parameters can also be provided.
            layout (str, optional): A layout of the files holding table data in the destination bucket/filesystem. Uses a set of pre-defined
                and user-defined (extra) placeholders. Please refer to https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem#files-layout
            extra_placeholders (Optional[TExtraPlaceholders]): A dictionary of extra placeholder names that can be used in the `layout` parameter. Names
                are mapped to string values or to callables evaluated at runtime.
            current_datetime (Optional[TCurrentDateTime]): Current datetime used by date/time related placeholders. If not provided, load package creation timestamp
                will be used.
            always_refresh_views (bool, optional): Always refresh sql_client views by setting the newest table metadata or globbing table files
            destination_name (str, optional): Name of the destination, can be used in config section to differentiate between multiple of the same type
            environment (str, optional): Environment of the destination
            **kwargs (Any): Additional arguments passed to the destination config
        """
        super().__init__(
            bucket_url=bucket_url,
            credentials=credentials,
            layout=layout,
            extra_placeholders=extra_placeholders,
            current_datetime=current_datetime,
            always_refresh_views=always_refresh_views,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


filesystem.register()
