import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext, TLoaderFileFormat
from dlt.common.destination.reference import DEFAULT_FILE_LAYOUT
from dlt.common.schema.typing import TLoaderMergeStrategy, TTableSchema
from dlt.common.storages.configuration import FileSystemCredentials

from dlt.destinations.impl.filesystem.configuration import FilesystemDestinationClientConfiguration
from dlt.destinations.impl.filesystem.typing import TCurrentDateTime, TExtraPlaceholders

if t.TYPE_CHECKING:
    from dlt.destinations.impl.filesystem.filesystem import FilesystemClient


def filesystem_loader_file_format_selector(
    preferred_loader_file_format: TLoaderFileFormat,
    supported_loader_file_formats: t.Sequence[TLoaderFileFormat],
    /,
    *,
    table_schema: TTableSchema,
) -> t.Tuple[TLoaderFileFormat, t.Sequence[TLoaderFileFormat]]:
    if table_schema.get("table_format") == "delta":
        return ("parquet", ["parquet"])
    return (preferred_loader_file_format, supported_loader_file_formats)


def filesystem_merge_strategies_selector(
    supported_merge_strategies: t.Sequence[TLoaderMergeStrategy],
    /,
    *,
    table_schema: TTableSchema,
) -> t.Sequence[TLoaderMergeStrategy]:
    if table_schema.get("table_format") == "delta":
        return supported_merge_strategies
    else:
        return []


class filesystem(Destination[FilesystemDestinationClientConfiguration, "FilesystemClient"]):
    spec = FilesystemDestinationClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext.generic_capabilities(
            preferred_loader_file_format="jsonl",
            loader_file_format_selector=filesystem_loader_file_format_selector,
            supported_table_formats=["delta"],
            supported_merge_strategies=["upsert"],
            merge_strategies_selector=filesystem_merge_strategies_selector,
        )
        caps.supported_loader_file_formats = list(caps.supported_loader_file_formats) + [
            "reference",
        ]
        caps.has_case_sensitive_identifiers = True
        caps.supported_replace_strategies = ["truncate-and-insert", "insert-from-staging"]
        return caps

    @property
    def client_class(self) -> t.Type["FilesystemClient"]:
        from dlt.destinations.impl.filesystem.filesystem import FilesystemClient

        return FilesystemClient

    def __init__(
        self,
        bucket_url: str = None,
        credentials: t.Union[FileSystemCredentials, t.Dict[str, t.Any], t.Any] = None,
        layout: str = DEFAULT_FILE_LAYOUT,
        extra_placeholders: t.Optional[TExtraPlaceholders] = None,
        current_datetime: t.Optional[TCurrentDateTime] = None,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
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
            bucket_url: The fsspec compatible bucket url to use for the destination.
            credentials: Credentials to connect to the filesystem. The type of credentials should correspond to
                the bucket protocol. For example, for AWS S3, the credentials should be an instance of `AwsCredentials`.
                A dictionary with the credentials parameters can also be provided.
            layout (str): A layout of the files holding table data in the destination bucket/filesystem. Uses a set of pre-defined
                and user-defined (extra) placeholders. Please refer to https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem#files-layout
            extra_placeholders (dict(str, str | callable)): A dictionary of extra placeholder names that can be used in the `layout` parameter. Names
                are mapped to string values or to callables evaluated at runtime.
            current_datetime (DateTime | callable): current datetime used by date/time related placeholders. If not provided, load package creation timestamp
                will be used.
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            bucket_url=bucket_url,
            credentials=credentials,
            layout=layout,
            extra_placeholders=extra_placeholders,
            current_datetime=current_datetime,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
