import os
from typing import Dict, Any, List, Optional, Union
from enum import Enum
from dataclasses import dataclass
import warnings

from fsspec import AbstractFileSystem
from packaging.version import Version

from dlt import version
from dlt.common import logger
from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.common.time import precise_time
from dlt.common.libs.pyarrow import cast_arrow_schema_types
from dlt.common.libs.utils import load_open_tables
from dlt.common.pipeline import SupportsPipeline
from dlt.common.schema.typing import TWriteDisposition, TTableSchema, _TTableSchemaBase
from dlt.common.schema.utils import get_first_column_name_with_prop, get_columns_names_with_prop
from dlt.common.utils import assert_min_pkg_version
from dlt.common.exceptions import MissingDependencyException
from dlt.common.storages.configuration import FileSystemCredentials, FilesystemConfiguration
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.configuration.specs.mixins import WithPyicebergConfig

from dlt.destinations.impl.filesystem.filesystem import FilesystemClient


try:
    import pyiceberg
    from pyiceberg.table import Table as IcebergTable
    from pyiceberg.catalog import Catalog as IcebergCatalog
    from pyiceberg.exceptions import NoSuchTableError
    from pyiceberg.partitioning import (
        UNPARTITIONED_PARTITION_SPEC,
        PartitionSpec as IcebergPartitionSpec,
    )
    import pyarrow as pa
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dlt pyiceberg helpers",
        [f"{version.DLT_PKG_NAME}[pyiceberg]"],
        "Install `pyiceberg` so dlt can create Iceberg tables in the `filesystem` destination.",
    )

pyiceberg_semver = Version(pyiceberg.__version__)

if pyiceberg_semver < Version("0.10.0"):
    import pyiceberg.io.pyarrow as _pio

    _orig_get_kwargs = _pio._get_parquet_writer_kwargs

    def _patched_get_parquet_writer_kwargs(table_properties):  # type: ignore[no-untyped-def]
        """Return the original kwargs **plus** store_decimal_as_integer=True."""
        kwargs = _orig_get_kwargs(table_properties)
        kwargs.setdefault("store_decimal_as_integer", True)
        return kwargs

    _pio._get_parquet_writer_kwargs = _patched_get_parquet_writer_kwargs


class PartitionType(Enum):
    """Supported Iceberg partition transform types."""

    IDENTITY = "identity"
    BUCKET = "bucket"
    TRUNCATE = "truncate"
    DAY = "day"
    MONTH = "month"
    YEAR = "year"
    HOUR = "hour"


@dataclass
class PartitionSpec:
    """Specification for an Iceberg partition."""

    column: str
    partition_type: str
    index: int  # to keep the order of partition in place
    name: Optional[str] = None
    bucket_count: Optional[int] = None


class IcebergPartitionManager:
    """Manages creation and application of Iceberg partition specifications."""

    @staticmethod
    def apply_partitioning(update_spec: Any, partition_specs: List[Dict[str, Any]]) -> None:
        from pyiceberg.transforms import (
            IdentityTransform,
            BucketTransform,
            TruncateTransform,
            YearTransform,
            MonthTransform,
            DayTransform,
            HourTransform,
        )

        transform_map = {
            "identity": IdentityTransform,
            "bucket": BucketTransform,
            "truncate": TruncateTransform,
            "year": YearTransform,
            "month": MonthTransform,
            "day": DayTransform,
            "hour": HourTransform,
        }

        for spec_dict in partition_specs:
            column = spec_dict["column"]
            transform_type = spec_dict["type"]
            partition_name = spec_dict.get("name")
            bucket_count = spec_dict.get("bucket_count")

            try:
                if transform_type == "identity":
                    update_spec.add_identity(column)
                elif transform_type == "bucket":
                    if not bucket_count:
                        raise ValueError(f"bucket_count required for bucket transform on {column}")
                    transform: Any = BucketTransform(bucket_count)
                    update_spec.add_field(column, transform, partition_name)
                else:
                    if transform_type not in transform_map:
                        continue

                    transform_class = transform_map[transform_type]
                    transform = transform_class()
                    update_spec.add_field(column, transform, partition_name)

            except Exception as e:
                logger.warning(
                    f"Failed to apply {transform_type} partition to column '{column}': {str(e)}."
                )
                continue


def _validate_partition_spec(spec: Dict[str, Any]) -> bool:
    """Validate partition specification structure and requirements."""
    column = spec.get("column", "unknown")
    partition_type = spec.get("type", "identity")

    # Validate bucket partition has bucket_count
    if partition_type == "bucket" and not spec.get("bucket_count"):
        logger.warning(
            f"Bucket partition on column '{column}' missing required 'bucket_count'. Skipping."
        )
        return False

    # Validate bucket_count is positive integer
    if partition_type == "bucket":
        bucket_count = spec.get("bucket_count")
        if not isinstance(bucket_count, int) or bucket_count <= 0:
            logger.warning(
                f"Bucket partition on column '{column}' has invalid bucket_count: {bucket_count}. "
                "Must be positive integer. Skipping."
            )
            return False

    # Validate index is positive integer
    index = spec.get("index")
    if not isinstance(index, int) or index <= 0:
        logger.warning(
            f"Partition on column '{column}' has invalid index: {index}. "
            "Must be positive integer. Skipping."
        )
        return False

    # Validate supported partition types
    supported_types = {"identity", "bucket", "truncate", "day", "month", "year", "hour"}
    if partition_type not in supported_types:
        logger.warning(
            f"Partition on column '{column}' has unsupported type: '{partition_type}'. "
            f"Supported types: {sorted(supported_types)}. Skipping."
        )
        return False

    return True


def _validate_and_fix_indices(specs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Validate partition indices are unique and sequential, auto-fix if needed."""
    if not specs:
        return specs

    # Check for duplicate indices
    indices = [spec["index"] for spec in specs]
    if len(set(indices)) == len(indices):
        return specs  # No duplicates, return as-is

    # TODO: Sort by original index, then assign sequential indices
    # sorted_specs = sorted(specs, key=lambda x: (x["index"], x["column"]))
    # for i, spec in enumerate(sorted_specs, 1):
    #     original_index = spec["index"]
    #     spec["index"] = i
    #     if original_index != i:
    #         logger.info(
    #             f"Partition on column '{spec['column']}' index changed: {original_index} → {i}"
    #         )
    # return sorted_specs
    raise ValueError("Duplicate partition indices found. Please fix the schema.")


def extract_partition_specs_from_schema(
    table_schema: Union[Dict[str, Any], _TTableSchemaBase], arrow_schema: pa.Schema
) -> List[Dict[str, Any]]:
    """Extract partition specifications from dlt table schema."""
    """
    Supports both legacy and advanced partitioning with priority system:
    - Legacy: partition: True -> identity partitioning
    - Advanced: partition: {...} or partition: [...] -> custom transforms
    - Priority: Advanced takes precedence over legacy (prevents conflicts)

    Args:
        table_schema: dlt table schema dictionary
        arrow_schema: PyArrow schema for the table

    Returns:
        List of partition specification dictionaries, sorted by index
    """
    columns = table_schema.get("columns", {})

    # Performance optimization: Early exit if no partition hints found
    if not any("partition" in col for col in columns.values()):
        return []

    advanced_partitions: List[Dict[str, Any]] = []
    legacy_partitions: List[Dict[str, Any]] = []

    for column_name, column_config in columns.items():
        partition_hint = column_config.get("partition")

        if partition_hint is None:
            continue

        if partition_hint is True:
            # Legacy partitioning: simple identity
            legacy_partitions.append(
                {
                    "column": column_name,
                    "type": "identity",
                    "index": len(legacy_partitions) + 1,  # Auto-assign index
                    "name": None,
                    "bucket_count": None,
                }
            )

        elif isinstance(partition_hint, dict):
            # Advanced single partition
            spec = {
                "column": column_name,
                "type": partition_hint.get("type", "identity"),
                "index": partition_hint.get("index", 1),
                "name": partition_hint.get("name"),
                "bucket_count": partition_hint.get("bucket_count"),
            }
            # Validate before adding
            if _validate_partition_spec(spec):
                advanced_partitions.append(spec)

        elif isinstance(partition_hint, list):
            # Advanced multiple partitions on same column
            for spec_dict in partition_hint:
                if isinstance(spec_dict, dict):
                    spec = {
                        "column": column_name,
                        "type": spec_dict.get("type", "identity"),
                        "index": spec_dict.get("index", 1),
                        "name": spec_dict.get("name"),
                        "bucket_count": spec_dict.get("bucket_count"),
                    }
                    # Validate before adding
                    if _validate_partition_spec(spec):
                        advanced_partitions.append(spec)

    # Priority system: advanced partitions override legacy ones
    if advanced_partitions:
        # Validate and fix indices, then sort by corrected index
        validated_specs = _validate_and_fix_indices(advanced_partitions)
        return sorted(validated_specs, key=lambda x: x["index"])
    elif legacy_partitions:
        logger.info(f"Using legacy partitioning ({len(legacy_partitions)} partitions)")
        return legacy_partitions
    else:
        return []


def ensure_iceberg_compatible_arrow_schema(schema: pa.Schema) -> pa.Schema:
    ARROW_TO_ICEBERG_COMPATIBLE_ARROW_TYPE_MAP = {
        pa.types.is_time32: pa.time64("us"),
        pa.types.is_decimal256: pa.string(),  # pyarrow does not allow downcasting to decimal128
        pa.types.is_dictionary: lambda t_: t_.value_type,
    }
    return cast_arrow_schema_types(schema, ARROW_TO_ICEBERG_COMPATIBLE_ARROW_TYPE_MAP)


def ensure_iceberg_compatible_arrow_data(data: pa.Table) -> pa.Table:
    schema = ensure_iceberg_compatible_arrow_schema(data.schema)
    return data.cast(schema)


def write_iceberg_table(
    table: IcebergTable,
    data: pa.Table,
    write_disposition: TWriteDisposition,
) -> None:
    start_ts = precise_time()
    if write_disposition == "append":
        table.append(ensure_iceberg_compatible_arrow_data(data))
    elif write_disposition == "replace":
        table.overwrite(ensure_iceberg_compatible_arrow_data(data))
    logger.debug(
        f"pyiceberg: {write_disposition} arrow with {data.num_rows} rows to table {table.name()} at"
        f" location {table.location()} took {(precise_time() - start_ts)} seconds."
    )


def merge_iceberg_table(
    table: IcebergTable,
    data: pa.Table,
    schema: TTableSchema,
    load_table_name: str,
) -> None:
    """Merges in-memory Arrow data into on-disk Iceberg table."""
    strategy = schema["x-merge-strategy"]  # type: ignore[typeddict-item]
    if strategy == "upsert":
        # evolve schema
        with table.update_schema() as update:
            update.union_by_name(ensure_iceberg_compatible_arrow_schema(data.schema))

        if "parent" in schema:
            join_cols = [get_first_column_name_with_prop(schema, "unique")]
        else:
            join_cols = get_columns_names_with_prop(schema, "primary_key")

        # TODO: replace the batching method with transaction with pyiceberg's release after 0.9.1
        for rb in data.to_batches(max_chunksize=1_000):
            batch_tbl = pa.Table.from_batches([rb])
            batch_tbl = ensure_iceberg_compatible_arrow_data(batch_tbl)

            table.upsert(
                df=batch_tbl,
                join_cols=join_cols,
                when_matched_update_all=True,
                when_not_matched_insert_all=True,
                case_sensitive=True,
            )
    else:
        raise ValueError(
            f'Merge strategy "{strategy}" is not supported for Iceberg tables. '
            f'Table: "{load_table_name}".'
        )


def get_sql_catalog(
    catalog_name: str,
    uri: str,
    credentials: FileSystemCredentials,
    properties: Dict[str, Any] = None,
) -> IcebergCatalog:  # noqa: F821
    assert_min_pkg_version(
        pkg_name="sqlalchemy",
        version="2.0.18",
        msg=(
            "`sqlalchemy>=2.0.18` is needed for `iceberg` table format on `filesystem` destination."
        ),
    )

    from pyiceberg.catalog.sql import SqlCatalog

    return SqlCatalog(
        catalog_name,
        uri=uri,
        **_get_fileio_config(credentials),
        **(properties or {}),
    )


def evolve_table(
    catalog: IcebergCatalog,
    client: FilesystemClient,
    table_id: str,
    table_location: str,
    schema: Optional[pa.Schema] = None,
) -> IcebergTable:
    try:
        table = catalog.load_table(table_id)
    except NoSuchTableError:
        # add table to catalog
        metadata_path = f"{table_location.rstrip('/')}/metadata"
        if client.fs_client.exists(metadata_path):
            # found metadata; register existing table
            table = register_table(
                table_id, metadata_path, catalog, client.fs_client, client.config
            )
        else:
            raise

    # evolve schema
    if schema is not None:
        with table.update_schema() as update:
            update.union_by_name(ensure_iceberg_compatible_arrow_schema(schema))

    return table


def create_table(
    catalog: IcebergCatalog,
    table_id: str,
    table_location: str,
    schema: Union[pa.Schema, "pyiceberg.schema.Schema"],
    partition_columns: Optional[List[str]] = None,
    partition_spec: Optional[IcebergPartitionSpec] = UNPARTITIONED_PARTITION_SPEC,
) -> None:
    if isinstance(schema, pa.Schema):
        schema = ensure_iceberg_compatible_arrow_schema(schema)

    if partition_columns:
        warnings.warn(
            "partition_columns is deprecated. Use partition_spec instead.", DeprecationWarning
        )
        with catalog.create_table_transaction(
            table_id,
            schema=schema,
            location=table_location,
        ) as txn:
            # add partitioning
            with txn.update_spec() as update_spec:
                for col in partition_columns:
                    update_spec.add_identity(col)
    else:
        catalog.create_table(
            identifier=table_id,
            schema=schema,
            location=table_location,
            partition_spec=partition_spec,
        )


def get_iceberg_tables(
    pipeline: SupportsPipeline,
    *tables: str,
    schema_name: Optional[str] = None,
    include_dlt_tables: bool = False,
) -> Dict[str, IcebergTable]:
    """Returns Iceberg tables in `pipeline.default_schema (default)` or `schema_name` as `pyiceberg.Table` objects.

    Returned object is a dictionary with table names as keys and `Tables` objects as values.
    Optionally filters dictionary by table names specified as `*tables*`.
    Raises ValueError if table name specified as `*tables` is not found. You may try to switch to other
    schemas via `schema_name` argument.
    """
    return load_open_tables(
        pipeline, "iceberg", *tables, schema_name=schema_name, include_dlt_tables=include_dlt_tables
    )


def _get_fileio_config(credentials: CredentialsConfiguration) -> Dict[str, Any]:
    if isinstance(credentials, WithPyicebergConfig):
        return credentials.to_pyiceberg_fileio_config()
    return {}


def get_last_metadata_file(
    metadata_path: str, fs_client: AbstractFileSystem, config: FilesystemConfiguration
) -> str:
    # TODO: read version-hint.txt first and save it in filesystem
    try:
        metadata_files = [f for f in fs_client.ls(metadata_path) if f.endswith(".json")]
    except FileNotFoundError:
        raise DestinationUndefinedEntity(FileNotFoundError(metadata_path))
    if len(metadata_files) == 0:
        raise DestinationUndefinedEntity(FileNotFoundError(metadata_path))
    return make_location(sorted(metadata_files)[-1], config)


def register_table(
    identifier: str,
    metadata_path: str,
    catalog: IcebergCatalog,
    fs_client: AbstractFileSystem,
    config: FilesystemConfiguration,
) -> IcebergTable:
    last_metadata_file = get_last_metadata_file(metadata_path, fs_client, config)
    return catalog.register_table(identifier, last_metadata_file)


def make_location(path: str, config: FilesystemConfiguration) -> str:
    # don't use file protocol for local files because duckdb does not support it
    # https://github.com/duckdb/duckdb/issues/13669
    location = config.make_url(path)
    if config.is_local_filesystem and os.name == "nt":
        # pyiceberg cannot deal with windows absolute urls
        location = location.replace("file:///", "file://")
    return location
