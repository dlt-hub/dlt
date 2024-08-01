import pytest
import contextlib
import codecs
import os
from typing import Any, Iterator, List, Sequence, IO, Tuple, Optional, Dict, Union, Generator, cast
import shutil
from pathlib import Path
from urllib.parse import urlparse
from dataclasses import dataclass

import dlt
from dlt.common import json, sleep
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.destination.reference import (
    DestinationClientDwhConfiguration,
    JobClientBase,
    LoadJob,
    DestinationClientStagingConfiguration,
    TDestinationReferenceArg,
    WithStagingDataset,
)
from dlt.common.destination import TLoaderFileFormat, Destination
from dlt.common.destination.reference import DEFAULT_FILE_LAYOUT
from dlt.common.data_writers import DataWriter
from dlt.common.pipeline import PipelineContext
from dlt.common.schema import TTableSchemaColumns, Schema
from dlt.common.schema.typing import TTableFormat
from dlt.common.storages import SchemaStorage, FileStorage, SchemaStorageConfiguration
from dlt.common.schema.utils import new_table, normalize_table_identifiers
from dlt.common.storages import ParsedLoadJobFileName, LoadStorage, PackageStorage
from dlt.common.typing import StrAny
from dlt.common.utils import uniq_id

from dlt.destinations.exceptions import CantExtractTablePrefix
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.job_client_impl import SqlJobClientBase

from tests.utils import (
    ACTIVE_DESTINATIONS,
    IMPLEMENTED_DESTINATIONS,
    SQL_DESTINATIONS,
    EXCLUDED_DESTINATION_CONFIGURATIONS,
)
from tests.cases import (
    TABLE_UPDATE_COLUMNS_SCHEMA,
    TABLE_UPDATE,
    TABLE_ROW_ALL_DATA_TYPES,
    assert_all_data_types_row,
)

# Bucket urls.
AWS_BUCKET = dlt.config.get("tests.bucket_url_s3", str)
GCS_BUCKET = dlt.config.get("tests.bucket_url_gs", str)
AZ_BUCKET = dlt.config.get("tests.bucket_url_az", str)
GDRIVE_BUCKET = dlt.config.get("tests.bucket_url_gdrive", str)
FILE_BUCKET = dlt.config.get("tests.bucket_url_file", str)
R2_BUCKET = dlt.config.get("tests.bucket_url_r2", str)
MEMORY_BUCKET = dlt.config.get("tests.memory", str)

ALL_FILESYSTEM_DRIVERS = dlt.config.get("ALL_FILESYSTEM_DRIVERS", list) or [
    "s3",
    "gs",
    "az",
    "gdrive",
    "file",
    "memory",
    "r2",
]

# Filter out buckets not in all filesystem drivers
WITH_GDRIVE_BUCKETS = [GCS_BUCKET, AWS_BUCKET, FILE_BUCKET, MEMORY_BUCKET, AZ_BUCKET, GDRIVE_BUCKET]
WITH_GDRIVE_BUCKETS = [
    bucket
    for bucket in WITH_GDRIVE_BUCKETS
    if (urlparse(bucket).scheme or "file") in ALL_FILESYSTEM_DRIVERS
]

# temporary solution to include gdrive bucket in tests,
# while gdrive is not working as a destination
DEFAULT_BUCKETS = [bucket for bucket in WITH_GDRIVE_BUCKETS if bucket != GDRIVE_BUCKET]

# Add r2 in extra buckets so it's not run for all tests
R2_BUCKET_CONFIG = dict(
    bucket_url=R2_BUCKET,
    # Credentials included so we can override aws credentials in env later
    credentials=dict(
        aws_access_key_id=dlt.config.get("tests.r2_aws_access_key_id", str),
        aws_secret_access_key=dlt.config.get("tests.r2_aws_secret_access_key", str),
        endpoint_url=dlt.config.get("tests.r2_endpoint_url", str),
    ),
)

# interesting filesystem layouts for basic tests
FILE_LAYOUT_CLASSIC = "{schema_name}.{table_name}.{load_id}.{file_id}.{ext}"
FILE_LAYOUT_MANY_TABLES_ONE_FOLDER = "{table_name}88{load_id}-u-{file_id}.{ext}"
FILE_LAYOUT_TABLE_IN_MANY_FOLDERS = "{table_name}/{load_id}/{file_id}.{ext}"
FILE_LAYOUT_TABLE_NOT_FIRST = "{schema_name}/{table_name}/{load_id}/{file_id}.{ext}"

TEST_FILE_LAYOUTS = [
    DEFAULT_FILE_LAYOUT,
    FILE_LAYOUT_CLASSIC,
    FILE_LAYOUT_MANY_TABLES_ONE_FOLDER,
    FILE_LAYOUT_TABLE_IN_MANY_FOLDERS,
    FILE_LAYOUT_TABLE_NOT_FIRST,
]

EXTRA_BUCKETS: List[Dict[str, Any]] = []
if "r2" in ALL_FILESYSTEM_DRIVERS:
    EXTRA_BUCKETS.append(R2_BUCKET_CONFIG)

ALL_BUCKETS = DEFAULT_BUCKETS + EXTRA_BUCKETS


@dataclass
class DestinationTestConfiguration:
    """Class for defining test setup for one destination."""

    destination: str
    staging: Optional[TDestinationReferenceArg] = None
    file_format: Optional[TLoaderFileFormat] = None
    table_format: Optional[TTableFormat] = None
    bucket_url: Optional[str] = None
    stage_name: Optional[str] = None
    staging_iam_role: Optional[str] = None
    staging_use_msi: bool = False
    extra_info: Optional[str] = None
    supports_merge: bool = True  # TODO: take it from client base class
    force_iceberg: bool = False
    supports_dbt: bool = True
    disable_compression: bool = False
    dev_mode: bool = False
    credentials: Optional[Union[CredentialsConfiguration, Dict[str, Any]]] = None
    env_vars: Optional[Dict[str, str]] = None

    @property
    def name(self) -> str:
        name: str = self.destination
        if self.file_format:
            name += f"-{self.file_format}"
        if self.table_format:
            name += f"-{self.table_format}"
        if not self.staging:
            name += "-no-staging"
        else:
            name += "-staging"
        if self.extra_info:
            name += f"-{self.extra_info}"
        return name

    @property
    def factory_kwargs(self) -> Dict[str, Any]:
        return {
            k: getattr(self, k)
            for k in [
                "bucket_url",
                "stage_name",
                "staging_iam_role",
                "staging_use_msi",
                "force_iceberg",
            ]
            if getattr(self, k, None) is not None
        }

    def setup(self) -> None:
        """Sets up environment variables for this destination configuration"""
        for k, v in self.factory_kwargs.items():
            os.environ[f"DESTINATION__{k.upper()}"] = str(v)

        # For the filesystem destinations we disable compression to make analyzing the result easier
        if self.destination == "filesystem" or self.disable_compression:
            os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "True"

        if self.credentials is not None:
            for key, value in dict(self.credentials).items():
                os.environ[f"DESTINATION__CREDENTIALS__{key.upper()}"] = str(value)

        if self.env_vars is not None:
            for k, v in self.env_vars.items():
                os.environ[k] = v

    def setup_pipeline(
        self, pipeline_name: str, dataset_name: str = None, dev_mode: bool = False, **kwargs
    ) -> dlt.Pipeline:
        """Convenience method to setup pipeline with this configuration"""
        self.dev_mode = dev_mode
        self.setup()
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=kwargs.pop("destination", self.destination),
            staging=kwargs.pop("staging", self.staging),
            dataset_name=dataset_name or pipeline_name,
            dev_mode=dev_mode,
            **kwargs,
        )
        return pipeline

    def attach_pipeline(self, pipeline_name: str, **kwargs) -> dlt.Pipeline:
        """Attach to existing pipeline keeping the dev_mode"""
        # remember dev_mode from setup_pipeline
        pipeline = dlt.attach(pipeline_name, **kwargs)
        return pipeline


def destinations_configs(
    default_sql_configs: bool = False,
    default_vector_configs: bool = False,
    default_staging_configs: bool = False,
    all_staging_configs: bool = False,
    local_filesystem_configs: bool = False,
    all_buckets_filesystem_configs: bool = False,
    table_format_filesystem_configs: bool = False,
    subset: Sequence[str] = (),
    bucket_subset: Sequence[str] = (),
    exclude: Sequence[str] = (),
    bucket_exclude: Sequence[str] = (),
    file_format: Union[TLoaderFileFormat, Sequence[TLoaderFileFormat]] = None,
    table_format: Union[TTableFormat, Sequence[TTableFormat]] = None,
    supports_merge: Optional[bool] = None,
    supports_dbt: Optional[bool] = None,
    force_iceberg: Optional[bool] = None,
) -> List[DestinationTestConfiguration]:
    # sanity check
    for item in subset:
        assert item in IMPLEMENTED_DESTINATIONS, f"Destination {item} is not implemented"

    # import filesystem destination to use named version for minio
    from dlt.destinations import filesystem

    # build destination configs
    destination_configs: List[DestinationTestConfiguration] = []

    # default non staging sql based configs, one per destination
    if default_sql_configs:
        destination_configs += [
            DestinationTestConfiguration(destination=destination)
            for destination in SQL_DESTINATIONS
            if destination not in ("athena", "synapse", "databricks", "dremio", "clickhouse")
        ]
        destination_configs += [
            DestinationTestConfiguration(destination="duckdb", file_format="parquet")
        ]
        # Athena needs filesystem staging, which will be automatically set; we have to supply a bucket url though.
        destination_configs += [
            DestinationTestConfiguration(
                destination="athena",
                file_format="parquet",
                supports_merge=False,
                bucket_url=AWS_BUCKET,
            )
        ]
        destination_configs += [
            DestinationTestConfiguration(
                destination="athena",
                file_format="parquet",
                bucket_url=AWS_BUCKET,
                force_iceberg=True,
                supports_merge=True,
                supports_dbt=False,
                extra_info="iceberg",
            )
        ]
        destination_configs += [
            DestinationTestConfiguration(
                destination="clickhouse", file_format="jsonl", supports_dbt=False
            )
        ]
        destination_configs += [
            DestinationTestConfiguration(
                destination="databricks",
                file_format="parquet",
                bucket_url=AZ_BUCKET,
                extra_info="az-authorization",
            )
        ]

        destination_configs += [
            DestinationTestConfiguration(
                destination="dremio",
                staging=filesystem(destination_name="minio"),
                file_format="parquet",
                bucket_url=AWS_BUCKET,
                supports_dbt=False,
            )
        ]
        destination_configs += [
            # DestinationTestConfiguration(destination="mssql", supports_dbt=False),
            DestinationTestConfiguration(destination="synapse", supports_dbt=False),
        ]

        # sanity check that when selecting default destinations, one of each sql destination is actually
        # provided
        assert set(SQL_DESTINATIONS) == {d.destination for d in destination_configs}

    if default_vector_configs:
        destination_configs += [
            DestinationTestConfiguration(destination="weaviate"),
            DestinationTestConfiguration(destination="lancedb"),
            DestinationTestConfiguration(
                destination="qdrant",
                credentials=dict(path=str(Path(FILE_BUCKET) / "qdrant_data")),
                extra_info="local-file",
            ),
            DestinationTestConfiguration(destination="qdrant", extra_info="server"),
        ]

    if default_staging_configs or all_staging_configs:
        destination_configs += [
            DestinationTestConfiguration(
                destination="redshift",
                staging="filesystem",
                file_format="parquet",
                bucket_url=AWS_BUCKET,
                staging_iam_role="arn:aws:iam::267388281016:role/redshift_s3_read",
                extra_info="s3-role",
            ),
            DestinationTestConfiguration(
                destination="bigquery",
                staging="filesystem",
                file_format="parquet",
                bucket_url=GCS_BUCKET,
                extra_info="gcs-authorization",
            ),
            DestinationTestConfiguration(
                destination="snowflake",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=GCS_BUCKET,
                stage_name="PUBLIC.dlt_gcs_stage",
                extra_info="gcs-integration",
            ),
            DestinationTestConfiguration(
                destination="snowflake",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=AWS_BUCKET,
                extra_info="s3-integration",
            ),
            DestinationTestConfiguration(
                destination="snowflake",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=AWS_BUCKET,
                stage_name="PUBLIC.dlt_s3_stage",
                extra_info="s3-integration",
            ),
            DestinationTestConfiguration(
                destination="snowflake",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=AZ_BUCKET,
                stage_name="PUBLIC.dlt_az_stage",
                extra_info="az-integration",
            ),
            DestinationTestConfiguration(
                destination="snowflake",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=AZ_BUCKET,
                extra_info="az-authorization",
            ),
            DestinationTestConfiguration(
                destination="databricks",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=AWS_BUCKET,
                extra_info="s3-authorization",
                disable_compression=True,
            ),
            DestinationTestConfiguration(
                destination="databricks",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=AZ_BUCKET,
                extra_info="az-authorization",
                disable_compression=True,
            ),
            DestinationTestConfiguration(
                destination="databricks",
                staging="filesystem",
                file_format="parquet",
                bucket_url=AWS_BUCKET,
                extra_info="s3-authorization",
            ),
            DestinationTestConfiguration(
                destination="synapse",
                staging="filesystem",
                file_format="parquet",
                bucket_url=AZ_BUCKET,
                extra_info="az-authorization",
                disable_compression=True,
            ),
            DestinationTestConfiguration(
                destination="clickhouse",
                staging="filesystem",
                file_format="parquet",
                bucket_url=AWS_BUCKET,
                extra_info="s3-authorization",
            ),
            DestinationTestConfiguration(
                destination="clickhouse",
                staging="filesystem",
                file_format="parquet",
                bucket_url=AZ_BUCKET,
                extra_info="az-authorization",
            ),
            DestinationTestConfiguration(
                destination="clickhouse",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=AZ_BUCKET,
                extra_info="az-authorization",
            ),
            DestinationTestConfiguration(
                destination="clickhouse",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=AWS_BUCKET,
                extra_info="s3-authorization",
            ),
            DestinationTestConfiguration(
                destination="dremio",
                staging=filesystem(destination_name="minio"),
                file_format="parquet",
                bucket_url=AWS_BUCKET,
                supports_dbt=False,
            ),
        ]

    if all_staging_configs:
        destination_configs += [
            DestinationTestConfiguration(
                destination="redshift",
                staging="filesystem",
                file_format="parquet",
                bucket_url=AWS_BUCKET,
                extra_info="credential-forwarding",
            ),
            DestinationTestConfiguration(
                destination="snowflake",
                staging="filesystem",
                file_format="parquet",
                bucket_url=AWS_BUCKET,
                extra_info="credential-forwarding",
            ),
            DestinationTestConfiguration(
                destination="redshift",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=AWS_BUCKET,
                extra_info="credential-forwarding",
            ),
            DestinationTestConfiguration(
                destination="bigquery",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=GCS_BUCKET,
                extra_info="gcs-authorization",
            ),
            DestinationTestConfiguration(
                destination="synapse",
                staging="filesystem",
                file_format="parquet",
                bucket_url=AZ_BUCKET,
                staging_use_msi=True,
                extra_info="az-managed-identity",
            ),
        ]

    # add local filesystem destinations if requested
    if local_filesystem_configs:
        destination_configs += [
            DestinationTestConfiguration(
                destination="filesystem",
                bucket_url=FILE_BUCKET,
                file_format="insert_values",
                supports_merge=False,
            )
        ]
        destination_configs += [
            DestinationTestConfiguration(
                destination="filesystem",
                bucket_url=FILE_BUCKET,
                file_format="parquet",
                supports_merge=False,
            )
        ]
        destination_configs += [
            DestinationTestConfiguration(
                destination="filesystem",
                bucket_url=FILE_BUCKET,
                file_format="jsonl",
                supports_merge=False,
            )
        ]

    if all_buckets_filesystem_configs:
        for bucket in DEFAULT_BUCKETS:
            destination_configs += [
                DestinationTestConfiguration(
                    destination="filesystem",
                    bucket_url=bucket,
                    extra_info=bucket,
                    supports_merge=False,
                )
            ]

    if table_format_filesystem_configs:
        for bucket in DEFAULT_BUCKETS:
            destination_configs += [
                DestinationTestConfiguration(
                    destination="filesystem",
                    bucket_url=bucket,
                    extra_info=bucket,
                    table_format="delta",
                    supports_merge=True,
                    env_vars=(
                        {
                            "DESTINATION__FILESYSTEM__DELTALAKE_STORAGE_OPTIONS": (
                                '{"AWS_S3_ALLOW_UNSAFE_RENAME": "true"}'
                            )
                        }
                        if bucket == AWS_BUCKET
                        else None
                    ),
                )
            ]

    # filter out non active destinations
    destination_configs = [
        conf for conf in destination_configs if conf.destination in ACTIVE_DESTINATIONS
    ]

    # filter out destinations not in subset
    if subset:
        destination_configs = [conf for conf in destination_configs if conf.destination in subset]
    if bucket_subset:
        destination_configs = [
            conf
            for conf in destination_configs
            if conf.destination != "filesystem" or conf.bucket_url in bucket_subset
        ]
    if exclude:
        destination_configs = [
            conf for conf in destination_configs if conf.destination not in exclude
        ]
    if bucket_exclude:
        destination_configs = [
            conf
            for conf in destination_configs
            if conf.destination != "filesystem" or conf.bucket_url not in bucket_exclude
        ]
    if file_format:
        if not isinstance(file_format, Sequence):
            file_format = [file_format]
        destination_configs = [
            conf
            for conf in destination_configs
            if conf.file_format and conf.file_format in file_format
        ]
    if table_format:
        if not isinstance(table_format, Sequence):
            table_format = [table_format]
        destination_configs = [
            conf
            for conf in destination_configs
            if conf.table_format and conf.table_format in table_format
        ]
    if supports_merge is not None:
        destination_configs = [
            conf for conf in destination_configs if conf.supports_merge == supports_merge
        ]
    if supports_dbt is not None:
        destination_configs = [
            conf for conf in destination_configs if conf.supports_dbt == supports_dbt
        ]

    # filter out excluded configs
    destination_configs = [
        conf for conf in destination_configs if conf.name not in EXCLUDED_DESTINATION_CONFIGURATIONS
    ]

    if force_iceberg is not None:
        destination_configs = [
            conf for conf in destination_configs if conf.force_iceberg is force_iceberg
        ]

    # add marks
    destination_configs = [
        cast(
            DestinationTestConfiguration,
            pytest.param(
                conf,
                marks=pytest.mark.needspyarrow17 if conf.table_format == "delta" else [],
            ),
        )
        for conf in destination_configs
    ]

    return destination_configs


@pytest.fixture(autouse=True)
def drop_pipeline(request, preserve_environ) -> Iterator[None]:
    # NOTE: keep `preserve_environ` to make sure fixtures are executed in order``
    yield
    if "no_load" in request.keywords:
        return
    try:
        drop_active_pipeline_data()
    except CantExtractTablePrefix:
        # for some tests we test that this exception is raised,
        # so we suppress it here
        pass


def drop_active_pipeline_data() -> None:
    """Drops all the datasets for currently active pipeline, wipes the working folder and then deactivated it."""
    if Container()[PipelineContext].is_active():
        try:
            # take existing pipeline
            p = dlt.pipeline()

            def _drop_dataset(schema_name: str) -> None:
                with p.destination_client(schema_name) as client:
                    try:
                        client.drop_storage()
                        print("dropped")
                    except Exception as exc:
                        print(exc)
                    if isinstance(client, WithStagingDataset):
                        with client.with_staging_dataset():
                            try:
                                client.drop_storage()
                                print("staging dropped")
                            except Exception as exc:
                                print(exc)

            # drop_func = _drop_dataset_fs if _is_filesystem(p) else _drop_dataset_sql
            # take all schemas and if destination was set
            if p.destination:
                if p.config.use_single_dataset:
                    # drop just the dataset for default schema
                    if p.default_schema_name:
                        _drop_dataset(p.default_schema_name)
                else:
                    # for each schema, drop the dataset
                    for schema_name in p.schema_names:
                        _drop_dataset(schema_name)

            # p._wipe_working_folder()
        finally:
            # always deactivate context, working directory will be wiped when the next test starts
            Container()[PipelineContext].deactivate()


@pytest.fixture
def empty_schema() -> Schema:
    schema = Schema("event")
    table = new_table("event_test_table")
    schema.update_table(table)
    return schema


def get_normalized_dataset_name(client: JobClientBase) -> str:
    if isinstance(client.config, DestinationClientDwhConfiguration):
        return client.config.normalize_dataset_name(client.schema)
    else:
        raise TypeError(
            f"{type(client)} client has configuration {type(client.config)} that does not support"
            " dataset name"
        )


def load_table(name: str) -> Dict[str, TTableSchemaColumns]:
    with open(f"./tests/load/cases/{name}.json", "rb") as f:
        return json.load(f)


def expect_load_file(
    client: JobClientBase,
    file_storage: FileStorage,
    query: str,
    table_name: str,
    status="completed",
) -> LoadJob:
    file_name = ParsedLoadJobFileName(
        table_name,
        ParsedLoadJobFileName.new_file_id(),
        0,
        client.capabilities.preferred_loader_file_format,
    ).file_name()
    file_storage.save(file_name, query.encode("utf-8"))
    table = client.prepare_load_table(table_name)
    job = client.start_file_load(table, file_storage.make_full_path(file_name), uniq_id())
    while job.state() == "running":
        sleep(0.5)
    assert job.file_name() == file_name
    assert job.state() == status
    return job


def prepare_table(
    client: JobClientBase,
    case_name: str = "event_user",
    table_name: str = "event_user",
    make_uniq_table: bool = True,
) -> str:
    client.schema._bump_version()
    client.update_stored_schema()
    user_table = load_table(case_name)[table_name]
    if make_uniq_table:
        user_table_name = table_name + uniq_id()
    else:
        user_table_name = table_name
    client.schema.update_table(new_table(user_table_name, columns=list(user_table.values())))
    client.schema._bump_version()
    client.update_stored_schema()
    return user_table_name


def yield_client(
    destination_type: str,
    dataset_name: str = None,
    default_config_values: StrAny = None,
    schema_name: str = "event",
) -> Iterator[SqlJobClientBase]:
    os.environ.pop("DATASET_NAME", None)
    # import destination reference by name
    destination = Destination.from_reference(destination_type)
    # create initial config
    dest_config: DestinationClientDwhConfiguration = None
    dest_config = destination.spec()  # type: ignore
    dest_config.dataset_name = dataset_name

    if default_config_values is not None:
        # apply the values to credentials, if dict is provided it will be used as default
        # dest_config.credentials = default_config_values  # type: ignore[assignment]
        # also apply to config
        dest_config.update(default_config_values)
    # get event default schema
    storage_config = resolve_configuration(
        SchemaStorageConfiguration(),
        explicit_value={"schema_volume_path": "tests/common/cases/schemas/rasa"},
    )
    schema_storage = SchemaStorage(storage_config)
    schema = schema_storage.load_schema(schema_name)
    schema.update_normalizers()
    # NOTE: schema version is bumped because new default hints are added
    schema._bump_version()
    # create client and dataset
    client: SqlJobClientBase = None

    # athena requires staging config to be present, so stick this in there here
    if destination_type == "athena":
        staging_config = DestinationClientStagingConfiguration(
            bucket_url=AWS_BUCKET,
        )._bind_dataset_name(dataset_name=dest_config.dataset_name)
        staging_config.destination_type = "filesystem"
        staging_config.resolve()
        dest_config.staging_config = staging_config  # type: ignore[attr-defined]

    # lookup for credentials in the section that is destination name
    with Container().injectable_context(
        ConfigSectionContext(
            sections=(
                "destination",
                destination_type,
            )
        )
    ):
        with destination.client(schema, dest_config) as client:  # type: ignore[assignment]
            yield client


@contextlib.contextmanager
def cm_yield_client(
    destination_type: str,
    dataset_name: str,
    default_config_values: StrAny = None,
    schema_name: str = "event",
) -> Iterator[SqlJobClientBase]:
    return yield_client(destination_type, dataset_name, default_config_values, schema_name)


def yield_client_with_storage(
    destination_type: str, default_config_values: StrAny = None, schema_name: str = "event"
) -> Iterator[SqlJobClientBase]:
    # create dataset with random name
    dataset_name = "test_" + uniq_id()

    with cm_yield_client(
        destination_type, dataset_name, default_config_values, schema_name
    ) as client:
        client.initialize_storage()
        yield client
        if client.is_storage_initialized():
            client.sql_client.drop_dataset()
        if isinstance(client, WithStagingDataset):
            with client.with_staging_dataset():
                if client.is_storage_initialized():
                    client.sql_client.drop_dataset()


def delete_dataset(client: SqlClientBase[Any], normalized_dataset_name: str) -> None:
    try:
        with client.with_alternative_dataset_name(normalized_dataset_name) as client:
            client.drop_dataset()
    except Exception as ex1:
        print(f"Error when deleting temp dataset {normalized_dataset_name}: {str(ex1)}")


@contextlib.contextmanager
def cm_yield_client_with_storage(
    destination_type: str, default_config_values: StrAny = None, schema_name: str = "event"
) -> Iterator[SqlJobClientBase]:
    return yield_client_with_storage(destination_type, default_config_values, schema_name)


def write_dataset(
    client: JobClientBase,
    f: IO[bytes],
    rows: Union[List[Dict[str, Any]], List[StrAny]],
    columns_schema: TTableSchemaColumns,
) -> None:
    spec = DataWriter.writer_spec_from_file_format(
        client.capabilities.preferred_loader_file_format, "object"
    )
    # adapt bytes stream to text file format
    if not spec.is_binary_format and isinstance(f.read(0), bytes):
        f = codecs.getwriter("utf-8")(f)  # type: ignore[assignment]
    writer = DataWriter.from_file_format(
        client.capabilities.preferred_loader_file_format, "object", f, client.capabilities
    )
    # remove None values
    for idx, row in enumerate(rows):
        rows[idx] = {k: v for k, v in row.items() if v is not None}
    writer.write_all(columns_schema, rows)
    writer.close()


def prepare_load_package(
    load_storage: LoadStorage, cases: Sequence[str], write_disposition: str = "append"
) -> Tuple[str, Schema]:
    load_id = uniq_id()
    load_storage.new_packages.create_package(load_id)
    for case in cases:
        path = f"./tests/load/cases/loading/{case}"
        shutil.copy(
            path,
            load_storage.new_packages.storage.make_full_path(
                load_storage.new_packages.get_job_state_folder_path(load_id, "new_jobs")
            ),
        )
    schema_path = Path("./tests/load/cases/loading/schema.json")
    # load without migration
    data = json.loads(schema_path.read_text(encoding="utf8"))
    for name, table in data["tables"].items():
        if name.startswith("_dlt"):
            continue
        table["write_disposition"] = write_disposition
    full_package_path = load_storage.new_packages.storage.make_full_path(
        load_storage.new_packages.get_package_path(load_id)
    )
    Path(full_package_path).joinpath(schema_path.name).write_text(json.dumps(data), encoding="utf8")

    schema_update_path = "./tests/load/cases/loading/schema_updates.json"
    shutil.copy(schema_update_path, full_package_path)

    load_storage.commit_new_load_package(load_id)
    schema = load_storage.normalized_packages.load_schema(load_id)
    return load_id, schema


def sequence_generator() -> Generator[List[Dict[str, str]], None, None]:
    count = 1
    while True:
        yield [{"content": str(count + i)} for i in range(3)]
        count += 3


def normalize_storage_table_cols(
    table_name: str, cols: TTableSchemaColumns, schema: Schema
) -> TTableSchemaColumns:
    """Normalize storage table columns back into schema naming"""
    # go back to schema naming convention. this is a hack - will work here to
    # reverse snowflake UPPER case folding
    storage_table = normalize_table_identifiers(
        new_table(table_name, columns=cols.values()), schema.naming  # type: ignore[arg-type]
    )
    return storage_table["columns"]
