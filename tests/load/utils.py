import pytest
import contextlib
import codecs
import os
from typing import (
    Any,
    AnyStr,
    Iterator,
    List,
    Sequence,
    IO,
    Tuple,
    Optional,
    Dict,
    Union,
    Generator,
    cast,
)
import shutil
from pathlib import Path
from urllib.parse import urlparse
from dataclasses import dataclass

import dlt
from dlt.common import json, sleep
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.base_configuration import BaseConfiguration
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.configuration.specs import (
    CredentialsConfiguration,
)
from dlt.common.data_types.typing import TDataType
from dlt.common.destination.client import (
    DestinationClientDwhConfiguration,
    HasFollowupJobs,
    JobClientBase,
    RunnableLoadJob,
    LoadJob,
    DestinationClientStagingConfiguration,
    WithStagingDataset,
    DestinationCapabilitiesContext,
    DEFAULT_FILE_LAYOUT,
)
from dlt.common.destination import TLoaderFileFormat, Destination, TDestinationReferenceArg
from dlt.common.destination.exceptions import DestinationUndefinedEntity, SqlClientNotAvailable
from dlt.common.data_writers import DataWriter
from dlt.common.exceptions import MissingDependencyException
from dlt.common.pipeline import PipelineContext
from dlt.common.schema import TTableSchemaColumns, Schema
from dlt.common.schema.typing import TTableFormat, TTableSchema
from dlt.common.storages import SchemaStorage, FileStorage, SchemaStorageConfiguration
from dlt.common.schema.utils import new_table, normalize_table_identifiers
from dlt.common.storages import ParsedLoadJobFileName, LoadStorage, PackageStorage
from dlt.common.storages.load_package import LoadJobInfo, create_load_id
from dlt.common.typing import StrAny
from dlt.common.utils import uniq_id

from dlt.destinations.exceptions import CantExtractTablePrefix
from dlt.destinations.impl.filesystem.configuration import FilesystemDestinationClientConfiguration
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.job_client_impl import SqlJobClientBase

from tests.utils import (
    ACTIVE_DESTINATIONS,
    ACTIVE_TABLE_FORMATS,
    IMPLEMENTED_DESTINATIONS,
    SQL_DESTINATIONS,
    EXCLUDED_DESTINATION_CONFIGURATIONS,
)
from tests.cases import (
    TABLE_UPDATE_COLUMNS_SCHEMA,
    TABLE_UPDATE,
    TABLE_ROW_ALL_DATA_TYPES,
    assert_all_data_types_row,
    table_update_and_row,
)

# Bucket urls.
AWS_BUCKET = dlt.config.get("tests.bucket_url_s3", str)
GCS_BUCKET = dlt.config.get("tests.bucket_url_gs", str)
AZ_BUCKET = dlt.config.get("tests.bucket_url_az", str)
ABFS_BUCKET = dlt.config.get("tests.bucket_url_abfss", str)
GDRIVE_BUCKET = dlt.config.get("tests.bucket_url_gdrive", str)
FILE_BUCKET = dlt.config.get("tests.bucket_url_file", str)
R2_BUCKET = dlt.config.get("tests.bucket_url_r2", str)
SFTP_BUCKET = dlt.config.get("tests.bucket_url_sftp", str)
HTTP_BUCKET = dlt.config.get("tests.bucket_url_http", str)
MEMORY_BUCKET = dlt.config.get("tests.memory", str)

ALL_FILESYSTEM_DRIVERS = dlt.config.get("ALL_FILESYSTEM_DRIVERS", list) or [
    "s3",
    "gs",
    "az",
    "abfss",
    "gdrive",
    "file",
    "memory",
    "https",
    "r2",
    "sftp",
]

# Filter out buckets not in all filesystem drivers
WITH_GDRIVE_BUCKETS = [
    GCS_BUCKET,
    AWS_BUCKET,
    FILE_BUCKET,
    MEMORY_BUCKET,
    ABFS_BUCKET,
    AZ_BUCKET,
    GDRIVE_BUCKET,
    SFTP_BUCKET,
]
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
        region_name=dlt.config.get("tests.r2_region_name", str),
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

    destination_type: str
    staging: Optional[TDestinationReferenceArg] = None
    file_format: Optional[TLoaderFileFormat] = None
    table_format: Optional[TTableFormat] = None
    bucket_url: Optional[str] = None
    stage_name: Optional[str] = None
    staging_iam_role: Optional[str] = None
    staging_use_msi: bool = False
    extra_info: Optional[str] = None
    supports_merge: bool = True  # TODO: take it from client base class
    force_iceberg: bool = None  # used only to test deprecation
    supports_dbt: bool = True
    disable_compression: bool = None  # use default value
    dev_mode: bool = False
    credentials: Optional[Union[CredentialsConfiguration, Dict[str, Any], str]] = None
    env_vars: Optional[Dict[str, str]] = None
    destination_name: Optional[str] = None

    def destination_factory(self, **kwargs) -> Destination[Any, Any]:
        dest_type = kwargs.pop("destination", self.destination_type)
        dest_name = kwargs.pop("destination_name", self.destination_name)
        self.setup()
        return Destination.from_reference(
            dest_type, self.credentials, destination_name=dest_name, **kwargs
        )

    def raw_capabilities(self) -> DestinationCapabilitiesContext:
        dest = Destination.from_reference(self.destination_type)
        return dest._raw_capabilities()

    @property
    def name(self) -> str:
        name: str = self.destination_name or self.destination_type
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
        env_key_prefix = (  # "DESTINATION__" + (self.destination_name or self.destination_type).upper()
            "DESTINATION"
        )
        for k, v in self.factory_kwargs.items():
            os.environ[f"{env_key_prefix}__{k.upper()}"] = str(v)

        # For the filesystem destinations we disable compression to make analyzing the result easier
        os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = str(
            self.destination_type == "filesystem"
            if self.disable_compression is None
            else self.disable_compression
        )

        if self.credentials is not None:
            if isinstance(self.credentials, str):
                os.environ[f"{env_key_prefix}__CREDENTIALS"] = self.credentials
            else:
                for key, value in dict(self.credentials).items():
                    os.environ[f"{env_key_prefix}__CREDENTIALS__{key.upper()}"] = str(value)

        if self.env_vars is not None:
            for k, v in self.env_vars.items():
                os.environ[k] = v

    def setup_pipeline(
        self, pipeline_name: str, dataset_name: str = None, dev_mode: bool = False, **kwargs
    ) -> dlt.Pipeline:
        """Convenience method to setup pipeline with this configuration"""

        self.dev_mode = dev_mode
        destination = kwargs.pop("destination", None)
        if destination is None:
            destination = self.destination_factory(**kwargs)
        else:
            self.setup()
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            staging=kwargs.pop("staging", self.staging),
            dataset_name=dataset_name if dataset_name is not None else pipeline_name + "_data",
            dev_mode=dev_mode,
            **kwargs,
        )
        return pipeline

    def attach_pipeline(self, pipeline_name: str, **kwargs) -> dlt.Pipeline:
        """Attach to existing pipeline keeping the dev_mode"""
        # remember dev_mode from setup_pipeline
        pipeline = dlt.attach(pipeline_name, **kwargs)
        return pipeline

    def supports_sql_client(self, pipeline: dlt.Pipeline) -> bool:
        """Checks if destination supports SQL queries"""
        try:
            pipeline.sql_client()
            return True
        except SqlClientNotAvailable:
            return False

    @property
    def run_kwargs(self):
        """Returns a dict of kwargs to be passed to pipeline.run method: currently file and table format"""
        return dict(loader_file_format=self.file_format, table_format=self.table_format)


def destinations_configs(
    default_sql_configs: bool = False,
    default_vector_configs: bool = False,
    default_staging_configs: bool = False,
    all_staging_configs: bool = False,
    local_filesystem_configs: bool = False,
    all_buckets_filesystem_configs: bool = False,
    table_format_filesystem_configs: bool = False,
    table_format_local_configs: bool = False,
    subset: Sequence[str] = (),
    bucket_subset: Sequence[str] = (),
    exclude: Sequence[str] = (),
    bucket_exclude: Sequence[str] = (),
    with_file_format: Union[TLoaderFileFormat, Sequence[TLoaderFileFormat]] = None,
    with_table_format: Union[TTableFormat, Sequence[TTableFormat]] = None,
    supports_merge: Optional[bool] = None,
    supports_dbt: Optional[bool] = None,
) -> List[DestinationTestConfiguration]:
    input_args = locals()

    # import filesystem destination to use named version for minio
    from dlt.destinations import filesystem

    # build destination configs
    destination_configs: List[DestinationTestConfiguration] = []

    # default sql configs that are also default staging configs
    default_sql_configs_with_staging = [
        # Athena needs filesystem staging, which will be automatically set; we have to supply a bucket url though.
        DestinationTestConfiguration(
            destination_type="athena",
            file_format="parquet",
            supports_merge=False,
            bucket_url=AWS_BUCKET,
        ),
        DestinationTestConfiguration(
            destination_type="athena",
            file_format="parquet",
            bucket_url=AWS_BUCKET,
            supports_merge=True,
            supports_dbt=False,
            table_format="iceberg",
            extra_info="iceberg",
        ),
    ]

    # default non staging sql based configs, one per destination
    if default_sql_configs:
        destination_configs += [
            DestinationTestConfiguration(destination_type=destination)
            for destination in SQL_DESTINATIONS
            if destination
            not in ("athena", "synapse", "dremio", "clickhouse", "sqlalchemy", "ducklake")
        ]
        destination_configs += [
            DestinationTestConfiguration(destination_type="duckdb", file_format="parquet"),
            DestinationTestConfiguration(destination_type="ducklake", supports_dbt=False),
            DestinationTestConfiguration(
                destination_type="motherduck", file_format="insert_values"
            ),
        ]

        # add Athena staging configs
        destination_configs += default_sql_configs_with_staging

        destination_configs += [
            DestinationTestConfiguration(
                destination_type="sqlalchemy",
                supports_merge=True,
                supports_dbt=False,
                destination_name="sqlalchemy_mysql",
                credentials=(  # Use root cause we need to create databases,
                    "mysql+pymysql://root:root@127.0.0.1:3306/dlt_data"
                ),
            ),
            DestinationTestConfiguration(
                destination_type="sqlalchemy",
                supports_merge=True,
                supports_dbt=False,
                destination_name="sqlalchemy_sqlite",
                credentials="sqlite:///_storage/dl_data.sqlite",
            ),
            # TODO: enable in sql alchemy destination test, 99% of tests work
            # DestinationTestConfiguration(
            #     destination_type="sqlalchemy",
            #     supports_merge=True,
            #     supports_dbt=False,
            #     destination_name="sqlalchemy_mssql",
            #     credentials=(  # Use root cause we need to create databases,
            #         "mssql+pyodbc://sa:Strong%21Passw0rd@localhost:1433/master"
            #         "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
            #     ),
            # ),
            # DestinationTestConfiguration(
            #     destination_type="sqlalchemy",
            #     supports_merge=True,
            #     supports_dbt=False,
            #     destination_name="sqlalchemy_trino",
            #     credentials=(  # Use root cause we need to create databases,
            #         "trino://admin:@127.0.0.1:8080/postgres"
            #     ),
            # ),
        ]

        destination_configs += [
            DestinationTestConfiguration(
                destination_type="clickhouse", file_format="jsonl", supports_dbt=False
            )
        ]

        destination_configs += [
            DestinationTestConfiguration(
                destination_type="dremio",
                staging=filesystem(destination_name="minio"),
                file_format="parquet",
                bucket_url=AWS_BUCKET,
                supports_dbt=False,
                extra_info="minio",
            )
        ]
        destination_configs += [
            # DestinationTestConfiguration(destination_type="mssql", supports_dbt=False),
            DestinationTestConfiguration(destination_type="synapse", supports_dbt=False),
        ]

        # sanity check that when selecting default destinations, one of each sql destination is actually
        # provided
        assert set(SQL_DESTINATIONS) == {d.destination_type for d in destination_configs}

    if default_vector_configs:
        destination_configs += [
            DestinationTestConfiguration(destination_type="weaviate"),
            DestinationTestConfiguration(destination_type="lancedb"),
            DestinationTestConfiguration(
                destination_type="qdrant",
                credentials=dict(path=str(Path(FILE_BUCKET) / "qdrant_data")),
                extra_info="local-file",
            ),
            DestinationTestConfiguration(
                destination_type="qdrant",
                credentials=dict(location="http://localhost:6333"),
                extra_info="server",
            ),
        ]

    if (default_sql_configs or all_staging_configs) and not default_sql_configs:
        # athena default configs not added yet
        destination_configs += default_sql_configs_with_staging

    if default_staging_configs or all_staging_configs:
        destination_configs += [
            DestinationTestConfiguration(
                destination_type="redshift",
                staging="filesystem",
                file_format="parquet",
                bucket_url=AWS_BUCKET,
                staging_iam_role="arn:aws:iam::267388281016:role/redshift_s3_read",
                extra_info="s3-role",
            ),
            DestinationTestConfiguration(
                destination_type="bigquery",
                staging="filesystem",
                file_format="parquet",
                bucket_url=GCS_BUCKET,
                extra_info="gcs-authorization",
            ),
            DestinationTestConfiguration(
                destination_type="snowflake",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=GCS_BUCKET,
                stage_name="PUBLIC.dlt_gcs_stage",
                extra_info="gcs-integration",
            ),
            DestinationTestConfiguration(
                destination_type="snowflake",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=AWS_BUCKET,
                extra_info="s3-integration",
            ),
            DestinationTestConfiguration(
                destination_type="snowflake",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=AWS_BUCKET,
                stage_name="PUBLIC.dlt_s3_stage",
                extra_info="s3-integration-public-stage",
            ),
            DestinationTestConfiguration(
                destination_type="snowflake",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=AZ_BUCKET,
                stage_name="PUBLIC.dlt_az_stage",
                extra_info="az-integration",
            ),
            DestinationTestConfiguration(
                destination_type="snowflake",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=AZ_BUCKET,
                extra_info="az-authorization",
            ),
            DestinationTestConfiguration(
                destination_type="databricks",
                staging="filesystem",
                file_format="parquet",
                bucket_url=AZ_BUCKET,
                extra_info="az-authorization",
            ),
            DestinationTestConfiguration(
                destination_type="databricks",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=AWS_BUCKET,
                extra_info="s3-authorization",
            ),
            DestinationTestConfiguration(
                destination_type="databricks",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=AZ_BUCKET,
                extra_info="az-authorization",
            ),
            DestinationTestConfiguration(
                destination_type="databricks",
                staging="filesystem",
                file_format="parquet",
                bucket_url=AWS_BUCKET,
                extra_info="s3-authorization",
            ),
            DestinationTestConfiguration(
                destination_type="synapse",
                staging="filesystem",
                file_format="parquet",
                bucket_url=AZ_BUCKET,
                extra_info="az-authorization",
                disable_compression=True,
            ),
            DestinationTestConfiguration(
                destination_type="clickhouse",
                staging="filesystem",
                file_format="parquet",
                bucket_url=AWS_BUCKET,
                extra_info="s3-authorization",
            ),
            DestinationTestConfiguration(
                destination_type="clickhouse",
                staging="filesystem",
                file_format="parquet",
                bucket_url=AZ_BUCKET,
                extra_info="az-authorization",
            ),
            DestinationTestConfiguration(
                destination_type="clickhouse",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=AZ_BUCKET,
                extra_info="az-authorization",
            ),
            DestinationTestConfiguration(
                destination_type="clickhouse",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=AWS_BUCKET,
                extra_info="s3-authorization",
            ),
        ]

    if all_staging_configs:
        destination_configs += [
            DestinationTestConfiguration(
                destination_type="redshift",
                staging="filesystem",
                file_format="parquet",
                bucket_url=AWS_BUCKET,
                extra_info="credential-forwarding",
            ),
            DestinationTestConfiguration(
                destination_type="redshift",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=AWS_BUCKET,
                staging_iam_role="arn:aws:iam::267388281016:role/redshift_s3_read",
                extra_info="s3-role",
            ),
            DestinationTestConfiguration(
                destination_type="snowflake",
                staging="filesystem",
                file_format="parquet",
                bucket_url=AWS_BUCKET,
                extra_info="credential-forwarding",
            ),
            DestinationTestConfiguration(
                destination_type="redshift",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=AWS_BUCKET,
                extra_info="credential-forwarding",
            ),
            DestinationTestConfiguration(
                destination_type="bigquery",
                staging="filesystem",
                file_format="jsonl",
                bucket_url=GCS_BUCKET,
                extra_info="gcs-authorization",
            ),
            DestinationTestConfiguration(
                destination_type="synapse",
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
                destination_type="filesystem",
                bucket_url=FILE_BUCKET,
                file_format="parquet",
                supports_merge=False,
            )
        ]
        destination_configs += [
            DestinationTestConfiguration(
                destination_type="filesystem",
                bucket_url=FILE_BUCKET,
                file_format="jsonl",
                supports_merge=False,
            )
        ]

    if all_buckets_filesystem_configs:
        for bucket in DEFAULT_BUCKETS:
            destination_configs += [
                DestinationTestConfiguration(
                    destination_type="filesystem",
                    bucket_url=bucket,
                    extra_info=bucket,
                    supports_merge=False,
                    file_format="jsonl",  # keep jsonl as default, test utils are setup for this
                )
            ]

    if table_format_filesystem_configs or table_format_local_configs:
        if table_format_filesystem_configs:
            table_buckets = set(DEFAULT_BUCKETS) - {SFTP_BUCKET, MEMORY_BUCKET}
        else:
            table_buckets = {FILE_BUCKET}

        # NOTE: delta does not work on memory buckets
        for bucket in table_buckets:
            destination_configs += [
                DestinationTestConfiguration(
                    destination_type="filesystem",
                    # so not set the bucket for GCS because we switch to s3 compat
                    # note google s3 compat does not implement DeleteObjects so we cannot drop dataset at the end
                    bucket_url=(
                        GCS_BUCKET.replace("gs://", "s3://") if bucket == GCS_BUCKET else bucket
                    ),
                    extra_info=bucket,
                    table_format="delta",
                    supports_merge=True,
                    file_format="parquet",
                    env_vars=(
                        {
                            "DESTINATION__FILESYSTEM__DELTALAKE_STORAGE_OPTIONS": (
                                '{"AWS_S3_ALLOW_UNSAFE_RENAME": "true"}'
                            )
                        }
                        if bucket == AWS_BUCKET
                        else None
                    ),
                    # in case of delta on gcs we use the s3 compat layer for reading
                    # for writing we still need to use the gc authentication, as delta_rs seems to use
                    # methods on the s3 interface that are not implemented by gcs
                    destination_name="filesystem_s3_gcs_comp" if bucket == GCS_BUCKET else None,
                )
            ]
            if bucket in [AZ_BUCKET]:
                # `pyiceberg` does not support `az` scheme
                continue
            destination_configs += [
                DestinationTestConfiguration(
                    destination_type="filesystem",
                    bucket_url=bucket,
                    extra_info=bucket,
                    table_format="iceberg",
                    supports_merge=True,
                    file_format="parquet",
                    destination_name="fsgcpoauth" if bucket == GCS_BUCKET else None,
                )
            ]

    try:
        # register additional destinations from _addons.py which must be placed in the same folder
        # as tests
        from _addons import destinations_configs  # type: ignore[import-not-found]

        destination_configs += destinations_configs(**input_args)
    except ImportError:
        pass

    # filter out non active destinations
    destination_configs = [
        conf for conf in destination_configs if conf.destination_type in ACTIVE_DESTINATIONS
    ]

    # filter out non active table formats
    destination_configs = [
        conf
        for conf in destination_configs
        if conf.table_format is None or conf.table_format in ACTIVE_TABLE_FORMATS
    ]

    # filter out destinations not in subset
    if subset:
        destination_configs = [
            conf for conf in destination_configs if conf.destination_type in subset
        ]
    if bucket_subset:
        destination_configs = [
            conf
            for conf in destination_configs
            # filter by bucket when (1) filesystem OR (2) specific set of destinations requested
            if (conf.destination_type != "filesystem" and not subset)
            or conf.bucket_url in bucket_subset
        ]
    if exclude:
        destination_configs = [
            conf
            for conf in destination_configs
            if not (conf.destination_type in exclude or conf.destination_name in exclude)
        ]
        destination_configs = [conf for conf in destination_configs if conf.name not in exclude]
    if bucket_exclude:
        destination_configs = [
            conf
            for conf in destination_configs
            if conf.destination_type != "filesystem" or conf.bucket_url not in bucket_exclude
        ]
    if with_file_format:
        if not isinstance(with_file_format, Sequence):
            with_file_format = [with_file_format]
        destination_configs = [
            conf
            for conf in destination_configs
            if conf.file_format and conf.file_format in with_file_format
        ]
    if with_table_format:
        if not isinstance(with_table_format, Sequence):
            with_table_format = [with_table_format]
        destination_configs = [
            conf
            for conf in destination_configs
            if conf.table_format and conf.table_format in with_table_format
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

    return destination_configs


@pytest.fixture(autouse=True)
def drop_pipeline(request, preserve_environ, auto_test_run_context) -> Iterator[None]:
    # NOTE: keep `preserve_environ` to make sure fixtures are executed in order``
    # enable activation history (for the main thread) to be able to drop pipelines active during test
    Container()[PipelineContext].enable_activation_history = True
    yield
    try:
        if "no_load" in request.keywords:
            # always deactivate
            Container()[PipelineContext].deactivate()
            Container()[PipelineContext].clear_activation_history()
        else:
            drop_active_pipeline_data()
    except CantExtractTablePrefix:
        # for some tests we test that this exception is raised,
        # so we suppress it here
        pass
    finally:
        # disable activation history
        Container()[PipelineContext].enable_activation_history = False


def drop_pipeline_data(p: dlt.Pipeline) -> None:
    """Drops all the datasets for a given pipeline"""

    def _drop_dataset(schema_name: str) -> None:
        try:
            with p.destination_client(schema_name) as client:
                try:
                    client.drop_storage()
                except DestinationUndefinedEntity:
                    pass
                except Exception as exc:
                    print("drop dataset", exc)
                if isinstance(client, WithStagingDataset):
                    with client.with_staging_dataset():
                        try:
                            client.drop_storage()
                        except DestinationUndefinedEntity:
                            pass
                        except Exception as exc:
                            print("drop staging dataset", exc)
        except Exception as exc:
            print("connect to dataset", exc)

    # take all schemas and if destination was set
    if p.destination:
        if p.config.use_single_dataset:
            # drop just the dataset for default schema
            _drop_dataset(p.default_schema_name)
        else:
            # for each schema, drop the dataset
            for schema_name in p.schema_names:
                _drop_dataset(schema_name)


def drop_active_pipeline_data() -> None:
    """Drops all the datasets for currently active pipeline, wipes the working folder and then deactivated it."""
    # NOTE: requires activation history to be enabled, see drop_pipeline
    activated_pipelines = Container()[PipelineContext].activation_history()
    if activated_pipelines:
        try:
            for pipeline in activated_pipelines:
                drop_pipeline_data(pipeline)  # type: ignore[arg-type]
        finally:
            # always deactivate context, working directory will be wiped when the next test starts
            Container()[PipelineContext].clear_activation_history()
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
    query: AnyStr,
    table_name: str,
    status="completed",
    file_format: TLoaderFileFormat = None,
) -> LoadJob:
    # recover spec used to write file
    spec = DataWriter.writer_spec_from_file_format(
        file_format or client.capabilities.preferred_loader_file_format, "object"
    )
    file_name = ParsedLoadJobFileName(
        table_name,
        ParsedLoadJobFileName.new_file_id(),
        0,
        spec.file_format,
    ).file_name()
    full_path = file_storage.make_full_path(file_name)
    if isinstance(query, str):
        query = query.encode("utf-8")  # type: ignore[assignment]
    file_storage.save(file_name, query)
    table = client.prepare_load_table(table_name)
    load_id = create_load_id()

    def _run_job(file_name_: str) -> LoadJob:
        job = client.create_load_job(table, file_storage.make_full_path(file_name_), load_id)

        if isinstance(job, RunnableLoadJob):
            job.set_run_vars(load_id=load_id, schema=client.schema, load_table=table)
            job.run_managed(client, None)
        # TODO: use semaphore
        while job.state() == "running":
            sleep(0.1)

        assert job.state() == status, f"Got {job.state()} with ({job.exception()})"

        return job

    if isinstance(client, WithStagingDataset) and client.should_load_data_to_staging_dataset(
        table_name
    ):
        # load to staging dataset on merge
        with client.with_staging_dataset():
            job = _run_job(file_name)
    else:
        job = _run_job(file_name)
    # execute table chain job
    if chain_jobs := client.create_table_chain_completed_followup_jobs(
        [table], [LoadJobInfo("completed_jobs", full_path, 0, 0, 0, job.job_file_info(), "")]  # type: ignore[arg-type]
    ):
        assert len(chain_jobs) == 1, "can't execute more than 1 chain job in this test"

        _run_job(chain_jobs[0].new_file_path())

    if isinstance(job, HasFollowupJobs):
        if chain_jobs := job.create_followup_jobs("completed"):
            assert len(chain_jobs) == 1, "can't execute more than 1 chain job in this test"
            _run_job(chain_jobs[0].new_file_path())

    return job


def prepare_table(
    client: JobClientBase,
    case_name: str = "event_user",
    table_name: str = "event_user",
    make_uniq_table: bool = True,
    skip_normalization: bool = False,
) -> str:
    client.schema._bump_version()
    client.update_stored_schema()
    user_table = load_table(case_name)[table_name]
    if make_uniq_table:
        user_table_name = table_name + uniq_id()
    else:
        user_table_name = table_name
    renamed_table = new_table(user_table_name, columns=list(user_table.values()))
    if skip_normalization:
        client.schema.tables[user_table_name] = renamed_table
    else:
        client.schema.update_table(renamed_table)
    print(client.schema.to_pretty_yaml())
    client.verify_schema([user_table_name])
    client.schema._bump_version()
    client.update_stored_schema()
    return user_table_name


def yield_client(
    destination_ref: TDestinationReferenceArg,
    dataset_name: str = None,
    default_config_values: StrAny = None,
    schema_name: str = "event",
) -> Iterator[SqlJobClientBase]:
    os.environ.pop("DATASET_NAME", None)
    # import destination reference by name
    destination = Destination.from_reference(destination_ref)
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
    if destination.destination_name == "athena":
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
                destination.destination_name,
            )
        )
    ):
        with destination.client(schema, dest_config) as client:  # type: ignore[assignment]
            try:
                from dlt.destinations.impl.duckdb.sql_client import WithTableScanners

                # open table scanners automatically, context manager above does not do that
                if issubclass(client.sql_client_class, WithTableScanners):
                    client.sql_client.open_connection()
            except (ImportError, MissingDependencyException):
                pass
            yield client


@contextlib.contextmanager
def cm_yield_client(
    destination: TDestinationReferenceArg,
    dataset_name: str,
    default_config_values: StrAny = None,
    schema_name: str = "event",
) -> Iterator[SqlJobClientBase]:
    return yield_client(destination, dataset_name, default_config_values, schema_name)


def yield_client_with_storage(
    destination: TDestinationReferenceArg,
    default_config_values: StrAny = None,
    schema_name: str = "event",
) -> Iterator[SqlJobClientBase]:
    # create dataset with random name
    dataset_name = "test_" + uniq_id()

    with cm_yield_client(destination, dataset_name, default_config_values, schema_name) as client:
        client.initialize_storage()
        yield client
        if client.is_storage_initialized():
            try:
                client.drop_storage()
            except Exception as exc:
                print(f"drop dataset {dataset_name}: {exc}")
        if isinstance(client, WithStagingDataset):
            with client.with_staging_dataset():
                if client.is_storage_initialized():
                    try:
                        client.drop_storage()
                    except Exception as exc:
                        print(f"drop dataset {dataset_name} STAGING: {exc}")


def delete_dataset(client: SqlClientBase[Any], normalized_dataset_name: str) -> None:
    try:
        with client.with_alternative_dataset_name(normalized_dataset_name) as client:
            client.drop_dataset()
    except Exception as ex1:
        print(f"Error when deleting temp dataset {normalized_dataset_name}: {str(ex1)}")


@contextlib.contextmanager
def cm_yield_client_with_storage(
    destination: TDestinationReferenceArg,
    default_config_values: StrAny = None,
    schema_name: str = "event",
) -> Iterator[SqlJobClientBase]:
    return yield_client_with_storage(destination, default_config_values, schema_name)


def write_dataset(
    client: JobClientBase,
    f: IO[bytes],
    rows: Union[List[Dict[str, Any]], List[StrAny]],
    table_schema: TTableSchema,
    file_format: TLoaderFileFormat = None,
) -> None:
    spec = DataWriter.writer_spec_from_file_format(
        file_format or client.capabilities.preferred_loader_file_format, "object"
    )
    # adapt bytes stream to text file format
    if not spec.is_binary_format and isinstance(f.read(0), bytes):
        f = codecs.getwriter("utf-8")(f)  # type: ignore[assignment]
    writer = DataWriter.from_file_format(spec.file_format, "object", f, client.capabilities)
    # remove None values
    for idx, row in enumerate(rows):
        rows[idx] = {k: v for k, v in row.items() if v is not None}
    table_schema["x-normalizer"] = {"seen-data": True}
    writer.write_all(table_schema["columns"], rows)
    writer.close()


def prepare_load_package(
    load_storage: LoadStorage,
    cases: Sequence[str],
    write_disposition: str = "append",
    jobs_per_case: int = 1,
) -> Tuple[str, Schema]:
    """
    Create a load package with explicitely provided files
    job_per_case multiplies the amount of load jobs, for big packages use small files
    """
    load_id = create_load_id()
    load_storage.new_packages.create_package(load_id)
    for case in cases:
        path = f"./tests/load/cases/loading/{case}"
        for _ in range(jobs_per_case):
            new_path = load_storage.new_packages.storage.make_full_path(
                load_storage.new_packages.get_job_state_folder_path(load_id, "new_jobs")
            )
            shutil.copy(
                path,
                new_path,
            )
            if jobs_per_case > 1:
                parsed_name = ParsedLoadJobFileName.parse(case)
                new_file_name = ParsedLoadJobFileName(
                    parsed_name.table_name,
                    ParsedLoadJobFileName.new_file_id(),
                    0,
                    parsed_name.file_format,
                ).file_name()
                shutil.move(new_path + "/" + case, new_path + "/" + new_file_name)

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


def count_job_types(p: dlt.Pipeline) -> Dict[str, Dict[str, Any]]:
    """
    gets a list of loaded jobs by type for each table
    this is useful for testing to check that the correct transformations were used
    """

    jobs = p.last_trace.last_load_info.load_packages[0].jobs["completed_jobs"]
    tables: Dict[str, Dict[str, Any]] = {}

    for j in jobs:
        file_format = j.job_file_info.file_format
        table_name = j.job_file_info.table_name
        if table_name.startswith("_dlt"):
            continue
        tables.setdefault(table_name, {})[file_format] = (
            tables.get(table_name, {}).get(file_format, 0) + 1
        )

    return tables


def set_always_refresh_views(config: BaseConfiguration) -> None:
    # set filesystem views to autorefresh
    if isinstance(config, FilesystemDestinationClientConfiguration):
        config["always_refresh_views"] = True


def table_update_and_row_for_destination(destination_config: DestinationTestConfiguration):
    # Redshift parquet -> exclude col7_precision
    # redshift and athena, parquet and jsonl, exclude time types
    exclude_types: List[TDataType] = []
    exclude_columns: List[str] = []

    if destination_config.destination_type in (
        "redshift",
        "athena",
        "databricks",
        "clickhouse",
    ) and destination_config.file_format in ("parquet", "jsonl"):
        # Redshift copy doesn't support TIME column
        exclude_types.append("time")

    if (
        destination_config.destination_type == "synapse"
        and destination_config.file_format == "parquet"
    ):
        # TIME columns are not supported for staged parquet loads into Synapse
        exclude_types.append("time")

    if destination_config.destination_type in (
        "redshift",
        "dremio",
    ) and destination_config.file_format in (
        "parquet",
        "jsonl",
    ):
        # Redshift can't load fixed width binary columns from parquet
        exclude_columns.append("col7_precision")

    if (
        destination_config.destination_type == "databricks"
        and destination_config.file_format == "jsonl"
    ):
        exclude_types.extend(["decimal", "binary", "wei", "json", "date"])
        exclude_columns.append("col1_precision")
        exclude_columns.append("col12")

    column_schemas, data_types = table_update_and_row(
        exclude_types=exclude_types, exclude_columns=exclude_columns
    )

    # bigquery and clickhouse cannot load into JSON fields from parquet
    if destination_config.file_format == "parquet":
        if destination_config.destination_type in ["bigquery"]:
            # change datatype to text and then allow for it in the assert (parse_json_strings)
            column_schemas["col9_null"]["data_type"] = column_schemas["col9"]["data_type"] = "text"
    # redshift cannot load from json into VARBYTE
    if destination_config.file_format == "jsonl":
        if destination_config.destination_type == "redshift":
            # change the datatype to text which will result in inserting base64 (allow_base64_binary)
            binary_cols = ["col7", "col7_null"]
            for col in binary_cols:
                column_schemas[col]["data_type"] = "text"

    return column_schemas, data_types
