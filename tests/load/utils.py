import contextlib
import codecs
import os
from typing import Any, Iterator, List, Sequence, IO, Tuple, Optional, Dict, Union
import shutil
from pathlib import Path
from dataclasses import dataclass

import dlt
from dlt.common import json, sleep
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.destination.reference import (
    DestinationClientDwhConfiguration,
    JobClientBase,
    LoadJob,
    DestinationClientStagingConfiguration,
    WithStagingDataset,
)
from dlt.common.destination import TLoaderFileFormat, Destination
from dlt.common.data_writers import DataWriter
from dlt.common.schema import TTableSchemaColumns, Schema
from dlt.common.storages import SchemaStorage, FileStorage, SchemaStorageConfiguration
from dlt.common.schema.utils import new_table
from dlt.common.storages import ParsedLoadJobFileName, LoadStorage, PackageStorage
from dlt.common.typing import StrAny
from dlt.common.utils import uniq_id

from dlt.load import Load
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

# bucket urls
AWS_BUCKET = dlt.config.get("tests.bucket_url_s3", str)
GCS_BUCKET = dlt.config.get("tests.bucket_url_gs", str)
AZ_BUCKET = dlt.config.get("tests.bucket_url_az", str)
FILE_BUCKET = dlt.config.get("tests.bucket_url_file", str)
R2_BUCKET = dlt.config.get("tests.bucket_url_r2", str)
MEMORY_BUCKET = dlt.config.get("tests.memory", str)

ALL_FILESYSTEM_DRIVERS = dlt.config.get("ALL_FILESYSTEM_DRIVERS", list) or [
    "s3",
    "gs",
    "az",
    "file",
    "memory",
    "r2",
]

# Filter out buckets not in all filesystem drivers
DEFAULT_BUCKETS = [GCS_BUCKET, AWS_BUCKET, FILE_BUCKET, MEMORY_BUCKET, AZ_BUCKET]
DEFAULT_BUCKETS = [
    bucket for bucket in DEFAULT_BUCKETS if bucket.split(":")[0] in ALL_FILESYSTEM_DRIVERS
]

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

EXTRA_BUCKETS: List[Dict[str, Any]] = []
if "r2" in ALL_FILESYSTEM_DRIVERS:
    EXTRA_BUCKETS.append(R2_BUCKET_CONFIG)

ALL_BUCKETS = DEFAULT_BUCKETS + EXTRA_BUCKETS


@dataclass
class DestinationTestConfiguration:
    """Class for defining test setup for one destination."""

    destination: str
    staging: Optional[str] = None
    file_format: Optional[TLoaderFileFormat] = None
    bucket_url: Optional[str] = None
    stage_name: Optional[str] = None
    staging_iam_role: Optional[str] = None
    extra_info: Optional[str] = None
    supports_merge: bool = True  # TODO: take it from client base class
    force_iceberg: bool = False
    supports_dbt: bool = True

    @property
    def name(self) -> str:
        name: str = self.destination
        if self.file_format:
            name += f"-{self.file_format}"
        if not self.staging:
            name += "-no-staging"
        else:
            name += "-staging"
        if self.extra_info:
            name += f"-{self.extra_info}"
        return name

    def setup(self) -> None:
        """Sets up environment variables for this destination configuration"""
        os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = self.bucket_url or ""
        os.environ["DESTINATION__STAGE_NAME"] = self.stage_name or ""
        os.environ["DESTINATION__STAGING_IAM_ROLE"] = self.staging_iam_role or ""
        os.environ["DESTINATION__FORCE_ICEBERG"] = str(self.force_iceberg) or ""

        """For the filesystem destinations we disable compression to make analyzing the result easier"""
        if self.destination == "filesystem":
            os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "True"

    def setup_pipeline(
        self, pipeline_name: str, dataset_name: str = None, full_refresh: bool = False, **kwargs
    ) -> dlt.Pipeline:
        """Convenience method to setup pipeline with this configuration"""
        self.setup()
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=self.destination,
            staging=self.staging,
            dataset_name=dataset_name or pipeline_name,
            full_refresh=full_refresh,
            **kwargs,
        )
        return pipeline


def destinations_configs(
    default_sql_configs: bool = False,
    default_vector_configs: bool = False,
    default_staging_configs: bool = False,
    all_staging_configs: bool = False,
    local_filesystem_configs: bool = False,
    all_buckets_filesystem_configs: bool = False,
    subset: Sequence[str] = (),
    exclude: Sequence[str] = (),
    file_format: Optional[TLoaderFileFormat] = None,
) -> List[DestinationTestConfiguration]:
    # sanity check
    for item in subset:
        assert item in IMPLEMENTED_DESTINATIONS, f"Destination {item} is not implemented"

    # build destination configs
    destination_configs: List[DestinationTestConfiguration] = []

    # default non staging sql based configs, one per destination
    if default_sql_configs:
        destination_configs += [
            DestinationTestConfiguration(destination=destination)
            for destination in SQL_DESTINATIONS
            if destination not in ("athena", "synapse")
        ]
        destination_configs += [
            DestinationTestConfiguration(destination="duckdb", file_format="parquet")
        ]
        # athena needs filesystem staging, which will be automatically set, we have to supply a bucket url though
        destination_configs += [
            DestinationTestConfiguration(
                destination="athena",
                staging="filesystem",
                file_format="parquet",
                supports_merge=False,
                bucket_url=AWS_BUCKET,
            )
        ]
        destination_configs += [
            DestinationTestConfiguration(
                destination="athena",
                staging="filesystem",
                file_format="parquet",
                bucket_url=AWS_BUCKET,
                force_iceberg=True,
                supports_merge=False,
                supports_dbt=False,
                extra_info="iceberg",
            )
        ]
        # dbt for Synapse has some complications and I couldn't get it to pass all tests.
        destination_configs += [
            DestinationTestConfiguration(destination="synapse", supports_dbt=False)
        ]

    if default_vector_configs:
        # for now only weaviate
        destination_configs += [DestinationTestConfiguration(destination="weaviate")]

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
        ]

    # add local filesystem destinations if requested
    if local_filesystem_configs:
        destination_configs += [
            DestinationTestConfiguration(
                destination="filesystem", bucket_url=FILE_BUCKET, file_format="insert_values"
            )
        ]
        destination_configs += [
            DestinationTestConfiguration(
                destination="filesystem", bucket_url=FILE_BUCKET, file_format="parquet"
            )
        ]
        destination_configs += [
            DestinationTestConfiguration(
                destination="filesystem", bucket_url=FILE_BUCKET, file_format="jsonl"
            )
        ]

    if all_buckets_filesystem_configs:
        for bucket in DEFAULT_BUCKETS:
            destination_configs += [
                DestinationTestConfiguration(
                    destination="filesystem", bucket_url=bucket, extra_info=bucket
                )
            ]

    # filter out non active destinations
    destination_configs = [
        conf for conf in destination_configs if conf.destination in ACTIVE_DESTINATIONS
    ]

    # filter out destinations not in subset
    if subset:
        destination_configs = [conf for conf in destination_configs if conf.destination in subset]
    if exclude:
        destination_configs = [
            conf for conf in destination_configs if conf.destination not in exclude
        ]
    if file_format:
        destination_configs = [
            conf for conf in destination_configs if conf.file_format == file_format
        ]

    # filter out excluded configs
    destination_configs = [
        conf for conf in destination_configs if conf.name not in EXCLUDED_DESTINATION_CONFIGURATIONS
    ]

    return destination_configs


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
    table = client.get_load_table(table_name)
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
    client.schema.bump_version()
    client.update_stored_schema()
    user_table = load_table(case_name)[table_name]
    if make_uniq_table:
        user_table_name = table_name + uniq_id()
    else:
        user_table_name = table_name
    client.schema.update_table(new_table(user_table_name, columns=list(user_table.values())))
    client.schema.bump_version()
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
    dest_config = destination.spec()  # type: ignore[assignment]
    dest_config.dataset_name = dataset_name  # type: ignore[misc]

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
    # create client and dataset
    client: SqlJobClientBase = None

    # athena requires staging config to be present, so stick this in there here
    if destination_type == "athena":
        staging_config = DestinationClientStagingConfiguration(
            destination_type="fake-stage",  # type: ignore
            dataset_name=dest_config.dataset_name,
            default_schema_name=dest_config.default_schema_name,
            bucket_url=AWS_BUCKET,
        )
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
    data_format = DataWriter.data_format_from_file_format(
        client.capabilities.preferred_loader_file_format
    )
    # adapt bytes stream to text file format
    if not data_format.is_binary_format and isinstance(f.read(0), bytes):
        f = codecs.getwriter("utf-8")(f)  # type: ignore[assignment]
    writer = DataWriter.from_destination_capabilities(client.capabilities, f)
    # remove None values
    for idx, row in enumerate(rows):
        rows[idx] = {k: v for k, v in row.items() if v is not None}
    writer.write_all(columns_schema, rows)


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
                load_storage.new_packages.get_job_folder_path(load_id, "new_jobs")
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
