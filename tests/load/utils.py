import base64
import contextlib
from importlib import import_module
import codecs
import os
from typing import Any, Iterator, List, Sequence, cast, IO, Tuple
import shutil
from pathlib import Path

import dlt
from dlt.common import json, Decimal, sleep, pendulum
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.destination.reference import DestinationClientDwhConfiguration, DestinationReference, JobClientBase, LoadJob
from dlt.common.data_writers import DataWriter
from dlt.common.schema import TColumnSchema, TTableSchemaColumns, Schema
from dlt.common.storages import SchemaStorage, FileStorage, SchemaStorageConfiguration
from dlt.common.schema.utils import new_table
from dlt.common.storages.load_storage import ParsedLoadJobFileName, LoadStorage
from dlt.common.typing import StrAny
from dlt.common.utils import uniq_id

from dlt.load import Load
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.job_client_impl import SqlJobClientBase

from tests.utils import ALL_DESTINATIONS

# bucket urls
AWS_BUCKET = dlt.config.get("tests.bucket_url_s3", str)
GCS_BUCKET = dlt.config.get("tests.bucket_url_gs", str)
FILE_BUCKET = dlt.config.get("tests.bucket_url_file", str)
MEMORY_BUCKET = dlt.config.get("tests.memory", str)

ALL_FILESYSTEM_DRIVERS = dlt.config.get("ALL_FILESYSTEM_DRIVERS", list) or ["s3", "gs", "file", "memory"]

# Filter out buckets not in all filesystem drivers
ALL_BUCKETS = [GCS_BUCKET, AWS_BUCKET, FILE_BUCKET, MEMORY_BUCKET]
ALL_BUCKETS = [bucket for bucket in ALL_BUCKETS if bucket.split(':')[0] in ALL_FILESYSTEM_DRIVERS]

ALL_CLIENTS = [f"{name}_client" for name in ALL_DESTINATIONS]


def ALL_CLIENTS_SUBSET(subset: Sequence[str]) -> List[str]:
    return list(set(subset).intersection(ALL_CLIENTS))


TABLE_UPDATE: List[TColumnSchema] = [
    {
        "name": "col1",
        "data_type": "bigint",
        "nullable": False
    },
    {
        "name": "col2",
        "data_type": "double",
        "nullable": False
    },
    {
        "name": "col3",
        "data_type": "bool",
        "nullable": False
    },
    {
        "name": "col4",
        "data_type": "timestamp",
        "nullable": False
    },
    {
        "name": "col5",
        "data_type": "text",
        "nullable": False
    },
    {
        "name": "col6",
        "data_type": "decimal",
        "nullable": False
    },
    {
        "name": "col7",
        "data_type": "binary",
        "nullable": False
    },
    {
        "name": "col8",
        "data_type": "wei",
        "nullable": False
    },
    {
        "name": "col9",
        "data_type": "complex",
        "nullable": False,
        "variant": True
    },
    {
        "name": "col10",
        "data_type": "date",
        "nullable": False
    },
    {
        "name": "col1_null",
        "data_type": "bigint",
        "nullable": True
    },
    {
        "name": "col2_null",
        "data_type": "double",
        "nullable": True
    },
    {
        "name": "col3_null",
        "data_type": "bool",
        "nullable": True
    },
    {
        "name": "col4_null",
        "data_type": "timestamp",
        "nullable": True
    },
    {
        "name": "col5_null",
        "data_type": "text",
        "nullable": True
    },
    {
        "name": "col6_null",
        "data_type": "decimal",
        "nullable": True
    },
    {
        "name": "col7_null",
        "data_type": "binary",
        "nullable": True
    },
    {
        "name": "col8_null",
        "data_type": "wei",
        "nullable": True
    },
    {
        "name": "col9_null",
        "data_type": "complex",
        "nullable": True,
        "variant": True
    },
    {
        "name": "col10_null",
        "data_type": "date",
        "nullable": True
    }
]
TABLE_UPDATE_COLUMNS_SCHEMA: TTableSchemaColumns = {t["name"]:t for t in TABLE_UPDATE}

TABLE_ROW_ALL_DATA_TYPES  = {
    "col1": 989127831,
    "col2": 898912.821982,
    "col3": True,
    "col4": "2022-05-23T13:26:45+00:00",
    "col5": "string data \n \r \x8e 🦆",
    "col6": Decimal("2323.34"),
    "col7": b'binary data \n \r \x8e',
    "col8": 2**56 + 92093890840,
    "col9": {"complex":[1,2,3,"a"], "link": "?commen\ntU\nrn=urn%3Ali%3Acomment%3A%28acti\012 \6 \\vity%3A69'08444473\n\n551163392%2C6n \r \x8e9085"},
    "col10": "2023-02-27",
    "col1_null": None,
    "col2_null": None,
    "col3_null": None,
    "col4_null": None,
    "col5_null": None,
    "col6_null": None,
    "col7_null": None,
    "col8_null": None,
    "col9_null": None,
    "col10_null": None
}


def assert_all_data_types_row(db_row: List[Any], parse_complex_strings: bool = False, allow_base64_binary: bool = False) -> None:
    print(db_row)
    # content must equal
    db_row[3] = str(pendulum.instance(db_row[3]))  # serialize date
    if isinstance(db_row[6], str):
        try:
            db_row[6] = bytes.fromhex(db_row[6])  # redshift returns binary as hex string
        except ValueError:
            if not allow_base64_binary:
                raise
            db_row[6] = base64.b64decode(db_row[6], validate=True)
    else:
        db_row[6] = bytes(db_row[6])
    # redshift and bigquery return strings from structured fields
    if isinstance(db_row[8], str):
        # then it must be json
        db_row[8] = json.loads(db_row[8])
    # parse again
    if parse_complex_strings and isinstance(db_row[8], str):
        # then it must be json
        db_row[8] = json.loads(db_row[8])

    db_row[9] = db_row[9].isoformat()

    expected_rows = list(TABLE_ROW_ALL_DATA_TYPES.values())
    print(expected_rows)
    assert db_row == expected_rows


def load_table(name: str) -> TTableSchemaColumns:
    with open(f"./tests/load/cases/{name}.json", "rb") as f:
        return cast(TTableSchemaColumns, json.load(f))


def expect_load_file(client: JobClientBase, file_storage: FileStorage, query: str, table_name: str, status = "completed") -> LoadJob:
    file_name = ParsedLoadJobFileName(table_name, uniq_id(), 0, client.capabilities.preferred_loader_file_format).job_id()
    file_storage.save(file_name, query.encode("utf-8"))
    table = Load.get_load_table(client.schema, file_name)
    job = client.start_file_load(table, file_storage.make_full_path(file_name), uniq_id())
    while job.state() == "running":
        sleep(0.5)
    assert job.file_name() == file_name
    assert job.state() ==  status
    return job


def prepare_table(client: JobClientBase, case_name: str = "event_user", table_name: str = "event_user", make_uniq_table: bool = True) -> None:
    client.schema.bump_version()
    client.update_storage_schema()
    user_table = load_table(case_name)[table_name]
    if make_uniq_table:
        user_table_name = table_name + uniq_id()
    else:
        user_table_name = table_name
    client.schema.update_schema(new_table(user_table_name, columns=user_table.values()))
    client.schema.bump_version()
    client.update_storage_schema()
    return user_table_name


def yield_client(
    destination_name: str,
    dataset_name: str = None,
    default_config_values: StrAny = None,
    schema_name: str = "event"
) -> Iterator[SqlJobClientBase]:
    os.environ.pop("DATASET_NAME", None)
    # import destination reference by name
    destination: DestinationReference = import_module(f"dlt.destinations.{destination_name}")
    # create initial config
    dest_config: DestinationClientDwhConfiguration = None
    dest_config = destination.spec()()
    dest_config.dataset_name = dataset_name

    if default_config_values is not None:
        # apply the values to credentials, if dict is provided it will be used as default
        dest_config.credentials = default_config_values
        # also apply to config
        dest_config.update(default_config_values)
    # get event default schema
    storage_config = resolve_configuration(SchemaStorageConfiguration(), explicit_value={
        "schema_volume_path": "tests/common/cases/schemas/rasa"
    })
    schema_storage = SchemaStorage(storage_config)
    schema = schema_storage.load_schema(schema_name)
    # create client and dataset
    client: SqlJobClientBase = None

    # lookup for credentials in the section that is destination name
    with Container().injectable_context(ConfigSectionContext(sections=(destination_name,))):
        with destination.client(schema, dest_config) as client:
            yield client


@contextlib.contextmanager
def cm_yield_client(
    destination_name: str,
    dataset_name: str,
    default_config_values: StrAny = None,
    schema_name: str = "event"
) -> Iterator[SqlJobClientBase]:
    return yield_client(destination_name, dataset_name, default_config_values, schema_name)


def yield_client_with_storage(
    destination_name: str,
    default_config_values: StrAny = None,
    schema_name: str = "event"
) -> Iterator[SqlJobClientBase]:

    # create dataset with random name
    dataset_name = "test_" + uniq_id()

    with cm_yield_client(destination_name, dataset_name, default_config_values, schema_name) as client:
        client.initialize_storage()
        yield client
        # print(dataset_name)
        client.sql_client.drop_dataset()
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
    destination_name: str,
    default_config_values: StrAny = None,
    schema_name: str = "event"
) -> Iterator[SqlJobClientBase]:
    return yield_client_with_storage(destination_name, default_config_values, schema_name)


def write_dataset(client: JobClientBase, f: IO[bytes], rows: List[StrAny], columns_schema: TTableSchemaColumns) -> None:
    data_format = DataWriter.data_format_from_file_format(client.capabilities.preferred_loader_file_format)
    # adapt bytes stream to text file format
    if not data_format.is_binary_format and isinstance(f.read(0), bytes):
        f = codecs.getwriter("utf-8")(f)
    writer = DataWriter.from_destination_capabilities(client.capabilities, f)
    # remove None values
    for idx, row in enumerate(rows):
        rows[idx] = {k:v for k, v in row.items() if v is not None}
    writer.write_all(columns_schema, rows)


def prepare_load_package(load_storage: LoadStorage, cases: Sequence[str], write_disposition: str='append') -> Tuple[str, Schema]:
    load_id = uniq_id()
    load_storage.create_temp_load_package(load_id)
    for case in cases:
        path = f"./tests/load/cases/loading/{case}"
        shutil.copy(path, load_storage.storage.make_full_path(f"{load_id}/{LoadStorage.NEW_JOBS_FOLDER}"))
    schema_path = Path("./tests/load/cases/loading/schema.json")
    data = json.loads(schema_path.read_text(encoding='utf8'))
    for name, table in data['tables'].items():
        if name.startswith('_dlt'):
            continue
        table['write_disposition'] = write_disposition
    Path(
        load_storage.storage.make_full_path(load_id)
    ).joinpath(schema_path.name).write_text(json.dumps(data), encoding='utf8')

    schema_update_path = "./tests/load/cases/loading/schema_updates.json"
    shutil.copy(schema_update_path, load_storage.storage.make_full_path(load_id))

    load_storage.commit_temp_load_package(load_id)
    schema = load_storage.load_package_schema(load_id)
    return load_id, schema
