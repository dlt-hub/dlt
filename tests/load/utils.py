import contextlib
from importlib import import_module
import os
from typing import Any, ContextManager, Iterator, List, Sequence, cast, IO

from dlt.common import json, Decimal
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import SchemaVolumeConfiguration
from dlt.common.destination import DestinationClientDwhConfiguration, DestinationReference, JobClientBase, LoadJob
from dlt.common.data_writers import DataWriter
from dlt.common.schema import TColumnSchema, TTableSchemaColumns
from dlt.common.storages import SchemaStorage, FileStorage
from dlt.common.schema.utils import new_table
from dlt.common.time import sleep
from dlt.common.typing import StrAny
from dlt.common.utils import uniq_id

from dlt.load import Load
from dlt.load.client_base_impl import SqlJobClientBase


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
        "nullable": True
    },
    {
        "name": "col6",
        "data_type": "decimal",
        "nullable": False
    },
    {
        "name": "col7",
        "data_type": "binary",
        "nullable": True
    },
    {
        "name": "col8",
        "data_type": "wei",
        "nullable": True
    },
    {
        "name": "col9",
        "data_type": "complex",
        "nullable": False
    },
]
TABLE_UPDATE_COLUMNS_SCHEMA: TTableSchemaColumns = {t["name"]:t for t in TABLE_UPDATE}

TABLE_ROW = {
    "col1": 989127831,
    "col2": 898912.821982,
    "col3": True,
    "col4": "2022-05-23T13:26:45+00:00",
    "col5": "string data",
    "col6": Decimal("2323.34"),
    "col7": b'binary data',
    "col8": 2**56 + 92093890840,
    "col9": "{complex: [1,2,3]}"
}

def load_table(name: str) -> TTableSchemaColumns:
    with open(f"./tests/load/cases/{name}.json", "tr", encoding="utf-8") as f:
        return cast(TTableSchemaColumns, json.load(f))


def expect_load_file(client: JobClientBase, file_storage: FileStorage, query: str, table_name: str, status = "completed") -> LoadJob:
    file_name = uniq_id()
    file_storage.save(file_name, query.encode("utf-8"))
    table = Load.get_load_table(client.schema, table_name, file_name)
    job = client.start_file_load(table, file_storage.make_full_path(file_name))
    while job.status() == "running":
        sleep(0.5)
    assert job.file_name() == file_name
    assert job.status() ==  status
    return job


def prepare_table(client: JobClientBase, case_name: str = "event_user", table_name: str = "event_user") -> None:
    client.update_storage_schema()
    user_table = load_table(case_name)[table_name]
    user_table_name = table_name + uniq_id()
    client.schema.update_schema(new_table(user_table_name, columns=user_table.values()))
    client.schema.bump_version()
    client.update_storage_schema()
    return user_table_name



def yield_client_with_storage(destination_name: str, initial_values: StrAny = None) -> Iterator[SqlJobClientBase]:
    os.environ.pop("DEFAULT_DATASET", None)
    # import destination reference by name
    destination: DestinationReference = import_module(f"dlt.load.{destination_name}")
    # create dataset with random name
    dataset_name = "test_" + uniq_id()
    # create initial config
    config: DestinationClientDwhConfiguration = None
    config = destination.spec()()
    # print(config.destination_name)
    # print(destination.spec())
    # print(destination.spec().destination_name)
    config.dataset_name = dataset_name

    if initial_values is not None:
        # apply the values to credentials, if dict is provided it will be used as initial
        config.credentials = initial_values
        # also apply to config
        config.update(initial_values)
    # get event default schema
    C = resolve_configuration(SchemaVolumeConfiguration(), initial_value={
        "schema_volume_path": "tests/common/cases/schemas/rasa"
    })
    schema_storage = SchemaStorage(C)
    schema = schema_storage.load_schema("event")
    # create client and dataset
    client: SqlJobClientBase = None

    with destination.client(schema, config) as client:
        client.initialize_storage()
        yield client
        client.sql_client.drop_dataset()


@contextlib.contextmanager
def cm_yield_client_with_storage(destination_name: str, initial_values: StrAny = None) -> ContextManager[SqlJobClientBase]:
    return yield_client_with_storage(destination_name, initial_values)


def write_dataset(client: JobClientBase, f: IO[Any], rows: Sequence[StrAny], columns_schema: TTableSchemaColumns) -> None:
    file_format = client.capabilities()["preferred_loader_file_format"]
    writer = DataWriter.from_file_format(file_format, f)
    writer.write_all(columns_schema, rows)
