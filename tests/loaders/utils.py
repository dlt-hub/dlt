from typing import List, cast

from dlt.common import json, Decimal
from dlt.common.file_storage import FileStorage
from dlt.common.schema import Column, Table
from dlt.common.time import sleep
from dlt.common.utils import uniq_id
from dlt.loaders.client_base import ClientBase


TABLE_UPDATE: List[Column] = [
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

def load_table(name: str) -> Table:
    with open(f"./tests/loaders/cases/{name}.json", "tr") as f:
        return cast(Table, json.load(f))


def expect_load_file(client: ClientBase, file_storage: FileStorage, query: str, table_name: str) -> None:
    file_name = uniq_id()
    file_storage.save(file_name, query.encode("utf-8"))
    # redshift insert jobs execute immediately
    job = client.start_file_load(table_name, file_storage._make_path(file_name))
    while job.status() == "running":
        sleep(0.5)
    assert job.status() ==  "completed"
    assert job.file_name() == file_name


def prepare_event_user_table(client: ClientBase) -> None:
    client.update_storage_schema()
    user_table = load_table("event_user")["event_user"]
    user_table_name = "event_user_" + uniq_id()
    client.schema.update_schema(user_table_name, user_table.values())
    client.update_storage_schema()
    return user_table_name
