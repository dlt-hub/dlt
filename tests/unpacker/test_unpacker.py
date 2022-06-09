from typing import Dict, List, Sequence
import os
import pytest
import shutil
from fnmatch import fnmatch
from prometheus_client import CollectorRegistry
from multiprocessing.dummy import Pool as ThreadPool

from dlt.common import json
from dlt.common.sources import with_table_name
from dlt.common.utils import uniq_id
from dlt.common.typing import StrAny, TEvent
from dlt.common.parser import TUnpackedRowIterator, extract
from dlt.common.file_storage import FileStorage
from dlt.common.schema import DataType, Schema
from dlt.common.storages.loader_storage import LoaderStorage
from dlt.common.storages.unpacker_storage import UnpackerStorage
from dlt.common.storages import SchemaStorage
from dlt.extractors.extractor_storage import ExtractorStorageBase

from dlt.unpacker import unpacker, __version__
from tests.cases import JSON_TYPED_DICT, JSON_TYPED_DICT_TYPES

from tests.unpacker.utils import json_case_path
from tests.utils import TEST_STORAGE, assert_no_dict_key_starts_with, write_version, clean_storage, init_logger


@pytest.fixture()
def raw_unpacker() -> FileStorage:
    # does not install default schemas, so no type hints and row filters
    return init_unpacker()


@pytest.fixture
def default_unpacker() -> FileStorage:
    # install default schemas, includes type hints and row filters
    return init_unpacker("tests/unpacker/cases/schemas", ["event", "ethereum"])


def init_unpacker(default_schemas_path: str = None, schema_names: List[str] = None) -> FileStorage:
    storage = clean_storage()
    unpacker.configure(unpacker.configuration({"NAME": "test"}), CollectorRegistry(), _mock_rasa_extract, default_schemas_path, schema_names)
    # set jsonl as default writer
    unpacker.load_storage.writer_type = unpacker.CONFIG.WRITER_TYPE = "jsonl"
    return storage


def _mock_rasa_extract(schema: Schema, source_event: TEvent, load_id: str, add_json: bool) -> TUnpackedRowIterator:
    if schema.schema_name == "event":
        # this emulates rasa parser on standard parser
        event = {"sender_id": source_event["sender_id"], "timestamp": source_event["timestamp"]}
        yield from extract(schema, event, load_id, add_json)
        # add table name which is "event" field in RASA OSS
        with_table_name(source_event, "event_" + source_event["event"])

    # will generate tables properly
    yield from extract(schema, source_event, load_id, add_json)


@pytest.fixture(scope="module", autouse=True)
def logger_autouse() -> None:
    init_logger()


def test_intialize(default_unpacker: FileStorage) -> None:
    # create storages in fixture
    pass


def test_unpack_single_user_event_jsonl(raw_unpacker: FileStorage) -> None:
    expected_tables, load_files = unpack_event_user("event_user_load_1")
    # load, parse and verify jsonl
    for expected_table in expected_tables:
        expect_lines_file(load_files[expected_table])
    # return first line from event_user file
    event_text, lines = expect_lines_file(load_files["event_user"], 0)
    assert lines == 1
    event_json = json.loads(event_text)
    assert event_json["event"] == "user"
    assert event_json["parse_data__intent__name"] == "greet"
    assert event_json["text"] == "hello"
    event_text, lines = expect_lines_file(load_files["event_user__parse_data__response_selector__default__ranking"], 9)
    assert lines == 10
    event_json = json.loads(event_text)
    assert "id" in event_json
    assert "confidence" in event_json
    assert "intent_response_key" in event_json


def test_unpack_single_user_event_insert(raw_unpacker: FileStorage) -> None:
    unpacker.load_storage.writer_type = unpacker.CONFIG.WRITER_TYPE = "insert_values"
    expected_tables, load_files = unpack_event_user("event_user_load_1")
    # verify values line
    for expected_table in expected_tables:
        expect_lines_file(load_files[expected_table])
    # return first values line from event_user file
    event_text, lines = expect_lines_file(load_files["event_user"], 2)
    assert lines == 3
    assert "'user'" in  event_text
    assert "'greet'" in event_text
    assert "'hello'" in event_text
    event_text, lines = expect_lines_file(load_files["event_user__parse_data__response_selector__default__ranking"], 11)
    assert lines == 12
    assert "(7005479104644416710," in event_text


def test_unpack_filter_user_event(default_unpacker: FileStorage) -> None:
    load_id = unpack_cases(["event_user_load_v228_1"])
    load_files = expect_load_package(load_id, ["event", "event_user", "event_user__metadata__user_nicknames",
                                               "event_user__parse_data__entities", "event_user__parse_data__entities__processors", "event_user__parse_data__intent_ranking"])
    event_text, lines = expect_lines_file(load_files["event_user"], 0)
    assert lines == 1
    filtered_row = json.loads(event_text)
    assert "parse_data__intent__name" in filtered_row
    # response selectors are removed
    assert_no_dict_key_starts_with(filtered_row, "parse_data__response_selector")


def test_unpack_filter_bot_event(default_unpacker: FileStorage) -> None:
    load_id = unpack_cases(["event_bot_load_metadata_1"])
    load_files = expect_load_package(load_id, ["event", "event_bot"])
    event_text, lines = expect_lines_file(load_files["event_bot"], 0)
    assert lines == 1
    filtered_row = json.loads(event_text)
    assert "metadata__utter_action" in filtered_row
    assert "metadata__account_balance" not in filtered_row


def test_preserve_slot_complex_value_json_l(default_unpacker: FileStorage) -> None:
    # unpacker.load_storage.writer_type = unpacker.CONFIG.WRITER_TYPE = "insert_values"
    load_id = unpack_cases(["event_slot_session_metadata_1"])
    load_files = expect_load_package(load_id, ["event", "event_slot"])
    event_text, lines = expect_lines_file(load_files["event_slot"], 0)
    assert lines == 1
    filtered_row = json.loads(event_text)
    assert type(filtered_row["value"]) is str
    assert filtered_row["value"] == json.dumps({
            "user_id": "world",
            "mitter_id": "hello"
        })


def test_preserve_slot_complex_value_insert(default_unpacker: FileStorage) -> None:
    unpacker.load_storage.writer_type = unpacker.CONFIG.WRITER_TYPE = "insert_values"
    load_id = unpack_cases(["event_slot_session_metadata_1"])
    load_files = expect_load_package(load_id, ["event", "event_slot"])
    event_text, lines = expect_lines_file(load_files["event_slot"], 2)
    assert lines == 3
    c_val = json.dumps({
            "user_id": "world",
            "mitter_id": "hello"
        })
    assert c_val in event_text


def test_unpack_raw_no_type_hints(raw_unpacker: FileStorage) -> None:
    unpack_event_user("event_user_load_1")
    assert_timestamp_data_type("double")


def test_unpack_raw_type_hints(default_unpacker: FileStorage) -> None:
    unpack_cases(["event_user_load_1"])
    assert_timestamp_data_type("timestamp")


def test_unpack_many_events_insert(raw_unpacker: FileStorage) -> None:
    unpacker.load_storage.writer_type = unpacker.CONFIG.WRITER_TYPE = "insert_values"
    load_id = unpack_cases(["event_many_load_2", "event_user_load_1"])
    expected_tables = EXPECTED_USER_TABLES + ["event_bot", "event_action"]
    load_files = expect_load_package(load_id, expected_tables)
    # return first values line from event_user file
    event_text, lines = expect_lines_file(load_files["event"], 4)
    assert lines == 5
    assert f"'{load_id}'" in event_text


def test_unpack_many_schemas(raw_unpacker: FileStorage) -> None:
    unpacker.load_storage.writer_type = unpacker.CONFIG.WRITER_TYPE = "insert_values"
    copy_cases(["event_many_load_2", "event_user_load_1", "ethereum_blocks_9c1d9b504ea240a482b007788d5cd61c_2"])
    unpacker.unpack(ThreadPool())
    # must have two loading groups with model and event schemas
    loads = unpacker.load_storage.list_loads()
    assert len(loads) == 2
    schema_storage = SchemaStorage(unpacker.load_storage.storage.storage_path)
    schemas = []
    # load all schemas
    for load_id in loads:
        schema = schema_storage.load_folder_schema(unpacker.load_storage.get_load_path(load_id))
        schemas.append(schema.schema_name)
        # expect event tables
        if schema.schema_name == "event":
            expected_tables = EXPECTED_USER_TABLES + ["event_bot", "event_action"]
            expect_load_package(load_id, expected_tables)
        if schema.schema_name == "ethereum":
            expect_load_package(load_id, EXPECTED_ETH_TABLES)
    assert set(schemas) == set(["ethereum", "event"])


def test_unpack_typed_json(raw_unpacker: FileStorage) -> None:
    unpacker.load_storage.writer_type = unpacker.CONFIG.WRITER_TYPE = "jsonl"
    extract_items([JSON_TYPED_DICT], "special")
    unpacker.unpack(ThreadPool())
    loads = unpacker.load_storage.list_loads()
    assert len(loads) == 1
    schema_storage = SchemaStorage(unpacker.load_storage.storage.storage_path)
    # load all schemas
    schema = schema_storage.load_folder_schema(unpacker.load_storage.get_load_path(loads[0]))
    assert schema.schema_name == "special"
    # named as schema - default fallback
    table = schema.get_table("special")
    # assert inferred types
    for k, v in JSON_TYPED_DICT_TYPES.items():
        assert table[k]["data_type"] == v


EXPECTED_ETH_TABLES = ["blocks", "blocks__transactions", "blocks__transactions__logs", "blocks__transactions__logs__topics",
                       "blocks__uncles", "blocks__transactions__access_list", "blocks__transactions__access_list__storage_keys"]

EXPECTED_USER_TABLES = ["event", "event_user", "event_user__parse_data__intent_ranking", "event_user__parse_data__response_selector__all_retrieval_intents",
         "event_user__parse_data__response_selector__default__ranking", "event_user__parse_data__response_selector__default__response__response_templates",
         "event_user__parse_data__response_selector__default__response__responses"]


def extract_items(items: Sequence[StrAny], schema_name: str) -> None:
    extractor = ExtractorStorageBase("1.0.0", True, FileStorage(os.path.join(TEST_STORAGE, "extractor"), makedirs=True), unpacker.unpack_storage)
    load_id = uniq_id()
    extractor.save_json(f"{load_id}.json", items)
    extractor.commit_events(
        schema_name,
        extractor.storage._make_path(f"{load_id}.json"),
        "items",
        len(items),
        load_id
    )

def unpack_event_user(case: str) -> None:
    load_id = unpack_cases([case])
    return EXPECTED_USER_TABLES, expect_load_package(load_id, EXPECTED_USER_TABLES)


def unpack_cases(cases: Sequence[str]) -> str:
    copy_cases(cases)
    load_id = uniq_id()
    unpacker.load_storage.create_temp_load_folder(load_id)
    # pool not required for map_single
    dest_cases = [f"{UnpackerStorage.UNPACKING_FOLDER}/{c}.unpack.json" for c in cases]
    unpacker.spool_files(None, "event", load_id, unpacker.map_single, dest_cases)
    return load_id


def copy_cases(cases: Sequence[str]) -> None:
    for case in cases:
        event_user_path = json_case_path(f"{case}.unpack")
        shutil.copy(event_user_path, unpacker.unpack_storage.storage._make_path(UnpackerStorage.UNPACKING_FOLDER))


def expect_load_package(load_id: str, expected_tables: Sequence[str]) -> Dict[str, str]:
    files = unpacker.load_storage.list_new_jobs(load_id)
    print(files)
    print(expected_tables)
    assert len(files) == len(expected_tables)
    ofl: Dict[str, str] = {}
    for expected_table in expected_tables:
        file_mask = unpacker.load_storage.build_loading_file_name(load_id, expected_table, "*")
        candidates = [f for f in files if fnmatch(f, f"{LoaderStorage.LOADING_FOLDER}/{file_mask}")]
        assert len(candidates) == 1
        ofl[expected_table] = candidates[0]
    return ofl


def expect_lines_file(load_file: str, line: int = 0) -> str:
    with unpacker.load_storage.storage.open(load_file) as f:
        lines = f.readlines()
    return lines[line], len(lines)


def assert_timestamp_data_type(data_type: DataType) -> None:
    # load generated schema
    schema_storage = SchemaStorage(unpacker.load_storage.storage.storage_path)
    loads = unpacker.load_storage.list_loads()
    event_schema = schema_storage.load_folder_schema(unpacker.load_storage.get_load_path(loads[0]))
    # in raw unpacker timestamp column must not be coerced to timestamp
    assert event_schema.get_table("event")["timestamp"]["data_type"] == data_type


def test_version() -> None:
    assert unpacker.configuration({"NAME": "test"})._VERSION == __version__
