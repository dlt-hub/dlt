import pytest
from fnmatch import fnmatch
from typing import Dict, Iterator, List, Sequence, Tuple

# from multiprocessing import get_start_method, Pool
# from multiprocessing.dummy import Pool as ThreadPool
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from dlt.common import json
from dlt.common.schema.schema import Schema
from dlt.common.utils import uniq_id
from dlt.common.typing import StrAny
from dlt.common.data_types import TDataType
from dlt.common.storages import NormalizeStorage, LoadStorage, ParsedLoadJobFileName, PackageStorage
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.configuration.container import Container

from dlt.extract.extract import ExtractorStorage
from dlt.normalize import Normalize

from tests.cases import JSON_TYPED_DICT, JSON_TYPED_DICT_TYPES
from tests.utils import (
    TEST_DICT_CONFIG_PROVIDER,
    assert_no_dict_key_starts_with,
    clean_test_storage,
    init_test_logging,
)
from tests.normalize.utils import (
    json_case_path,
    INSERT_CAPS,
    JSONL_CAPS,
    DEFAULT_CAPS,
    ALL_CAPABILITIES,
)


@pytest.fixture(scope="module", autouse=True)
def default_caps() -> Iterator[DestinationCapabilitiesContext]:
    # set the postgres caps as default for the whole module
    with Container().injectable_context(DEFAULT_CAPS()) as caps:
        yield caps


@pytest.fixture
def caps(request) -> Iterator[DestinationCapabilitiesContext]:
    # NOTE: you must put this fixture before any normalize fixture
    _caps = request.param()
    # inject a different right load format for filesystem and bigquery since the tests depend on this being jsonl
    if _caps.preferred_loader_file_format == "parquet":
        _caps.preferred_loader_file_format = "jsonl"
    with Container().injectable_context(_caps):
        yield _caps


@pytest.fixture()
def raw_normalize() -> Iterator[Normalize]:
    # does not install default schemas, so no type hints and row filters
    # uses default json normalizer that does not yield additional rasa tables
    yield from init_normalize()


@pytest.fixture
def rasa_normalize() -> Iterator[Normalize]:
    # install default schemas, includes type hints and row filters
    # uses rasa json normalizer that yields event table and separate tables for event types
    yield from init_normalize("tests/normalize/cases/schemas")


def init_normalize(default_schemas_path: str = None) -> Iterator[Normalize]:
    clean_test_storage()
    # pass schema config fields to schema storage via dict config provider
    with TEST_DICT_CONFIG_PROVIDER().values(
        {"import_schema_path": default_schemas_path, "external_schema_format": "json"}
    ):
        # inject the destination capabilities
        n = Normalize()
        yield n


@pytest.fixture(scope="module", autouse=True)
def logger_autouse() -> None:
    init_test_logging()


def test_initialize(rasa_normalize: Normalize) -> None:
    # create storages in fixture
    pass


@pytest.mark.parametrize("caps", JSONL_CAPS, indirect=True)
def test_normalize_single_user_event_jsonl(
    caps: DestinationCapabilitiesContext, raw_normalize: Normalize
) -> None:
    expected_tables, load_files = normalize_event_user(
        raw_normalize, "event.event.user_load_1", EXPECTED_USER_TABLES
    )
    # load, parse and verify jsonl
    for expected_table in expected_tables:
        get_line_from_file(raw_normalize.load_storage, load_files[expected_table])
    # return first line from event_user file
    event_text, lines = get_line_from_file(raw_normalize.load_storage, load_files["event"], 0)
    assert lines == 1
    event_json = json.loads(event_text)
    assert event_json["event"] == "user"
    assert event_json["parse_data__intent__name"] == "greet"
    assert event_json["text"] == "hello"
    event_text, lines = get_line_from_file(
        raw_normalize.load_storage,
        load_files["event__parse_data__response_selector__default__ranking"],
        9,
    )
    assert lines == 10
    event_json = json.loads(event_text)
    assert "id" in event_json
    assert "confidence" in event_json
    assert "intent_response_key" in event_json


@pytest.mark.parametrize("caps", INSERT_CAPS, indirect=True)
def test_normalize_single_user_event_insert(
    caps: DestinationCapabilitiesContext, raw_normalize: Normalize
) -> None:
    # mock_destination_caps(raw_normalize, caps)
    expected_tables, load_files = normalize_event_user(
        raw_normalize, "event.event.user_load_1", EXPECTED_USER_TABLES
    )
    # verify values line
    for expected_table in expected_tables:
        get_line_from_file(raw_normalize.load_storage, load_files[expected_table])
    # return first values line from event_user file
    event_text, lines = get_line_from_file(raw_normalize.load_storage, load_files["event"], 2)
    assert lines == 3
    assert "'user'" in event_text
    assert "'greet'" in event_text
    assert "'hello'" in event_text
    event_text, lines = get_line_from_file(
        raw_normalize.load_storage,
        load_files["event__parse_data__response_selector__default__ranking"],
        11,
    )
    assert lines == 12
    assert "(7005479104644416710," in event_text


@pytest.mark.parametrize("caps", JSONL_CAPS, indirect=True)
def test_normalize_filter_user_event(
    caps: DestinationCapabilitiesContext, rasa_normalize: Normalize
) -> None:
    load_id = extract_and_normalize_cases(rasa_normalize, ["event.event.user_load_v228_1"])
    _, load_files = expect_load_package(
        rasa_normalize.load_storage,
        load_id,
        [
            "event",
            "event_user",
            "event_user__metadata__user_nicknames",
            "event_user__parse_data__entities",
            "event_user__parse_data__entities__processors",
            "event_user__parse_data__intent_ranking",
        ],
    )
    event_text, lines = get_line_from_file(rasa_normalize.load_storage, load_files["event_user"], 0)
    assert lines == 1
    filtered_row = json.loads(event_text)
    assert "parse_data__intent__name" in filtered_row
    # response selectors are removed
    assert_no_dict_key_starts_with(filtered_row, "parse_data__response_selector")


@pytest.mark.parametrize("caps", JSONL_CAPS, indirect=True)
def test_normalize_filter_bot_event(
    caps: DestinationCapabilitiesContext, rasa_normalize: Normalize
) -> None:
    load_id = extract_and_normalize_cases(
        rasa_normalize, ["event.event.bot_load_metadata_2987398237498798"]
    )
    _, load_files = expect_load_package(
        rasa_normalize.load_storage, load_id, ["event", "event_bot"]
    )
    event_text, lines = get_line_from_file(rasa_normalize.load_storage, load_files["event_bot"], 0)
    assert lines == 1
    filtered_row = json.loads(event_text)
    assert "metadata__utter_action" in filtered_row
    assert "metadata__account_balance" not in filtered_row


@pytest.mark.parametrize("caps", JSONL_CAPS, indirect=True)
def test_preserve_slot_complex_value_json_l(
    caps: DestinationCapabilitiesContext, rasa_normalize: Normalize
) -> None:
    load_id = extract_and_normalize_cases(rasa_normalize, ["event.event.slot_session_metadata_1"])
    _, load_files = expect_load_package(
        rasa_normalize.load_storage, load_id, ["event", "event_slot"]
    )
    event_text, lines = get_line_from_file(rasa_normalize.load_storage, load_files["event_slot"], 0)
    assert lines == 1
    filtered_row = json.loads(event_text)
    assert type(filtered_row["value"]) is dict
    assert filtered_row["value"] == {"user_id": "world", "mitter_id": "hello"}


@pytest.mark.parametrize("caps", INSERT_CAPS, indirect=True)
def test_preserve_slot_complex_value_insert(
    caps: DestinationCapabilitiesContext, rasa_normalize: Normalize
) -> None:
    load_id = extract_and_normalize_cases(rasa_normalize, ["event.event.slot_session_metadata_1"])
    _, load_files = expect_load_package(
        rasa_normalize.load_storage, load_id, ["event", "event_slot"]
    )
    event_text, lines = get_line_from_file(rasa_normalize.load_storage, load_files["event_slot"], 2)
    assert lines == 3
    c_val = json.dumps({"user_id": "world", "mitter_id": "hello"})
    assert c_val in event_text


@pytest.mark.parametrize("caps", INSERT_CAPS, indirect=True)
def test_normalize_many_events_insert(
    caps: DestinationCapabilitiesContext, rasa_normalize: Normalize
) -> None:
    load_id = extract_and_normalize_cases(
        rasa_normalize, ["event.event.many_load_2", "event.event.user_load_1"]
    )
    expected_tables = EXPECTED_USER_TABLES_RASA_NORMALIZER + ["event_bot", "event_action"]
    _, load_files = expect_load_package(rasa_normalize.load_storage, load_id, expected_tables)
    # return first values line from event_user file
    event_text, lines = get_line_from_file(rasa_normalize.load_storage, load_files["event"], 4)
    # 2 lines header + 3 lines data
    assert lines == 5
    assert f"'{load_id}'" in event_text


@pytest.mark.parametrize("caps", JSONL_CAPS, indirect=True)
def test_normalize_many_events(
    caps: DestinationCapabilitiesContext, rasa_normalize: Normalize
) -> None:
    load_id = extract_and_normalize_cases(
        rasa_normalize, ["event.event.many_load_2", "event.event.user_load_1"]
    )
    expected_tables = EXPECTED_USER_TABLES_RASA_NORMALIZER + ["event_bot", "event_action"]
    _, load_files = expect_load_package(rasa_normalize.load_storage, load_id, expected_tables)
    # return first values line from event_user file
    event_text, lines = get_line_from_file(rasa_normalize.load_storage, load_files["event"], 2)
    # 3 lines data
    assert lines == 3
    assert f"{load_id}" in event_text


@pytest.mark.parametrize("caps", ALL_CAPABILITIES, indirect=True)
def test_normalize_raw_no_type_hints(
    caps: DestinationCapabilitiesContext, raw_normalize: Normalize
) -> None:
    normalize_event_user(raw_normalize, "event.event.user_load_1", EXPECTED_USER_TABLES)
    assert_timestamp_data_type(raw_normalize.load_storage, "double")


@pytest.mark.parametrize("caps", ALL_CAPABILITIES, indirect=True)
def test_normalize_raw_type_hints(
    caps: DestinationCapabilitiesContext, rasa_normalize: Normalize
) -> None:
    extract_and_normalize_cases(rasa_normalize, ["event.event.user_load_1"])
    assert_timestamp_data_type(rasa_normalize.load_storage, "timestamp")


@pytest.mark.parametrize("caps", ALL_CAPABILITIES, indirect=True)
def test_multiprocess_row_counting(
    caps: DestinationCapabilitiesContext, raw_normalize: Normalize
) -> None:
    extract_cases(raw_normalize.normalize_storage, ["github.events.load_page_1_duck"])
    # use real process pool in tests
    with ProcessPoolExecutor(max_workers=4) as p:
        raw_normalize.run(p)

    assert raw_normalize._row_counts["events"] == 100
    assert raw_normalize._row_counts["events__payload__pull_request__requested_reviewers"] == 24


@pytest.mark.parametrize("caps", ALL_CAPABILITIES, indirect=True)
def test_normalize_many_schemas(
    caps: DestinationCapabilitiesContext, rasa_normalize: Normalize
) -> None:
    extract_cases(
        rasa_normalize.normalize_storage,
        [
            "event.event.many_load_2",
            "event.event.user_load_1",
            "ethereum.blocks.9c1d9b504ea240a482b007788d5cd61c_2",
        ],
    )
    # use real process pool in tests
    with ProcessPoolExecutor(max_workers=4) as p:
        rasa_normalize.run(p)
    # must have two loading groups with model and event schemas
    loads = rasa_normalize.load_storage.list_normalized_packages()
    assert len(loads) == 2
    schemas = []
    # load all schemas
    for load_id in loads:
        schema = rasa_normalize.load_storage.normalized_packages.load_schema(load_id)
        schemas.append(schema.name)
        # expect event tables
        if schema.name == "event":
            expected_tables = EXPECTED_USER_TABLES_RASA_NORMALIZER + ["event_bot", "event_action"]
            expect_load_package(rasa_normalize.load_storage, load_id, expected_tables)
        if schema.name == "ethereum":
            expect_load_package(
                rasa_normalize.load_storage, load_id, EXPECTED_ETH_TABLES, full_schema_update=False
            )
    assert set(schemas) == set(["ethereum", "event"])


@pytest.mark.parametrize("caps", ALL_CAPABILITIES, indirect=True)
def test_normalize_typed_json(
    caps: DestinationCapabilitiesContext, raw_normalize: Normalize
) -> None:
    extract_items(raw_normalize.normalize_storage, [JSON_TYPED_DICT], "special", "special")
    with ThreadPoolExecutor(max_workers=1) as pool:
        raw_normalize.run(pool)
    loads = raw_normalize.load_storage.list_normalized_packages()
    assert len(loads) == 1
    # load all schemas
    schema = raw_normalize.load_storage.normalized_packages.load_schema(loads[0])
    assert schema.name == "special"
    # named as schema - default fallback
    table = schema.get_table_columns("special", include_incomplete=True)
    # assert inferred types
    for k, v in JSON_TYPED_DICT_TYPES.items():
        assert table[k]["data_type"] == v


@pytest.mark.parametrize("caps", ALL_CAPABILITIES, indirect=True)
def test_schema_changes(caps: DestinationCapabilitiesContext, raw_normalize: Normalize) -> None:
    doc = {"str": "text", "int": 1}
    extract_items(raw_normalize.normalize_storage, [doc], "evolution", "doc")
    load_id = normalize_pending(raw_normalize, "evolution")
    _, table_files = expect_load_package(raw_normalize.load_storage, load_id, ["doc"])
    get_line_from_file(raw_normalize.load_storage, table_files["doc"], 0)
    assert len(table_files["doc"]) == 1
    s: Schema = raw_normalize.load_or_create_schema(raw_normalize.schema_storage, "evolution")
    doc_table = s.get_table("doc")
    assert "str" in doc_table["columns"]
    assert "int" in doc_table["columns"]

    # add column to doc in second step
    doc2 = {"str": "text", "int": 1, "bool": True}
    extract_items(raw_normalize.normalize_storage, [doc, doc2, doc], "evolution", "doc")
    load_id = normalize_pending(raw_normalize, "evolution")
    _, table_files = expect_load_package(raw_normalize.load_storage, load_id, ["doc"])
    assert len(table_files["doc"]) == 1
    s = raw_normalize.load_or_create_schema(raw_normalize.schema_storage, "evolution")
    doc_table = s.get_table("doc")
    assert "bool" in doc_table["columns"]

    # add and change several tables in one step
    doc3 = {"comp": [doc]}
    doc_v = {"int": "hundred"}
    doc3_2v = {"comp": [doc2]}
    doc3_doc_v = {"comp": [doc_v]}
    extract_items(raw_normalize.normalize_storage, [doc3, doc, doc_v], "evolution", "doc")
    extract_items(raw_normalize.normalize_storage, [doc3_2v, doc3_doc_v], "evolution", "doc")
    load_id = normalize_pending(raw_normalize, "evolution")

    _, table_files = expect_load_package(raw_normalize.load_storage, load_id, ["doc", "doc__comp"])
    assert len(table_files["doc"]) == 1
    assert len(table_files["doc__comp"]) == 1
    s = raw_normalize.load_or_create_schema(raw_normalize.schema_storage, "evolution")
    doc_table = s.get_table("doc")
    assert {"_dlt_load_id", "_dlt_id", "str", "int", "bool", "int__v_text"} == set(
        doc_table["columns"].keys()
    )
    doc__comp_table = s.get_table("doc__comp")
    assert doc__comp_table["parent"] == "doc"
    assert {
        "_dlt_id",
        "_dlt_list_idx",
        "_dlt_parent_id",
        "str",
        "int",
        "bool",
        "int__v_text",
    } == set(doc__comp_table["columns"].keys())


@pytest.mark.parametrize("caps", ALL_CAPABILITIES, indirect=True)
def test_normalize_twice_with_flatten(
    caps: DestinationCapabilitiesContext, raw_normalize: Normalize
) -> None:
    load_id = extract_and_normalize_cases(raw_normalize, ["github.issues.load_page_5_duck"])
    _, table_files = expect_load_package(
        raw_normalize.load_storage, load_id, ["issues", "issues__labels", "issues__assignees"]
    )
    assert len(table_files["issues"]) == 1
    _, lines = get_line_from_file(raw_normalize.load_storage, table_files["issues"], 0)
    # insert writer adds 2 lines
    assert lines in (100, 102)

    # check if schema contains a few crucial tables
    def assert_schema(_schema: Schema):
        # convention = _schema._normalizers_config["names"]
        assert "reactions___1" in _schema.tables["issues"]["columns"]
        assert "reactions__x1" in _schema.tables["issues"]["columns"]
        assert "reactions__1" not in _schema.tables["issues"]["columns"]

    schema = raw_normalize.load_or_create_schema(raw_normalize.schema_storage, "github")
    assert_schema(schema)

    load_id = extract_and_normalize_cases(raw_normalize, ["github.issues.load_page_5_duck"])
    _, table_files = expect_load_package(
        raw_normalize.load_storage,
        load_id,
        ["issues", "issues__labels", "issues__assignees"],
        full_schema_update=False,
    )
    assert len(table_files["issues"]) == 1
    _, lines = get_line_from_file(raw_normalize.load_storage, table_files["issues"], 0)
    # insert writer adds 2 lines
    assert lines in (100, 102)
    schema = raw_normalize.load_or_create_schema(raw_normalize.schema_storage, "github")
    assert_schema(schema)


def test_group_worker_files() -> None:
    files = ["f%03d" % idx for idx in range(0, 100)]

    assert Normalize.group_worker_files([], 4) == []
    assert Normalize.group_worker_files(["f001"], 1) == [["f001"]]
    assert Normalize.group_worker_files(["f001"], 100) == [["f001"]]
    assert Normalize.group_worker_files(files[:4], 4) == [["f000"], ["f001"], ["f002"], ["f003"]]
    assert Normalize.group_worker_files(files[:5], 4) == [
        ["f000"],
        ["f001"],
        ["f002"],
        ["f003", "f004"],
    ]
    assert Normalize.group_worker_files(files[:8], 4) == [
        ["f000", "f001"],
        ["f002", "f003"],
        ["f004", "f005"],
        ["f006", "f007"],
    ]
    assert Normalize.group_worker_files(files[:8], 3) == [
        ["f000", "f001"],
        ["f002", "f003", "f006"],
        ["f004", "f005", "f007"],
    ]
    assert Normalize.group_worker_files(files[:5], 3) == [
        ["f000"],
        ["f001", "f003"],
        ["f002", "f004"],
    ]

    # check if sorted
    files = ["tab1.1", "chd.3", "tab1.2", "chd.4", "tab1.3"]
    assert Normalize.group_worker_files(files, 3) == [
        ["chd.3"],
        ["chd.4", "tab1.2"],
        ["tab1.1", "tab1.3"],
    ]


EXPECTED_ETH_TABLES = [
    "blocks",
    "blocks__transactions",
    "blocks__transactions__logs",
    "blocks__transactions__logs__topics",
    "blocks__uncles",
    "blocks__transactions__access_list",
    "blocks__transactions__access_list__storage_keys",
]

EXPECTED_USER_TABLES_RASA_NORMALIZER = [
    "event",
    "event_user",
    "event_user__parse_data__intent_ranking",
]


EXPECTED_USER_TABLES = [
    "event",
    "event__parse_data__intent_ranking",
    "event__parse_data__response_selector__all_retrieval_intents",
    "event__parse_data__response_selector__default__ranking",
    "event__parse_data__response_selector__default__response__response_templates",
    "event__parse_data__response_selector__default__response__responses",
]


def extract_items(
    normalize_storage: NormalizeStorage, items: Sequence[StrAny], schema_name: str, table_name: str
) -> None:
    extractor = ExtractorStorage(normalize_storage.config)
    load_id = extractor.create_load_package(Schema(schema_name))
    extractor.write_data_item("puae-jsonl", load_id, schema_name, table_name, items, None)
    extractor.close_writers(load_id)
    extractor.commit_extract_files(load_id)


def normalize_event_user(
    normalize: Normalize, case: str, expected_user_tables: List[str] = None
) -> Tuple[List[str], Dict[str, List[str]]]:
    expected_user_tables = expected_user_tables or EXPECTED_USER_TABLES_RASA_NORMALIZER
    load_id = extract_and_normalize_cases(normalize, [case])
    return expect_load_package(normalize.load_storage, load_id, expected_user_tables)


def extract_and_normalize_cases(normalize: Normalize, cases: Sequence[str]) -> str:
    extract_cases(normalize.normalize_storage, cases)
    return normalize_pending(normalize)


def normalize_pending(normalize: Normalize, schema_name: str = "event") -> str:
    load_id = uniq_id()
    normalize.load_storage.new_packages.create_package(load_id)
    # pool not required for map_single
    files = normalize.normalize_storage.list_files_to_normalize_sorted()
    # create schema if it does not exist
    for schema_name, files_in_schema in normalize.normalize_storage.group_by_schema(files):
        normalize.spool_files(schema_name, load_id, normalize.map_single, list(files_in_schema))
    return load_id


def extract_cases(normalize_storage: NormalizeStorage, cases: Sequence[str]) -> None:
    for case in cases:
        schema_name, table_name, _, _ = NormalizeStorage.parse_normalize_file_name(case + ".jsonl")
        with open(json_case_path(case), "rb") as f:
            items = json.load(f)
        extract_items(normalize_storage, items, schema_name, table_name)


def expect_load_package(
    load_storage: LoadStorage,
    load_id: str,
    expected_tables: Sequence[str],
    full_schema_update: bool = True,
) -> Tuple[List[str], Dict[str, List[str]]]:
    # normalize tables as paths (original json is snake case so we may do it without real lineage info)
    schema = load_storage.normalized_packages.load_schema(load_id)
    # we are still in destination caps context so schema contains length
    assert schema.naming.max_length > 0
    expected_tables = [
        schema.naming.shorten_fragments(*schema.naming.break_path(table))
        for table in expected_tables
    ]

    # find jobs and processed files
    files = load_storage.list_new_jobs(load_id)
    files_tables = [ParsedLoadJobFileName.parse(file).table_name for file in files]
    assert set(files_tables) == set(expected_tables)
    ofl: Dict[str, List[str]] = {}
    for expected_table in expected_tables:
        # find all files for particular table, ignoring file id
        file_mask = PackageStorage.build_job_file_name(
            expected_table,
            "*",
            validate_components=False,
            loader_file_format=load_storage.loader_file_format,
        )
        # files are in normalized/<load_id>/new_jobs
        file_path = load_storage.normalized_packages.get_job_file_path(
            load_id, "new_jobs", file_mask
        )
        candidates = [f for f in files if fnmatch(f, file_path)]
        # assert len(candidates) == 1
        ofl[expected_table] = candidates
    # get the schema update
    schema_update = load_storage.begin_schema_update(load_id)
    if full_schema_update:
        assert set(expected_tables) == set(schema_update.keys())
    else:
        assert set(expected_tables) >= set(schema_update.keys())
    return expected_tables, ofl


def get_line_from_file(
    load_storage: LoadStorage, loaded_files: List[str], return_line: int = 0
) -> Tuple[str, int]:
    lines = []
    for file in loaded_files:
        with load_storage.normalized_packages.storage.open_file(file) as f:
            lines.extend(f.readlines())
    return lines[return_line], len(lines)


def assert_timestamp_data_type(load_storage: LoadStorage, data_type: TDataType) -> None:
    # load generated schema
    loads = load_storage.list_normalized_packages()
    event_schema = load_storage.normalized_packages.load_schema(loads[0])
    # in raw normalize timestamp column must not be coerced to timestamp
    assert event_schema.get_table_columns("event")["timestamp"]["data_type"] == data_type
