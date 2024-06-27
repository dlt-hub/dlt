import pytest
from fnmatch import fnmatch
from typing import Dict, Iterator, List, Sequence, Tuple
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from dlt.common import json
from dlt.common.destination.capabilities import TLoaderFileFormat
from dlt.common.schema.schema import Schema
from dlt.common.schema.utils import new_table
from dlt.common.storages.exceptions import SchemaNotFoundError
from dlt.common.typing import StrAny
from dlt.common.data_types import TDataType
from dlt.common.storages import NormalizeStorage, LoadStorage, ParsedLoadJobFileName, PackageStorage
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.configuration.container import Container

from dlt.extract.extract import ExtractStorage
from dlt.normalize import Normalize
from dlt.normalize.worker import group_worker_files
from dlt.normalize.exceptions import NormalizeJobFailed

from tests.cases import JSON_TYPED_DICT, JSON_TYPED_DICT_TYPES
from tests.utils import (
    TEST_DICT_CONFIG_PROVIDER,
    MockPipeline,
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
        caps.preferred_loader_file_format,
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
        rasa_normalize.load_storage,
        caps.preferred_loader_file_format,
        load_id,
        ["event", "event_bot"],
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
        rasa_normalize.load_storage,
        caps.preferred_loader_file_format,
        load_id,
        ["event", "event_slot"],
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
        rasa_normalize.load_storage,
        caps.preferred_loader_file_format,
        load_id,
        ["event", "event_slot"],
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
    _, load_files = expect_load_package(
        rasa_normalize.load_storage, caps.preferred_loader_file_format, load_id, expected_tables
    )
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
    _, load_files = expect_load_package(
        rasa_normalize.load_storage, caps.preferred_loader_file_format, load_id, expected_tables
    )
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
def test_multiprocessing_row_counting(
    caps: DestinationCapabilitiesContext, raw_normalize: Normalize
) -> None:
    extract_cases(raw_normalize, ["github.events.load_page_1_duck"])
    # use real process pool in tests
    with ProcessPoolExecutor(max_workers=4) as p:
        raw_normalize.run(p)
    # get step info
    step_info = raw_normalize.get_step_info(MockPipeline("multiprocessing_pipeline", True))  # type: ignore[abstract]
    assert step_info.row_counts["events"] == 100
    assert step_info.row_counts["events__payload__pull_request__requested_reviewers"] == 24
    # check if single load id
    assert len(step_info.loads_ids) == 1
    row_counts = {
        t: m.items_count
        for t, m in step_info.metrics[step_info.loads_ids[0]][0]["table_metrics"].items()
    }
    assert row_counts == step_info.row_counts


@pytest.mark.parametrize("caps", ALL_CAPABILITIES, indirect=True)
def test_normalize_many_packages(
    caps: DestinationCapabilitiesContext, rasa_normalize: Normalize
) -> None:
    extract_cases(
        rasa_normalize,
        [
            "event.event.many_load_2",
            "event.event.user_load_1",
        ],
    )
    extract_cases(
        rasa_normalize,
        [
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
            expect_load_package(
                rasa_normalize.load_storage,
                caps.preferred_loader_file_format,
                load_id,
                expected_tables,
            )
        if schema.name == "ethereum":
            expect_load_package(
                rasa_normalize.load_storage,
                caps.preferred_loader_file_format,
                load_id,
                EXPECTED_ETH_TABLES,
                full_schema_update=False,
            )
    assert set(schemas) == set(["ethereum", "event"])


@pytest.mark.parametrize("caps", ALL_CAPABILITIES, indirect=True)
def test_normalize_typed_json(
    caps: DestinationCapabilitiesContext, raw_normalize: Normalize
) -> None:
    extract_items(raw_normalize.normalize_storage, [JSON_TYPED_DICT], Schema("special"), "special")
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
    extract_items(raw_normalize.normalize_storage, [doc], Schema("evolution"), "doc")
    load_id = normalize_pending(raw_normalize)
    _, table_files = expect_load_package(
        raw_normalize.load_storage, caps.preferred_loader_file_format, load_id, ["doc"]
    )
    get_line_from_file(raw_normalize.load_storage, table_files["doc"], 0)
    assert len(table_files["doc"]) == 1
    schema = raw_normalize.schema_storage.load_schema("evolution")
    doc_table = schema.get_table("doc")
    assert "str" in doc_table["columns"]
    assert "int" in doc_table["columns"]

    # add column to doc in second step
    doc2 = {"str": "text", "int": 1, "bool": True}
    extract_items(raw_normalize.normalize_storage, [doc, doc2, doc], schema, "doc")
    load_id = normalize_pending(raw_normalize)
    _, table_files = expect_load_package(
        raw_normalize.load_storage, caps.preferred_loader_file_format, load_id, ["doc"]
    )
    assert len(table_files["doc"]) == 1
    schema = raw_normalize.schema_storage.load_schema("evolution")
    doc_table = schema.get_table("doc")
    assert "bool" in doc_table["columns"]

    # add and change several tables in one step
    doc3 = {"comp": [doc]}
    doc_v = {"int": "hundred"}
    doc3_2v = {"comp": [doc2]}
    doc3_doc_v = {"comp": [doc_v]}
    extract_items(
        raw_normalize.normalize_storage, [doc3, doc, doc_v, doc3_2v, doc3_doc_v], schema, "doc"
    )
    # schema = raw_normalize.schema_storage.load_schema("evolution")
    # extract_items(raw_normalize.normalize_storage, [doc3_2v, doc3_doc_v], schema, "doc")
    load_id = normalize_pending(raw_normalize)

    _, table_files = expect_load_package(
        raw_normalize.load_storage, caps.preferred_loader_file_format, load_id, ["doc", "doc__comp"]
    )
    assert len(table_files["doc"]) == 1
    assert len(table_files["doc__comp"]) == 1
    schema = raw_normalize.schema_storage.load_schema("evolution")
    doc_table = schema.get_table("doc")
    assert {"_dlt_load_id", "_dlt_id", "str", "int", "bool", "int__v_text"} == set(
        doc_table["columns"].keys()
    )
    doc__comp_table = schema.get_table("doc__comp")
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
        raw_normalize.load_storage,
        caps.preferred_loader_file_format,
        load_id,
        ["issues", "issues__labels", "issues__assignees"],
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

    schema = raw_normalize.schema_storage.load_schema("github")
    assert_schema(schema)

    load_id = extract_and_normalize_cases(raw_normalize, ["github.issues.load_page_5_duck"])
    _, table_files = expect_load_package(
        raw_normalize.load_storage,
        caps.preferred_loader_file_format,
        load_id,
        ["issues", "issues__labels", "issues__assignees"],
        full_schema_update=False,
    )
    assert len(table_files["issues"]) == 1
    _, lines = get_line_from_file(raw_normalize.load_storage, table_files["issues"], 0)
    # insert writer adds 2 lines
    assert lines in (100, 102)
    schema = raw_normalize.schema_storage.load_schema("github")
    assert_schema(schema)


def test_normalize_retry(raw_normalize: Normalize) -> None:
    load_id = extract_cases(raw_normalize, ["github.issues.load_page_5_duck"])
    schema = raw_normalize.normalize_storage.extracted_packages.load_schema(load_id)
    schema.set_schema_contract("freeze")
    raw_normalize.normalize_storage.extracted_packages.save_schema(load_id, schema)
    # will fail on contract violation
    with pytest.raises(NormalizeJobFailed):
        raw_normalize.run(None)

    # drop the contract requirements
    schema.set_schema_contract("evolve")
    # save this schema into schema storage from which normalizer must pick it up
    raw_normalize.schema_storage.save_schema(schema)
    # raw_normalize.normalize_storage.extracted_packages.save_schema(load_id, schema)
    # subsequent run must succeed
    raw_normalize.run(None)
    _, table_files = expect_load_package(
        raw_normalize.load_storage,
        raw_normalize.config.destination_capabilities.preferred_loader_file_format,
        load_id,
        ["issues", "issues__labels", "issues__assignees"],
    )
    assert len(table_files["issues"]) == 1


def test_collect_metrics_on_exception(raw_normalize: Normalize) -> None:
    load_id = extract_cases(raw_normalize, ["github.issues.load_page_5_duck"])
    schema = raw_normalize.normalize_storage.extracted_packages.load_schema(load_id)
    schema.set_schema_contract("freeze")
    raw_normalize.normalize_storage.extracted_packages.save_schema(load_id, schema)
    # will fail on contract violation
    with pytest.raises(NormalizeJobFailed) as job_ex:
        raw_normalize.run(None)
    # we excepted on a first row so nothing was written
    # TODO: improve this test to write some rows in buffered writer
    assert len(job_ex.value.writer_metrics) == 0
    raw_normalize.get_step_info(MockPipeline("multiprocessing_pipeline", True))  # type: ignore[abstract]


def test_group_worker_files() -> None:
    files = ["f%03d" % idx for idx in range(0, 100)]

    assert group_worker_files([], 4) == []
    assert group_worker_files(["f001"], 1) == [["f001"]]
    assert group_worker_files(["f001"], 100) == [["f001"]]
    assert group_worker_files(files[:4], 4) == [["f000"], ["f001"], ["f002"], ["f003"]]
    assert group_worker_files(files[:5], 4) == [
        ["f000"],
        ["f001"],
        ["f002"],
        ["f003", "f004"],
    ]
    assert group_worker_files(files[:8], 4) == [
        ["f000", "f001"],
        ["f002", "f003"],
        ["f004", "f005"],
        ["f006", "f007"],
    ]
    assert group_worker_files(files[:8], 3) == [
        ["f000", "f001"],
        ["f002", "f003", "f006"],
        ["f004", "f005", "f007"],
    ]
    assert group_worker_files(files[:5], 3) == [
        ["f000"],
        ["f001", "f003"],
        ["f002", "f004"],
    ]

    # check if sorted
    files = ["tab1.1", "chd.3", "tab1.2", "chd.4", "tab1.3"]
    assert group_worker_files(files, 3) == [
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
    normalize_storage: NormalizeStorage, items: Sequence[StrAny], schema: Schema, table_name: str
) -> str:
    extractor = ExtractStorage(normalize_storage.config)
    load_id = extractor.create_load_package(schema)
    extractor.item_storages["object"].write_data_item(load_id, schema.name, table_name, items, None)
    extractor.close_writers(load_id)
    extractor.commit_new_load_package(load_id, schema)
    return load_id


def normalize_event_user(
    normalize: Normalize, case: str, expected_user_tables: List[str] = None
) -> Tuple[List[str], Dict[str, List[str]]]:
    expected_user_tables = expected_user_tables or EXPECTED_USER_TABLES_RASA_NORMALIZER
    load_id = extract_and_normalize_cases(normalize, [case])
    return expect_load_package(
        normalize.load_storage,
        normalize.config.destination_capabilities.preferred_loader_file_format,
        load_id,
        expected_user_tables,
    )


def extract_and_normalize_cases(normalize: Normalize, cases: Sequence[str]) -> str:
    extract_cases(normalize, cases)
    return normalize_pending(normalize)


def normalize_pending(normalize: Normalize, schema: Schema = None) -> str:
    # pool not required for map_single
    load_ids = normalize.normalize_storage.extracted_packages.list_packages()
    assert len(load_ids) == 1, "Only one package allowed or rewrite tests"
    for load_id in load_ids:
        normalize._step_info_start_load_id(load_id)
        normalize.load_storage.new_packages.create_package(load_id)
        # read schema from package
        schema = schema or normalize.normalize_storage.extracted_packages.load_schema(load_id)
        # get files
        schema_files = normalize.normalize_storage.extracted_packages.list_new_jobs(load_id)
        # normalize without pool
        normalize.spool_files(load_id, schema, normalize.map_single, schema_files)

    return load_id


def extract_cases(normalize: Normalize, cases: Sequence[str]) -> str:
    items: List[StrAny] = []
    for case in cases:
        # our cases have schema and table name encoded in file name
        schema_name, table_name, _ = case.split(".", maxsplit=3)
        with open(json_case_path(case), "rb") as f:
            item = json.load(f)
            if isinstance(item, list):
                items.extend(item)
            else:
                items.append(item)
    # we assume that all items belonged to a single schema
    return extract_items(
        normalize.normalize_storage,
        items,
        load_or_create_schema(normalize, schema_name),
        table_name,
    )


def load_or_create_schema(normalize: Normalize, schema_name: str) -> Schema:
    try:
        schema = normalize.schema_storage.load_schema(schema_name)
        schema.update_normalizers()
    except SchemaNotFoundError:
        schema = Schema(schema_name)
    return schema


def expect_load_package(
    load_storage: LoadStorage,
    file_format: TLoaderFileFormat,
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
            loader_file_format=file_format,
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


def test_removal_of_normalizer_schema_section_and_add_seen_data(raw_normalize: Normalize) -> None:
    extract_cases(
        raw_normalize,
        [
            "event.event.user_load_1",
        ],
    )
    load_ids = raw_normalize.normalize_storage.extracted_packages.list_packages()
    assert len(load_ids) == 1
    extracted_schema = raw_normalize.normalize_storage.extracted_packages.load_schema(load_ids[0])

    # add some normalizer blocks
    extracted_schema.tables["event"] = new_table("event")
    extracted_schema.tables["event__parse_data__intent_ranking"] = new_table(
        "event__parse_data__intent_ranking"
    )
    extracted_schema.tables["event__random_table"] = new_table("event__random_table")

    # add x-normalizer info (and other block to control)
    extracted_schema.tables["event"]["x-normalizer"] = {"evolve-columns-once": True}
    extracted_schema.tables["event"]["x-other-info"] = "blah"  # type: ignore
    extracted_schema.tables["event__parse_data__intent_ranking"]["x-normalizer"] = {
        "seen-data": True,
        "random-entry": 1234,
    }
    extracted_schema.tables["event__random_table"]["x-normalizer"] = {"evolve-columns-once": True}

    normalize_pending(raw_normalize, extracted_schema)
    schema = raw_normalize.schema_storage.load_schema("event")
    # seen data gets added, schema settings get removed
    assert schema.tables["event"]["x-normalizer"] == {"seen-data": True}
    assert schema.tables["event__parse_data__intent_ranking"]["x-normalizer"] == {
        "seen-data": True,
        "random-entry": 1234,
    }
    # no data seen here, so seen-data is not set and evolve settings stays until first data is seen
    assert schema.tables["event__random_table"]["x-normalizer"] == {"evolve-columns-once": True}
    assert "x-other-info" in schema.tables["event"]
