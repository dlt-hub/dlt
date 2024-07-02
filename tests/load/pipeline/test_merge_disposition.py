from copy import copy
import pytest
import random
from typing import List
import pytest
import yaml

import dlt

from dlt.common import json, pendulum
from dlt.common.configuration.container import Container
from dlt.common.pipeline import StateInjectableContext
from dlt.common.schema.utils import has_table_seen_data
from dlt.common.schema.exceptions import (
    SchemaException,
    UnboundColumnException,
    CannotCoerceNullException,
)
from dlt.common.typing import StrAny
from dlt.common.utils import digest128
from dlt.extract import DltResource
from dlt.sources.helpers.transform import skip_first, take_first
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.normalize.exceptions import NormalizeJobFailed

from tests.pipeline.utils import assert_load_info, load_table_counts, select_data
from tests.load.utils import (
    normalize_storage_table_cols,
    destinations_configs,
    DestinationTestConfiguration,
)

# uncomment add motherduck tests
# NOTE: the tests are passing but we disable them due to frequent ATTACH DATABASE timeouts
# ACTIVE_DESTINATIONS += ["motherduck"]


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
def test_merge_on_keys_in_schema(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("eth_2", dev_mode=True)

    with open("tests/common/cases/schemas/eth/ethereum_schema_v5.yml", "r", encoding="utf-8") as f:
        schema = dlt.Schema.from_dict(yaml.safe_load(f))

    # make block uncles unseen to trigger filtering loader in loader for child tables
    if has_table_seen_data(schema.tables["blocks__uncles"]):
        del schema.tables["blocks__uncles"]["x-normalizer"]
        assert not has_table_seen_data(schema.tables["blocks__uncles"])

    with open(
        "tests/normalize/cases/ethereum.blocks.9c1d9b504ea240a482b007788d5cd61c_2.json",
        "r",
        encoding="utf-8",
    ) as f:
        data = json.load(f)

    # take only the first block. the first block does not have uncles so this table should not be created and merged
    info = p.run(
        data[:1],
        table_name="blocks",
        write_disposition="merge",
        schema=schema,
        loader_file_format=destination_config.file_format,
    )
    assert_load_info(info)
    eth_1_counts = load_table_counts(p, "blocks")
    # we load a single block
    assert eth_1_counts["blocks"] == 1
    # check root key propagation
    assert (
        p.default_schema.tables["blocks__transactions"]["columns"]["_dlt_root_id"]["root_key"]
        is True
    )
    # now we load the whole dataset. blocks should be created which adds columns to blocks
    # if the table would be created before the whole load would fail because new columns have hints
    info = p.run(
        data,
        table_name="blocks",
        write_disposition="merge",
        schema=schema,
        loader_file_format=destination_config.file_format,
    )
    eth_2_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    # we have 2 blocks in dataset
    assert eth_2_counts["blocks"] == 2 if destination_config.supports_merge else 3
    # make sure we have same record after merging full dataset again
    info = p.run(
        data,
        table_name="blocks",
        write_disposition="merge",
        schema=schema,
        loader_file_format=destination_config.file_format,
    )
    assert_load_info(info)
    # for non merge destinations we just check that the run passes
    if not destination_config.supports_merge:
        return
    eth_3_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    assert eth_2_counts == eth_3_counts


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
def test_merge_on_ad_hoc_primary_key(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("github_1", dev_mode=True)

    with open(
        "tests/normalize/cases/github.issues.load_page_5_duck.json", "r", encoding="utf-8"
    ) as f:
        data = json.load(f)
    # note: NodeId will be normalized to "node_id" which exists in the schema
    info = p.run(
        data[:17],
        table_name="issues",
        write_disposition="merge",
        primary_key="NodeId",
        loader_file_format=destination_config.file_format,
    )
    assert_load_info(info)
    github_1_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    # 17 issues
    assert github_1_counts["issues"] == 17
    # primary key set on issues
    assert p.default_schema.tables["issues"]["columns"]["node_id"]["primary_key"] is True
    assert p.default_schema.tables["issues"]["columns"]["node_id"]["data_type"] == "text"
    assert p.default_schema.tables["issues"]["columns"]["node_id"]["nullable"] is False

    info = p.run(
        data[5:],
        table_name="issues",
        write_disposition="merge",
        primary_key="node_id",
        loader_file_format=destination_config.file_format,
    )
    assert_load_info(info)
    # for non merge destinations we just check that the run passes
    if not destination_config.supports_merge:
        return
    github_2_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    # 100 issues total
    assert github_2_counts["issues"] == 100
    # still 100 after the reload


@dlt.source(root_key=True)
def github():
    @dlt.resource(
        table_name="issues",
        write_disposition="merge",
        primary_key="id",
        merge_key=("node_id", "url"),
    )
    def load_issues():
        with open(
            "tests/normalize/cases/github.issues.load_page_5_duck.json", "r", encoding="utf-8"
        ) as f:
            for item in json.load(f):
                yield item

    return load_issues


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
def test_merge_source_compound_keys_and_changes(
    destination_config: DestinationTestConfiguration,
) -> None:
    p = destination_config.setup_pipeline("github_3", dev_mode=True)

    info = p.run(github(), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    github_1_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    # 100 issues total
    assert github_1_counts["issues"] == 100
    # check keys created
    assert (
        p.default_schema.tables["issues"]["columns"]["node_id"].items()
        > {"merge_key": True, "data_type": "text", "nullable": False}.items()
    )
    assert (
        p.default_schema.tables["issues"]["columns"]["url"].items()
        > {"merge_key": True, "data_type": "text", "nullable": False}.items()
    )
    assert (
        p.default_schema.tables["issues"]["columns"]["id"].items()
        > {"primary_key": True, "data_type": "bigint", "nullable": False}.items()
    )

    # append load_issues resource
    info = p.run(
        github().load_issues,
        write_disposition="append",
        loader_file_format=destination_config.file_format,
    )
    assert_load_info(info)
    assert p.default_schema.tables["issues"]["write_disposition"] == "append"
    # the counts of all tables must be double
    github_2_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    assert {k: v * 2 for k, v in github_1_counts.items()} == github_2_counts

    # now replace all resources
    info = p.run(
        github(), write_disposition="replace", loader_file_format=destination_config.file_format
    )
    assert_load_info(info)
    assert p.default_schema.tables["issues"]["write_disposition"] == "replace"
    # assert p.default_schema.tables["issues__labels"]["write_disposition"] == "replace"
    # the counts of all tables must be double
    github_3_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    assert github_1_counts == github_3_counts


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
def test_merge_no_child_tables(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("github_3", dev_mode=True)
    github_data = github()
    assert github_data.max_table_nesting is None
    assert github_data.root_key is True
    # set max nesting to 0 so no child tables are generated
    github_data.max_table_nesting = 0
    assert github_data.max_table_nesting == 0
    github_data.root_key = False
    assert github_data.root_key is False

    # take only first 15 elements
    github_data.load_issues.add_filter(take_first(15))
    info = p.run(github_data, loader_file_format=destination_config.file_format)
    assert len(p.default_schema.data_tables()) == 1
    assert "issues" in p.default_schema.tables
    assert_load_info(info)
    github_1_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    assert github_1_counts["issues"] == 15

    # load all
    github_data = github()
    github_data.max_table_nesting = 0
    info = p.run(github_data, loader_file_format=destination_config.file_format)
    assert_load_info(info)
    github_2_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    # 100 issues total, or 115 if merge is not supported
    assert github_2_counts["issues"] == 100 if destination_config.supports_merge else 115


# mark as essential for now
@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, local_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_merge_no_merge_keys(destination_config: DestinationTestConfiguration) -> None:
    # NOTE: we can test filesystem destination merge behavior here too, will also fallback!
    if destination_config.file_format == "insert_values":
        pytest.skip("Insert values row count checking is buggy, skipping")
    p = destination_config.setup_pipeline("github_3", dev_mode=True)
    github_data = github()
    # remove all keys
    github_data.load_issues.apply_hints(merge_key=(), primary_key=())
    # skip first 45 rows
    github_data.load_issues.add_filter(skip_first(45))
    info = p.run(github_data, loader_file_format=destination_config.file_format)
    assert_load_info(info)
    github_1_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    assert github_1_counts["issues"] == 100 - 45

    # take first 10 rows.
    github_data = github()
    # remove all keys
    github_data.load_issues.apply_hints(merge_key=(), primary_key=())
    # skip first 45 rows
    github_data.load_issues.add_filter(take_first(10))
    info = p.run(github_data, loader_file_format=destination_config.file_format)
    assert_load_info(info)
    github_1_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    # we have 10 rows more, merge falls back to append if no keys present
    assert github_1_counts["issues"] == 100 - 45 + 10


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, file_format="parquet"),
    ids=lambda x: x.name,
)
def test_pipeline_load_parquet(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("github_3", dev_mode=True)
    # do not save state to destination so jobs counting is easier
    p.config.restore_from_destination = False
    github_data = github()
    # generate some complex types
    github_data.max_table_nesting = 2
    github_data_copy = github()
    github_data_copy.max_table_nesting = 2
    info = p.run(
        [github_data, github_data_copy], loader_file_format="parquet", write_disposition="merge"
    )
    assert_load_info(info)
    # make sure it was parquet or sql transforms
    expected_formats = ["parquet"]
    if p.staging:
        # allow references if staging is present
        expected_formats.append("reference")
    files = p.get_load_package_info(p.list_completed_load_packages()[0]).jobs["completed_jobs"]
    assert all(f.job_file_info.file_format in expected_formats + ["sql"] for f in files)

    github_1_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    expected_rows = 100
    if not destination_config.supports_merge:
        expected_rows *= 2
    assert github_1_counts["issues"] == expected_rows

    # now retry with replace
    github_data = github()
    # generate some complex types
    github_data.max_table_nesting = 2
    info = p.run(github_data, loader_file_format="parquet", write_disposition="replace")
    assert_load_info(info)
    # make sure it was parquet or sql inserts
    files = p.get_load_package_info(p.list_completed_load_packages()[1]).jobs["completed_jobs"]
    if destination_config.force_iceberg:
        # iceberg uses sql to copy tables
        expected_formats.append("sql")
    assert all(f.job_file_info.file_format in expected_formats for f in files)

    github_1_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    assert github_1_counts["issues"] == 100


@dlt.transformer(
    name="github_repo_events",
    primary_key="id",
    write_disposition="merge",
    table_name=lambda i: i["type"],
)
def github_repo_events(
    page: List[StrAny],
    last_created_at=dlt.sources.incremental("created_at", "1970-01-01T00:00:00Z"),
):
    """A transformer taking a stream of github events and dispatching them to tables named by event type. Deduplicates be 'id'. Loads incrementally by 'created_at'"""
    yield page


@dlt.transformer(name="github_repo_events", primary_key="id", write_disposition="merge")
def github_repo_events_table_meta(
    page: List[StrAny],
    last_created_at=dlt.sources.incremental("created_at", "1970-01-01T00:00:00Z"),
):
    """A transformer taking a stream of github events and dispatching them to tables using table meta. Deduplicates be 'id'. Loads incrementally by 'created_at'"""
    yield from [dlt.mark.with_table_name(p, p["type"]) for p in page]


@dlt.resource
def _get_shuffled_events(shuffle: bool = dlt.secrets.value):
    with open(
        "tests/normalize/cases/github.events.load_page_1_duck.json", "r", encoding="utf-8"
    ) as f:
        issues = json.load(f)
        # random order
        if shuffle:
            random.shuffle(issues)
        yield issues


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
@pytest.mark.parametrize("github_resource", [github_repo_events, github_repo_events_table_meta])
def test_merge_with_dispatch_and_incremental(
    destination_config: DestinationTestConfiguration, github_resource: DltResource
) -> None:
    newest_issues = list(
        sorted(_get_shuffled_events(True), key=lambda x: x["created_at"], reverse=True)
    )
    newest_issue = newest_issues[0]

    @dlt.resource
    def _new_event(node_id):
        new_i = copy(newest_issue)
        new_i["id"] = str(random.randint(0, 2 ^ 32))
        new_i["created_at"] = pendulum.now().isoformat()
        new_i["node_id"] = node_id
        # yield pages
        yield [new_i]

    @dlt.resource
    def _updated_event(node_id):
        new_i = copy(newest_issue)
        new_i["created_at"] = pendulum.now().isoformat()
        new_i["node_id"] = node_id
        # yield pages
        yield [new_i]

    # inject state that we can inspect and will be shared across calls
    with Container().injectable_context(StateInjectableContext(state={})):
        assert len(list(_get_shuffled_events(True) | github_resource)) == 100
        incremental_state = github_resource.state
        assert (
            incremental_state["incremental"]["created_at"]["last_value"]
            == newest_issue["created_at"]
        )
        assert incremental_state["incremental"]["created_at"]["unique_hashes"] == [
            digest128(f'"{newest_issue["id"]}"')
        ]
        # subsequent load will skip all elements
        assert len(list(_get_shuffled_events(True) | github_resource)) == 0
        # add one more issue
        assert len(list(_new_event("new_node") | github_resource)) == 1
        assert (
            incremental_state["incremental"]["created_at"]["last_value"]
            > newest_issue["created_at"]
        )
        assert incremental_state["incremental"]["created_at"]["unique_hashes"] != [
            digest128(str(newest_issue["id"]))
        ]

    # load to destination
    p = destination_config.setup_pipeline("github_3", dev_mode=True)
    info = p.run(
        _get_shuffled_events(True) | github_resource,
        loader_file_format=destination_config.file_format,
    )
    assert_load_info(info)
    # get top tables
    counts = load_table_counts(
        p, *[t["name"] for t in p.default_schema.data_tables() if t.get("parent") is None]
    )
    # total number of events in all top tables == 100
    assert sum(counts.values()) == 100
    # this should skip all events due to incremental load
    info = p.run(
        _get_shuffled_events(True) | github_resource,
        loader_file_format=destination_config.file_format,
    )
    # no packages were loaded
    assert len(info.loads_ids) == 0

    # load one more event with a new id
    info = p.run(
        _new_event("new_node") | github_resource, loader_file_format=destination_config.file_format
    )
    assert_load_info(info)
    counts = load_table_counts(
        p, *[t["name"] for t in p.default_schema.data_tables() if t.get("parent") is None]
    )
    assert sum(counts.values()) == 101
    # all the columns have primary keys and merge disposition derived from resource
    for table in p.default_schema.data_tables():
        if table.get("parent") is None:
            assert table["write_disposition"] == "merge"
            assert table["columns"]["id"]["primary_key"] is True

    # load updated event
    info = p.run(
        _updated_event("new_node_X") | github_resource,
        loader_file_format=destination_config.file_format,
    )
    assert_load_info(info)
    # still 101
    counts = load_table_counts(
        p, *[t["name"] for t in p.default_schema.data_tables() if t.get("parent") is None]
    )
    assert sum(counts.values()) == 101 if destination_config.supports_merge else 102
    # for non merge destinations we just check that the run passes
    if not destination_config.supports_merge:
        return
    # but we have it updated
    with p.sql_client() as c:
        qual_name = c.make_qualified_table_name("watch_event")
        with c.execute_query(f"SELECT node_id FROM {qual_name} WHERE node_id = 'new_node_X'") as q:
            assert len(list(q.fetchall())) == 1


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
def test_deduplicate_single_load(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("abstract", dev_mode=True)

    @dlt.resource(write_disposition="merge", primary_key="id")
    def duplicates():
        yield [
            {"id": 1, "name": "row1", "child": [1, 2, 3]},
            {"id": 1, "name": "row2", "child": [4, 5, 6]},
        ]

    info = p.run(duplicates(), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    counts = load_table_counts(p, "duplicates", "duplicates__child")
    assert counts["duplicates"] == 1 if destination_config.supports_merge else 2
    assert counts["duplicates__child"] == 3 if destination_config.supports_merge else 6
    qual_name = p.sql_client().make_qualified_table_name("duplicates")
    select_data(p, f"SELECT * FROM {qual_name}")[0]

    @dlt.resource(write_disposition="merge", primary_key=("id", "subkey"))
    def duplicates_no_child():
        yield [{"id": 1, "subkey": "AX", "name": "row1"}, {"id": 1, "subkey": "AX", "name": "row2"}]

    info = p.run(duplicates_no_child(), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    counts = load_table_counts(p, "duplicates_no_child")
    assert counts["duplicates_no_child"] == 1 if destination_config.supports_merge else 2


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
def test_no_deduplicate_only_merge_key(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("abstract", dev_mode=True)

    @dlt.resource(write_disposition="merge", merge_key="id")
    def duplicates():
        yield [
            {"id": 1, "name": "row1", "child": [1, 2, 3]},
            {"id": 1, "name": "row2", "child": [4, 5, 6]},
        ]

    info = p.run(duplicates(), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    counts = load_table_counts(p, "duplicates", "duplicates__child")
    assert counts["duplicates"] == 2
    assert counts["duplicates__child"] == 6

    @dlt.resource(write_disposition="merge", merge_key=("id", "subkey"))
    def duplicates_no_child():
        yield [{"id": 1, "subkey": "AX", "name": "row1"}, {"id": 1, "subkey": "AX", "name": "row2"}]

    info = p.run(duplicates_no_child(), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    counts = load_table_counts(p, "duplicates_no_child")
    assert counts["duplicates_no_child"] == 2


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
def test_complex_column_missing(destination_config: DestinationTestConfiguration) -> None:
    table_name = "test_complex_column_missing"

    @dlt.resource(name=table_name, write_disposition="merge", primary_key="id")
    def r(data):
        yield data

    p = destination_config.setup_pipeline("abstract", dev_mode=True)

    data = [{"id": 1, "simple": "foo", "complex": [1, 2, 3]}]
    info = p.run(r(data), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 1
    assert load_table_counts(p, table_name + "__complex")[table_name + "__complex"] == 3

    # complex column is missing, previously inserted records should be deleted from child table
    data = [{"id": 1, "simple": "bar"}]
    info = p.run(r(data), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 1
    assert load_table_counts(p, table_name + "__complex")[table_name + "__complex"] == 0


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("key_type", ["primary_key", "merge_key", "no_key"])
def test_hard_delete_hint(destination_config: DestinationTestConfiguration, key_type: str) -> None:
    # no_key setting will have the effect that hard deletes have no effect, since hard delete records
    # can not be matched
    table_name = "test_hard_delete_hint"

    @dlt.resource(
        name=table_name,
        write_disposition="merge",
        columns={"deleted": {"hard_delete": True}},
    )
    def data_resource(data):
        yield data

    if key_type == "primary_key":
        data_resource.apply_hints(primary_key="id", merge_key="")
    elif key_type == "merge_key":
        data_resource.apply_hints(primary_key="", merge_key="id")
    elif key_type == "no_key":
        # we test what happens if there are no merge keys
        pass

    p = destination_config.setup_pipeline(f"abstract_{key_type}", dev_mode=True)

    # insert two records
    data = [
        {"id": 1, "val": "foo", "deleted": False},
        {"id": 2, "val": "bar", "deleted": False},
    ]
    info = p.run(data_resource(data), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 2

    # delete one record
    data = [
        {"id": 1, "deleted": True},
    ]
    info = p.run(data_resource(data), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == (1 if key_type != "no_key" else 2)

    # update one record (None for hard_delete column is treated as "not True")
    data = [
        {"id": 2, "val": "baz", "deleted": None},
    ]
    info = p.run(data_resource(data), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == (1 if key_type != "no_key" else 3)

    # compare observed records with expected records
    if key_type != "no_key":
        qual_name = p.sql_client().make_qualified_table_name(table_name)
        observed = [
            {"id": row[0], "val": row[1], "deleted": row[2]}
            for row in select_data(p, f"SELECT id, val, deleted FROM {qual_name}")
        ]
        expected = [{"id": 2, "val": "baz", "deleted": None}]
        assert sorted(observed, key=lambda d: d["id"]) == expected

    # insert two records with same key
    data = [
        {"id": 3, "val": "foo", "deleted": False},
        {"id": 3, "val": "bar", "deleted": False},
    ]
    info = p.run(data_resource(data), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    counts = load_table_counts(p, table_name)[table_name]
    if key_type == "primary_key":
        assert counts == 2
    elif key_type == "merge_key":
        assert counts == 3
    elif key_type == "no_key":
        assert counts == 5

    # we do not need to test "no_key" further
    if key_type == "no_key":
        return

    # delete one key, resulting in one (primary key) or two (merge key) deleted records
    data = [
        {"id": 3, "val": "foo", "deleted": True},
    ]
    info = p.run(data_resource(data), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    counts = load_table_counts(p, table_name)[table_name]
    assert load_table_counts(p, table_name)[table_name] == 1

    table_name = "test_hard_delete_hint_complex"
    data_resource.apply_hints(table_name=table_name)

    # insert two records with childs and grandchilds
    data = [
        {
            "id": 1,
            "child_1": ["foo", "bar"],
            "child_2": [
                {"grandchild_1": ["foo", "bar"], "grandchild_2": True},
                {"grandchild_1": ["bar", "baz"], "grandchild_2": False},
            ],
            "deleted": False,
        },
        {
            "id": 2,
            "child_1": ["baz"],
            "child_2": [{"grandchild_1": ["baz"], "grandchild_2": True}],
            "deleted": False,
        },
    ]
    info = p.run(data_resource(data), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 2
    assert load_table_counts(p, table_name + "__child_1")[table_name + "__child_1"] == 3
    assert load_table_counts(p, table_name + "__child_2")[table_name + "__child_2"] == 3
    assert (
        load_table_counts(p, table_name + "__child_2__grandchild_1")[
            table_name + "__child_2__grandchild_1"
        ]
        == 5
    )

    # delete first record
    data = [
        {"id": 1, "deleted": True},
    ]
    info = p.run(data_resource(data), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 1
    assert load_table_counts(p, table_name + "__child_1")[table_name + "__child_1"] == 1
    assert (
        load_table_counts(p, table_name + "__child_2__grandchild_1")[
            table_name + "__child_2__grandchild_1"
        ]
        == 1
    )

    # delete second record
    data = [
        {"id": 2, "deleted": True},
    ]
    info = p.run(data_resource(data), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 0
    assert load_table_counts(p, table_name + "__child_1")[table_name + "__child_1"] == 0
    assert (
        load_table_counts(p, table_name + "__child_2__grandchild_1")[
            table_name + "__child_2__grandchild_1"
        ]
        == 0
    )


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
def test_hard_delete_hint_config(destination_config: DestinationTestConfiguration) -> None:
    table_name = "test_hard_delete_hint_non_bool"

    @dlt.resource(
        name=table_name,
        write_disposition="merge",
        primary_key="id",
        columns={
            "deleted_timestamp": {"data_type": "timestamp", "nullable": True, "hard_delete": True}
        },
    )
    def data_resource(data):
        yield data

    p = destination_config.setup_pipeline("abstract", dev_mode=True)

    # insert two records
    data = [
        {"id": 1, "val": "foo", "deleted_timestamp": None},
        {"id": 2, "val": "bar", "deleted_timestamp": None},
    ]
    info = p.run(data_resource(data), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 2

    # delete one record
    data = [
        {"id": 1, "deleted_timestamp": "2024-02-15T17:16:53Z"},
    ]
    info = p.run(data_resource(data), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 1

    # compare observed records with expected records
    qual_name = p.sql_client().make_qualified_table_name(table_name)
    observed = [
        {"id": row[0], "val": row[1], "deleted_timestamp": row[2]}
        for row in select_data(p, f"SELECT id, val, deleted_timestamp FROM {qual_name}")
    ]
    expected = [{"id": 2, "val": "bar", "deleted_timestamp": None}]
    assert sorted(observed, key=lambda d: d["id"]) == expected

    # test if exception is raised when more than one "hard_delete" column hints are provided
    @dlt.resource(
        name="test_hard_delete_hint_too_many_hints",
        write_disposition="merge",
        columns={"deleted_1": {"hard_delete": True}, "deleted_2": {"hard_delete": True}},
    )
    def r():
        yield {"id": 1, "val": "foo", "deleted_1": True, "deleted_2": False}

    with pytest.raises(PipelineStepFailed):
        info = p.run(r(), loader_file_format=destination_config.file_format)


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
def test_dedup_sort_hint(destination_config: DestinationTestConfiguration) -> None:
    table_name = "test_dedup_sort_hint"

    @dlt.resource(
        name=table_name,
        write_disposition="merge",
        primary_key="id",  # sort hints only have effect when a primary key is provided
        columns={
            "sequence": {"dedup_sort": "desc", "nullable": False},
            "val": {"dedup_sort": None},
        },
    )
    def data_resource(data):
        yield data

    p = destination_config.setup_pipeline("abstract", dev_mode=True)

    # three records with same primary key
    data = [
        {"id": 1, "val": "foo", "sequence": 1},
        {"id": 1, "val": "baz", "sequence": 3},
        {"id": 1, "val": "bar", "sequence": 2},
    ]
    info = p.run(data_resource(data), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 1

    # compare observed records with expected records
    # record with highest value in sort column is inserted (because "desc")
    qual_name = p.sql_client().make_qualified_table_name(table_name)
    observed = [
        {"id": row[0], "val": row[1], "sequence": row[2]}
        for row in select_data(p, f"SELECT id, val, sequence FROM {qual_name}")
    ]
    expected = [{"id": 1, "val": "baz", "sequence": 3}]
    assert sorted(observed, key=lambda d: d["id"]) == expected

    # now test "asc" sorting
    data_resource.apply_hints(columns={"sequence": {"dedup_sort": "asc", "nullable": False}})

    info = p.run(data_resource(data), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 1

    # compare observed records with expected records
    # record with highest lowest in sort column is inserted (because "asc")
    qual_name = p.sql_client().make_qualified_table_name(table_name)
    observed = [
        {"id": row[0], "val": row[1], "sequence": row[2]}
        for row in select_data(p, f"SELECT id, val, sequence FROM {qual_name}")
    ]
    expected = [{"id": 1, "val": "foo", "sequence": 1}]
    assert sorted(observed, key=lambda d: d["id"]) == expected

    table_name = "test_dedup_sort_hint_complex"
    data_resource.apply_hints(
        table_name=table_name,
        columns={"sequence": {"dedup_sort": "desc", "nullable": False}},
    )

    # three records with same primary key
    # only record with highest value in sort column is inserted
    data = [
        {"id": 1, "val": [1, 2, 3], "sequence": 1},
        {"id": 1, "val": [7, 8, 9], "sequence": 3},
        {"id": 1, "val": [4, 5, 6], "sequence": 2},
    ]
    info = p.run(data_resource(data), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 1
    assert load_table_counts(p, table_name + "__val")[table_name + "__val"] == 3

    # compare observed records with expected records, now for child table
    qual_name = p.sql_client().make_qualified_table_name(table_name + "__val")
    value_quoted = p.sql_client().escape_column_name("value")
    observed = [row[0] for row in select_data(p, f"SELECT {value_quoted} FROM {qual_name}")]
    assert sorted(observed) == [7, 8, 9]  # type: ignore[type-var]

    table_name = "test_dedup_sort_hint_with_hard_delete"
    data_resource.apply_hints(
        table_name=table_name,
        columns={
            "sequence": {"dedup_sort": "desc", "nullable": False},
            "deleted": {"hard_delete": True},
        },
    )

    # three records with same primary key
    # record with highest value in sort column is a delete, so no record will be inserted
    data = [
        {"id": 1, "val": "foo", "sequence": 1, "deleted": False},
        {"id": 1, "val": "baz", "sequence": 3, "deleted": True},
        {"id": 1, "val": "bar", "sequence": 2, "deleted": False},
    ]
    info = p.run(data_resource(data), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 0

    # three records with same primary key
    # record with highest value in sort column is not a delete, so it will be inserted
    data = [
        {"id": 1, "val": "foo", "sequence": 1, "deleted": False},
        {"id": 1, "val": "bar", "sequence": 2, "deleted": True},
        {"id": 1, "val": "baz", "sequence": 3, "deleted": False},
    ]
    info = p.run(data_resource(data), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 1

    # compare observed records with expected records
    qual_name = p.sql_client().make_qualified_table_name(table_name)
    observed = [
        {"id": row[0], "val": row[1], "sequence": row[2]}
        for row in select_data(p, f"SELECT id, val, sequence FROM {qual_name}")
    ]
    expected = [{"id": 1, "val": "baz", "sequence": 3}]
    assert sorted(observed, key=lambda d: d["id"]) == expected

    # additional tests with two records, run only on duckdb to limit test load
    if destination_config.destination == "duckdb":
        # two records with same primary key
        # record with highest value in sort column is a delete
        # existing record is deleted and no record will be inserted
        data = [
            {"id": 1, "val": "foo", "sequence": 1},
            {"id": 1, "val": "bar", "sequence": 2, "deleted": True},
        ]
        info = p.run(data_resource(data), loader_file_format=destination_config.file_format)
        assert_load_info(info)
        assert load_table_counts(p, table_name)[table_name] == 0

        # two records with same primary key
        # record with highest value in sort column is not a delete, so it will be inserted
        data = [
            {"id": 1, "val": "foo", "sequence": 2},
            {"id": 1, "val": "bar", "sequence": 1, "deleted": True},
        ]
        info = p.run(data_resource(data), loader_file_format=destination_config.file_format)
        assert_load_info(info)
        assert load_table_counts(p, table_name)[table_name] == 1

    # test if exception is raised for invalid column schema's
    @dlt.resource(
        name="test_dedup_sort_hint_too_many_hints",
        write_disposition="merge",
        columns={"dedup_sort_1": {"dedup_sort": "this_is_invalid"}},  # type: ignore[call-overload]
    )
    def r():
        yield {"id": 1, "val": "foo", "dedup_sort_1": 1, "dedup_sort_2": 5}

    # invalid value for "dedup_sort" hint
    with pytest.raises(PipelineStepFailed):
        info = p.run(r(), loader_file_format=destination_config.file_format)

    # more than one "dedup_sort" column hints are provided
    r.apply_hints(
        columns={"dedup_sort_1": {"dedup_sort": "desc"}, "dedup_sort_2": {"dedup_sort": "desc"}}
    )
    with pytest.raises(PipelineStepFailed):
        info = p.run(r(), loader_file_format=destination_config.file_format)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_invalid_merge_strategy(destination_config: DestinationTestConfiguration) -> None:
    @dlt.resource(write_disposition={"disposition": "merge", "strategy": "foo"})  # type: ignore[call-overload]
    def r():
        yield {"foo": "bar"}

    p = destination_config.setup_pipeline("abstract", dev_mode=True)
    with pytest.raises(PipelineStepFailed) as pip_ex:
        p.run(r())
    assert isinstance(pip_ex.value.__context__, SchemaException)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_missing_merge_key_column(destination_config: DestinationTestConfiguration) -> None:
    """Merge key is not present in data, error is raised"""

    @dlt.resource(merge_key="not_a_column", write_disposition={"disposition": "merge"})
    def merging_test_table():
        yield {"foo": "bar"}

    p = destination_config.setup_pipeline("abstract", full_refresh=True)
    with pytest.raises(PipelineStepFailed) as pip_ex:
        p.run(merging_test_table())

    ex = pip_ex.value
    assert ex.step == "normalize"
    assert isinstance(ex.__context__, UnboundColumnException)

    assert "not_a_column" in str(ex)
    assert "merge key" in str(ex)
    assert "merging_test_table" in str(ex)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_merge_key_null_values(destination_config: DestinationTestConfiguration) -> None:
    """Merge key is present in data, but some rows have null values"""

    @dlt.resource(merge_key="id", write_disposition={"disposition": "merge"})
    def r():
        yield [{"id": 1}, {"id": None}, {"id": 2}]

    p = destination_config.setup_pipeline("abstract", full_refresh=True)
    with pytest.raises(PipelineStepFailed) as pip_ex:
        p.run(r())

    ex = pip_ex.value
    assert ex.step == "normalize"

    assert isinstance(ex.__context__, NormalizeJobFailed)
    assert isinstance(ex.__context__.__context__, CannotCoerceNullException)
