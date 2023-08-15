from copy import copy
import pytest
import itertools
import random
from typing import List
import pytest
import yaml

import dlt

from dlt.common import json, pendulum
from dlt.common.configuration.container import Container
from dlt.common.pipeline import StateInjectableContext
from dlt.common.typing import AnyFun, StrAny
from dlt.common.utils import digest128
from dlt.extract.source import DltResource
from dlt.sources.helpers.transform import skip_first, take_first

from tests.pipeline.utils import assert_load_info
from tests.load.pipeline.utils import load_table_counts, select_data
from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration

# uncomment add motherduck tests
# NOTE: the tests are passing but we disable them due to frequent ATTACH DATABASE timeouts
# ALL_DESTINATIONS += ["motherduck"]


@pytest.mark.parametrize("destination_config", destinations_configs(default_configs=True), ids=lambda x: x.name)
def test_merge_on_keys_in_schema(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("eth_2", full_refresh=True)

    with open("tests/common/cases/schemas/eth/ethereum_schema_v5.yml", "r", encoding="utf-8") as f:
        schema = dlt.Schema.from_dict(yaml.safe_load(f))

    with open("tests/normalize/cases/ethereum.blocks.9c1d9b504ea240a482b007788d5cd61c_2.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    # take only the first block. the first block does not have uncles so this table should not be created and merged
    info = p.run(data[:1], table_name="blocks", write_disposition="merge", schema=schema)
    assert_load_info(info)
    eth_1_counts = load_table_counts(p, "blocks")
    # we load a single block
    assert eth_1_counts["blocks"] == 1
    # check root key propagation
    assert p.default_schema.tables["blocks__transactions"]["columns"]["_dlt_root_id"]["root_key"] is True
    # now we load the whole dataset. blocks should be created which adds columns to blocks
    # if the table would be created before the whole load would fail because new columns have hints
    info = p.run(data, table_name="blocks", write_disposition="merge", schema=schema)
    eth_2_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    # we have 2 blocks in dataset
    assert eth_2_counts["blocks"] == 2 if destination_config.supports_merge else 3
    # make sure we have same record after merging full dataset again
    info = p.run(data, table_name="blocks", write_disposition="merge", schema=schema)
    assert_load_info(info)
    # for non merge destinations we just check that the run passes
    if not destination_config.supports_merge:
        return
    eth_3_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    assert eth_2_counts == eth_3_counts


@pytest.mark.parametrize("destination_config", destinations_configs(default_configs=True), ids=lambda x: x.name)
def test_merge_on_ad_hoc_primary_key(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("github_1", full_refresh=True)

    with open("tests/normalize/cases/github.issues.load_page_5_duck.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    # note: NodeId will be normalized to "node_id" which exists in the schema
    info = p.run(data[:17], table_name="issues", write_disposition="merge", primary_key="NodeId")
    assert_load_info(info)
    github_1_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    # 17 issues
    assert github_1_counts["issues"] == 17
    # primary key set on issues
    assert p.default_schema.tables["issues"]["columns"]["node_id"]["primary_key"] is True
    assert p.default_schema.tables["issues"]["columns"]["node_id"]["data_type"] == "text"
    assert p.default_schema.tables["issues"]["columns"]["node_id"]["nullable"] is False

    info = p.run(data[5:], table_name="issues", write_disposition="merge", primary_key="node_id")
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

    @dlt.resource(table_name="issues", write_disposition="merge", primary_key="id", merge_key=("node_id", "url"))
    def load_issues():
        with open("tests/normalize/cases/github.issues.load_page_5_duck.json", "r", encoding="utf-8") as f:
            yield from json.load(f)

    return load_issues


@pytest.mark.parametrize("destination_config", destinations_configs(default_configs=True), ids=lambda x: x.name)
def test_merge_source_compound_keys_and_changes(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("github_3", full_refresh=True)

    info = p.run(github())
    assert_load_info(info)
    github_1_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    # 100 issues total
    assert github_1_counts["issues"] == 100
    # check keys created
    assert p.default_schema.tables["issues"]["columns"]["node_id"].items() > {"merge_key": True, "data_type": "text", "nullable": False}.items()
    assert p.default_schema.tables["issues"]["columns"]["url"].items() > {"merge_key": True, "data_type": "text", "nullable": False}.items()
    assert p.default_schema.tables["issues"]["columns"]["id"].items() > {"primary_key": True, "data_type": "bigint", "nullable": False}.items()

    # append load_issues resource
    info = p.run(github().load_issues, write_disposition="append")
    assert_load_info(info)
    assert p.default_schema.tables["issues"]["write_disposition"] == "append"
    # the counts of all tables must be double
    github_2_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    assert {k:v*2 for k, v in github_1_counts.items()} == github_2_counts

    # now replace all resources
    info = p.run(github(), write_disposition="replace" )
    assert_load_info(info)
    assert p.default_schema.tables["issues"]["write_disposition"] == "replace"
    # assert p.default_schema.tables["issues__labels"]["write_disposition"] == "replace"
    # the counts of all tables must be double
    github_3_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    assert github_1_counts == github_3_counts


@pytest.mark.parametrize("destination_config", destinations_configs(default_configs=True), ids=lambda x: x.name)
def test_merge_no_child_tables(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("github_3", full_refresh=True)
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
    info = p.run(github_data)
    assert len(p.default_schema.data_tables()) == 1
    assert "issues" in p.default_schema.tables
    assert_load_info(info)
    github_1_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    assert github_1_counts["issues"] == 15

    # load all
    github_data = github()
    github_data.max_table_nesting = 0
    info = p.run(github_data)
    assert_load_info(info)
    github_2_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    # 100 issues total, or 115 if merge is not supported
    assert github_2_counts["issues"] == 100 if destination_config.supports_merge else 115


@pytest.mark.parametrize("destination_config", destinations_configs(default_configs=True), ids=lambda x: x.name)
def test_merge_no_merge_keys(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("github_3", full_refresh=True)
    github_data = github()
    # remove all keys
    github_data.load_issues.apply_hints(merge_key=(), primary_key=())
    # skip first 45 rows
    github_data.load_issues.add_filter(skip_first(45))
    info = p.run(github_data)
    assert_load_info(info)
    github_1_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    assert github_1_counts["issues"] == 100 - 45

    # take first 10 rows.
    github_data = github()
    # remove all keys
    github_data.load_issues.apply_hints(merge_key=(), primary_key=())
    # skip first 45 rows
    github_data.load_issues.add_filter(take_first(10))
    info = p.run(github_data)
    assert_load_info(info)
    github_1_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    # only ten rows remains. merge falls back to replace when no keys are specified
    assert github_1_counts["issues"] == 10 if destination_config.supports_merge else 100 - 45


@pytest.mark.parametrize("destination_config", destinations_configs(default_configs=True), ids=lambda x: x.name)
def test_merge_keys_non_existing_columns(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("github_3", full_refresh=True)
    github_data = github()
    # set keys names that do not exist in the data
    github_data.load_issues.apply_hints(merge_key=("mA1", "Ma2"), primary_key=("123-x", ))
    # skip first 45 rows
    github_data.load_issues.add_filter(skip_first(45))
    info = p.run(github_data)
    assert_load_info(info)
    github_1_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    assert github_1_counts["issues"] == 100 - 45
    assert p.default_schema.tables["issues"]["columns"]["m_a1"].items() > {"merge_key": True, "nullable": False}.items()

    # for non merge destinations we just check that the run passes
    if not destination_config.supports_merge:
        return

    # all the keys are invalid so the merge falls back to replace
    github_data = github()
    github_data.load_issues.apply_hints(merge_key=("mA1", "Ma2"), primary_key=("123-x", ))
    github_data.load_issues.add_filter(take_first(1))
    info = p.run(github_data)
    assert_load_info(info)
    github_2_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    assert github_2_counts["issues"] == 1
    with p._sql_job_client(p.default_schema) as job_c:
        _, table_schema = job_c.get_storage_table("issues")
        assert "url" in table_schema
        assert "m_a1" not in table_schema  # unbound columns were not created


@pytest.mark.parametrize("destination_config", destinations_configs(default_configs=True, subset=["duckdb", "snowflake", "bigquery"]), ids=lambda x: x.name)
def test_pipeline_load_parquet(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("github_3", full_refresh=True)
    github_data = github()
    # generate some complex types
    github_data.max_table_nesting = 2
    github_data_copy = github()
    github_data_copy.max_table_nesting = 2
    info = p.run([github_data, github_data_copy], loader_file_format="parquet", write_disposition="merge")
    assert_load_info(info)
    # make sure it was parquet or sql transforms
    files = p.get_load_package_info(p.list_completed_load_packages()[0]).jobs["completed_jobs"]
    assert all(f.job_file_info.file_format in ["parquet", "sql"] for f in files)

    github_1_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    assert github_1_counts["issues"] == 100

    # now retry with replace
    github_data = github()
    # generate some complex types
    github_data.max_table_nesting = 2
    info = p.run(github_data, loader_file_format="parquet", write_disposition="replace")
    assert_load_info(info)
    # make sure it was parquet or sql inserts
    files = p.get_load_package_info(p.list_completed_load_packages()[1]).jobs["completed_jobs"]
    assert all(f.job_file_info.file_format in ["parquet"] for f in files)

    github_1_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables()])
    assert github_1_counts["issues"] == 100



@dlt.transformer(name="github_repo_events", primary_key="id", write_disposition="merge", table_name=lambda i: i['type'])
def github_repo_events(page: List[StrAny], last_created_at = dlt.sources.incremental("created_at", "1970-01-01T00:00:00Z")):
    """A transformer taking a stream of github events and dispatching them to tables named by event type. Deduplicates be 'id'. Loads incrementally by 'created_at' """
    yield page


@dlt.transformer(name="github_repo_events", primary_key="id", write_disposition="merge")
def github_repo_events_table_meta(page: List[StrAny], last_created_at = dlt.sources.incremental("created_at", "1970-01-01T00:00:00Z")):
    """A transformer taking a stream of github events and dispatching them to tables using table meta. Deduplicates be 'id'. Loads incrementally by 'created_at' """
    yield from [dlt.mark.with_table_name(p, p['type']) for p in page]


@dlt.resource
def _get_shuffled_events(shuffle: bool = dlt.secrets.value):
    with open("tests/normalize/cases/github.events.load_page_1_duck.json", "r", encoding="utf-8") as f:
        issues = json.load(f)
        # random order
        if shuffle:
            random.shuffle(issues)
        yield issues



@pytest.mark.parametrize("destination_config", destinations_configs(default_configs=True), ids=lambda x: x.name)
@pytest.mark.parametrize("github_resource",[github_repo_events, github_repo_events_table_meta])
def test_merge_with_dispatch_and_incremental(destination_config: DestinationTestConfiguration, github_resource: DltResource) -> None:

    # for athena we need to skip this test, as athena does not want a query string longer than 262144 (bytes?) and this is the
    # case here
    if destination_config.destination == "athena":
        pytest.skip("athena does not support long queries")

    newest_issues = list(sorted(_get_shuffled_events(True), key = lambda x: x["created_at"], reverse=True))
    newest_issue = newest_issues[0]

    @dlt.resource
    def _new_event(node_id):
        new_i = copy(newest_issue)
        new_i["id"] = str(random.randint(0, 2^32))
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
        assert incremental_state["incremental"]["created_at"]["last_value"] == newest_issue["created_at"]
        assert incremental_state["incremental"]["created_at"]["unique_hashes"] == [digest128(f'"{newest_issue["id"]}"')]
        # subsequent load will skip all elements
        assert len(list(_get_shuffled_events(True) | github_resource)) == 0
        # add one more issue
        assert len(list(_new_event("new_node") | github_resource)) == 1
        assert incremental_state["incremental"]["created_at"]["last_value"] > newest_issue["created_at"]
        assert incremental_state["incremental"]["created_at"]["unique_hashes"] != [digest128(str(newest_issue["id"]))]

    # load to destination
    p = destination_config.setup_pipeline("github_3", full_refresh=True)
    info = p.run(_get_shuffled_events(True) | github_resource)
    assert_load_info(info)
    # get top tables
    counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables() if t.get("parent") is None])
    # total number of events in all top tables == 100
    assert sum(counts.values()) == 100
    # this should skip all events due to incremental load
    info = p.run(_get_shuffled_events(True) | github_resource)
    # no packages were loaded
    assert len(info.loads_ids) == 0

    # load one more event with a new id
    info = p.run(_new_event("new_node") | github_resource)
    assert_load_info(info)
    counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables() if t.get("parent") is None])
    assert sum(counts.values()) == 101
    # all the columns have primary keys and merge disposition derived from resource
    for table in  p.default_schema.data_tables():
        if table.get("parent") is None:
            assert table["write_disposition"] == "merge"
            assert table["columns"]["id"]["primary_key"] is True

    # load updated event
    info = p.run(_updated_event("new_node_X") | github_resource)
    assert_load_info(info)
    # still 101
    counts = load_table_counts(p, *[t["name"] for t in p.default_schema.data_tables() if t.get("parent") is None])
    assert sum(counts.values()) == 101
    # but we have it updated
    with p.sql_client() as c:
        with c.execute_query("SELECT node_id FROM watch_event WHERE node_id = 'new_node_X'") as q:
            assert len(list(q.fetchall())) == 1


@pytest.mark.parametrize("destination_config", destinations_configs(default_configs=True), ids=lambda x: x.name)
def test_deduplicate_single_load(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("abstract", full_refresh=True)

    @dlt.resource(write_disposition="merge", primary_key="id")
    def duplicates():
        yield [{"id": 1, "name": "row1", "child": [1, 2, 3]}, {"id": 1, "name": "row2", "child": [4, 5, 6]}]

    info = p.run(duplicates())
    assert_load_info(info)
    counts = load_table_counts(p, "duplicates", "duplicates__child")
    assert counts["duplicates"] == 1 if destination_config.supports_merge else 2
    assert counts["duplicates__child"] == 3 if destination_config.supports_merge else 6
    select_data(p, "SELECT * FROM duplicates")[0]


    @dlt.resource(write_disposition="merge", primary_key=("id", "subkey"))
    def duplicates_no_child():
        yield [{"id": 1, "subkey": "AX", "name": "row1"}, {"id": 1, "subkey": "AX", "name": "row2"}]

    info = p.run(duplicates_no_child())
    assert_load_info(info)
    counts = load_table_counts(p, "duplicates_no_child")
    assert counts["duplicates_no_child"] == 1 if destination_config.supports_merge else 2


@pytest.mark.parametrize("destination_config", destinations_configs(default_configs=True), ids=lambda x: x.name)
def test_no_deduplicate_only_merge_key(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("abstract", full_refresh=True)

    @dlt.resource(write_disposition="merge", merge_key="id")
    def duplicates():
        yield [{"id": 1, "name": "row1", "child": [1, 2, 3]}, {"id": 1, "name": "row2", "child": [4, 5, 6]}]

    info = p.run(duplicates())
    assert_load_info(info)
    counts = load_table_counts(p, "duplicates", "duplicates__child")
    assert counts["duplicates"] == 2
    assert counts["duplicates__child"] == 6


    @dlt.resource(write_disposition="merge", merge_key=("id", "subkey"))
    def duplicates_no_child():
        yield [{"id": 1, "subkey": "AX", "name": "row1"}, {"id": 1, "subkey": "AX", "name": "row2"}]

    info = p.run(duplicates_no_child())
    assert_load_info(info)
    counts = load_table_counts(p, "duplicates_no_child")
    assert counts["duplicates_no_child"] == 2
