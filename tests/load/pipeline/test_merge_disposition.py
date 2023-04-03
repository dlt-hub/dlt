import pytest
import yaml

import dlt

from dlt.common import json

from tests.utils import ALL_DESTINATIONS, patch_home_dir, preserve_environ, autouse_test_storage
from tests.pipeline.utils import drop_dataset_from_env
from tests.load.utils import delete_dataset
from tests.load.pipeline.utils import assert_load_info, drop_pipeline, load_table_counts, select_data


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_merge_on_keys_in_schema(destination_name: str) -> None:
    p = dlt.pipeline(destination=destination_name, dataset_name="eth_2", full_refresh=True)

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
    assert p.default_schema.tables["blocks__transactions"]["columns"]["_dlt_root_id"]["root_key"] == True
    # now we load the whole dataset. blocks should be created which adds columns to blocks
    # if the table would be created before the whole load would fail because new columns have hints
    info = p.run(data, table_name="blocks", write_disposition="merge", schema=schema)
    assert_load_info(info)
    eth_2_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.all_tables()])
    # we have 2 blocks in dataset
    assert eth_2_counts["blocks"] == 2
    # make sure we have same record after merging full dataset again
    info = p.run(data, table_name="blocks", write_disposition="merge", schema=schema)
    assert_load_info(info)
    eth_3_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.all_tables()])
    assert eth_2_counts == eth_3_counts


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_merge_on_ad_hoc_primary_key(destination_name: str) -> None:
    p = dlt.pipeline(destination=destination_name, dataset_name="github_1", full_refresh=True)

    with open("tests/normalize/cases/github.issues.load_page_5_duck.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    info = p.run(data[:17], table_name="issues", write_disposition="merge", primary_key="node_id")
    assert_load_info(info)
    github_1_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.all_tables()])
    # 17 issues
    assert github_1_counts["issues"] == 17
    # primary key set on issues
    assert p.default_schema.tables["issues"]["columns"]["node_id"]["primary_key"] == True
    assert p.default_schema.tables["issues"]["columns"]["node_id"]["data_type"] == "text"
    assert p.default_schema.tables["issues"]["columns"]["node_id"]["nullable"] == False

    info = p.run(data, table_name="issues", write_disposition="merge", primary_key="node_id")
    assert_load_info(info)
    github_2_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.all_tables()])
    # 100 issues total
    assert github_2_counts["issues"] == 100
    # still 100 after the reload


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_merge_source_compound_keys_and_changes(destination_name: str) -> None:
    p = dlt.pipeline(destination=destination_name, dataset_name="github_3", full_refresh=True)

    @dlt.source(root_key=True)
    def github():

        @dlt.resource(table_name="issues", write_disposition="merge", primary_key="id", merge_key=("node_id", "url"), encoding="utf-8")
        def load_issues():
            with open("tests/normalize/cases/github.issues.load_page_5_duck.json", "r") as f:
                yield from json.load(f)

        return load_issues

    info = p.run(github())
    assert_load_info(info)
    github_1_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.all_tables()])
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
    github_2_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.all_tables()])
    assert {k:v*2 for k, v in github_1_counts.items()} == github_2_counts

    # now replace all resources
    info = p.run(github(), write_disposition="replace" )
    assert_load_info(info)
    assert p.default_schema.tables["issues"]["write_disposition"] == "replace"
    # assert p.default_schema.tables["issues__labels"]["write_disposition"] == "replace"
    # the counts of all tables must be double
    github_3_counts = load_table_counts(p, *[t["name"] for t in p.default_schema.all_tables()])
    assert github_1_counts == github_3_counts
