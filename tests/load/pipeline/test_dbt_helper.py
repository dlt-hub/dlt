import os
from typing import Iterator
import pytest
import tempfile

import dlt
from dlt.common.runners import Venv
from dlt.common.schema.schema import Schema
from dlt.common.utils import uniq_id
from dlt.helpers.dbt import create_venv
from dlt.helpers.dbt.exceptions import DBTProcessingError, PrerequisitesException

from tests.pipeline.utils import select_data
from tests.load.utils import destinations_configs, DestinationTestConfiguration
from tests.utils import ACTIVE_SQL_DESTINATIONS

# uncomment add motherduck tests
# NOTE: the tests are passing but we disable them due to frequent ATTACH DATABASE timeouts
# ACTIVE_DESTINATIONS += ["motherduck"]


@pytest.fixture(scope="module")
def dbt_venv() -> Iterator[Venv]:
    # context manager will delete venv at the end
    # yield Venv.restore_current()
    # NOTE: we limit the max version of dbt to allow all dbt adapters to run. ie. sqlserver does not work on 1.8
    # TODO: pytest marking below must be fixed
    dbt_configs = set(
        c.values[0].destination_type  # type: ignore[attr-defined]
        for c in destinations_configs(default_sql_configs=True, supports_dbt=True)
    )
    with create_venv(tempfile.mkdtemp(), list(dbt_configs), dbt_version="<1.8") as venv:
        yield venv


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_dbt=True),
    ids=lambda x: x.name,
)
def test_run_jaffle_package(
    destination_config: DestinationTestConfiguration, dbt_venv: Venv
) -> None:
    if destination_config.destination_type == "athena":
        pytest.skip(
            "dbt-athena requires database to be created and we don't do it in case of Jaffle"
        )
    pipeline = destination_config.setup_pipeline("jaffle_jaffle", dev_mode=True)
    # get runner, pass the env from fixture
    dbt = dlt.dbt.package(pipeline, "https://github.com/dbt-labs/jaffle_shop.git", venv=dbt_venv)
    # no default schema
    assert pipeline.default_schema_name is None
    # inject default schema otherwise dataset is not deleted
    pipeline._inject_schema(Schema("default"))
    # run the package (clone/pull repo, deps, seed, source tests, run)
    models = dbt.run_all()
    # all models were executed
    assert len(models) == 5
    assert all(r.status == "success" for r in models)
    # test the models
    tests = dbt.test()
    assert len(tests) == 20
    assert all(r.status == "pass" for r in tests)

    # get and display dataframe with customers
    qual_name = pipeline.sql_client().make_qualified_table_name
    customers = select_data(pipeline, f"SELECT * FROM {qual_name('customers')}")
    assert len(customers) == 100
    orders = select_data(pipeline, f"SELECT * FROM {qual_name('orders')}")
    assert len(orders) == 99


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_dbt=True),
    ids=lambda x: x.name,
)
def test_run_chess_dbt(destination_config: DestinationTestConfiguration, dbt_venv: Venv) -> None:
    if destination_config.destination_type == "mssql":
        pytest.skip(
            "mssql requires non standard SQL syntax and we do not have specialized dbt package"
            " for it"
        )

    from docs.examples.chess.chess import chess

    # provide chess url via environ
    os.environ["CHESS_URL"] = "https://api.chess.com/pub/"

    pipeline = destination_config.setup_pipeline(
        "chess_games", dataset_name="chess_dbt_test", dev_mode=True
    )
    assert pipeline.default_schema_name is None
    # get the runner for the "dbt_transform" package
    transforms = dlt.dbt.package(pipeline, "docs/examples/chess/dbt_transform", venv=dbt_venv)
    assert pipeline.default_schema_name is None
    # there's no data so the source tests will fail
    with pytest.raises(PrerequisitesException):
        transforms.run_all(source_tests_selector="source:*")
    # load data
    info = pipeline.run(chess(max_players=5, month=9), **destination_config.run_kwargs)
    print(info)
    assert pipeline.schema_names == ["chess"]
    # run all the steps (deps -> seed -> source tests -> run)
    # request all the source tests
    transforms.run_all(source_tests_selector="source:*")
    # run all the tests
    transforms.test()
    load_ids = select_data(
        pipeline, "SELECT load_id, schema_name, status FROM _dlt_loads ORDER BY status"
    )
    assert len(load_ids) == 2
    view_player_games = select_data(
        pipeline, "SELECT * FROM view_player_games ORDER BY username, uuid"
    )
    assert len(view_player_games) > 0
    # run again
    transforms.run()
    # no new load ids - no new data in view table
    new_load_ids = select_data(
        pipeline, "SELECT load_id, schema_name, status FROM _dlt_loads ORDER BY status"
    )
    new_view_player_games = select_data(
        pipeline, "SELECT * FROM view_player_games ORDER BY username, uuid"
    )
    assert load_ids == new_load_ids
    assert view_player_games == new_view_player_games


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_dbt=True),
    ids=lambda x: x.name,
)
def test_run_chess_dbt_to_other_dataset(
    destination_config: DestinationTestConfiguration, dbt_venv: Venv
) -> None:
    if destination_config.destination_type == "mssql":
        pytest.skip(
            "mssql requires non standard SQL syntax and we do not have specialized dbt package"
            " for it"
        )
    from docs.examples.chess.chess import chess

    # provide chess url via environ
    os.environ["CHESS_URL"] = "https://api.chess.com/pub/"

    pipeline = destination_config.setup_pipeline(
        "chess_games", dataset_name="chess_dbt_test", dev_mode=True
    )
    # load each schema in separate dataset
    pipeline.config.use_single_dataset = False
    # assert pipeline.default_schema_name is None
    # get the runner for the "dbt_transform" package
    transforms = dlt.dbt.package(pipeline, "docs/examples/chess/dbt_transform", venv=dbt_venv)
    # assert pipeline.default_schema_name is None
    # load data
    info = pipeline.run(chess(max_players=5, month=9), **destination_config.run_kwargs)
    print(info)
    assert pipeline.schema_names == ["chess"]
    # store transformations in alternative dataset
    test_suffix = "alt"
    # inject the schema for test_suffix. due to use_single_dataset = False, we are able to create a sql client for it and the dataset
    # will clean up automatically
    pipeline._inject_schema(Schema(test_suffix))
    transforms.run_all(destination_dataset_name=info.dataset_name + "_" + test_suffix)
    # run all the tests on original dataset which will fail
    with pytest.raises(DBTProcessingError):
        transforms.test()
    # run tests on destination dataset where transformations actually are
    transforms.test(destination_dataset_name=info.dataset_name + "_" + test_suffix)
    # get load ids from the source dataset
    load_ids = select_data(
        pipeline, "SELECT load_id, schema_name, status FROM _dlt_loads ORDER BY status"
    )
    assert len(load_ids) == 1
    # status is 0, no more entries
    assert load_ids[0][2] == 0
    # get from destination dataset
    load_ids = select_data(
        pipeline,
        "SELECT load_id, schema_name, status FROM _dlt_loads ORDER BY status",
        schema_name=test_suffix,
    )
    # TODO: the package is not finished, both results should be here
    assert len(load_ids) == 1
    # status is 1, no more entries
    assert load_ids[0][2] == 1
