import os
import pytest

import dlt
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.typing import DictStrStr
from dlt.common.utils import uniq_id
from dlt.common.storages import FilesystemConfiguration
from dlt.destinations import duckdb, dummy, filesystem

from tests.utils import TEST_STORAGE_ROOT


def test_default_name_to_type() -> None:
    duck = duckdb(credentials=os.path.join(TEST_STORAGE_ROOT, "quack.duckdb"))
    p = dlt.pipeline(pipeline_name="quack_pipeline", destination=duck)
    load_info = p.run([1, 2, 3], table_name="table", dataset_name="dataset")

    assert p.destination.destination_name == "duckdb"
    assert p.destination.destination_type == "dlt.destinations.duckdb"
    assert load_info.destination_name == "duckdb"
    assert load_info.destination_type == "dlt.destinations.duckdb"
    assert load_info.environment is None


def test_set_name_and_environment() -> None:
    duck = duckdb(
        credentials=os.path.join(TEST_STORAGE_ROOT, "quack.duckdb"),
        destination_name="duck1",
        environment="production",
    )
    p = dlt.pipeline(pipeline_name="quack_pipeline", destination=duck)
    assert (
        p.destination.destination_type == "dlt.destinations.duckdb" == p.state["destination_type"]
    )
    assert p.destination.destination_name == "duck1" == p.state["destination_name"]

    load_info = p.run([1, 2, 3], table_name="table", dataset_name="dataset")
    assert (
        p.destination.destination_type == "dlt.destinations.duckdb" == p.state["destination_type"]
    )
    assert p.destination.destination_name == "duck1" == p.state["destination_name"]

    assert load_info.destination_name == "duck1"
    assert load_info.destination_type == "dlt.destinations.duckdb"
    # TODO: create destination_info and have same information for staging
    assert load_info.environment == "production"
    p.drop()

    rp = dlt.pipeline(pipeline_name="quack_pipeline", destination=duck)
    assert rp.default_schema_name is None
    assert rp.schema_names == []
    rp.sync_destination()
    assert rp.default_schema_name == "quack"
    assert rp.schema_names == ["quack"]


def test_preserve_destination_instance() -> None:
    dummy1 = dummy(destination_name="dummy1", environment="dev/null/1")
    filesystem1 = filesystem(
        FilesystemConfiguration.make_file_url(TEST_STORAGE_ROOT),
        destination_name="local_fs",
        environment="devel",
    )
    p = dlt.pipeline(pipeline_name="dummy_pipeline", destination=dummy1, staging=filesystem1)
    destination_id = id(p.destination)
    staging_id = id(p.staging)
    import os

    os.environ["COMPLETED_PROB"] = "1.0"
    load_info = p.run([1, 2, 3], table_name="table", dataset_name="dataset")
    # destination and staging stay the same
    assert destination_id == id(p.destination)
    assert staging_id == id(p.staging)
    # all names and types correctly set
    assert (
        p.destination.destination_name
        == "dummy1"
        == p.state["destination_name"]
        == load_info.destination_name
    )
    assert (
        p.destination.destination_type
        == "dlt.destinations.dummy"
        == p.state["destination_type"]
        == load_info.destination_type
    )
    assert p.destination.config_params["environment"] == "dev/null/1" == load_info.environment
    assert (
        p.staging.destination_name
        == "local_fs"
        == p.state["staging_name"]
        == load_info.staging_name
    )
    assert (
        p.staging.destination_type
        == "dlt.destinations.filesystem"
        == p.state["staging_type"]
        == load_info.staging_type
    )
    assert p.staging.config_params["environment"] == "devel"

    # attach pipeline
    p = dlt.attach(pipeline_name="dummy_pipeline")
    assert p.destination.destination_name == "dummy1" == p.state["destination_name"]
    assert p.destination.destination_type == "dlt.destinations.dummy" == p.state["destination_type"]
    assert p.staging.destination_name == "local_fs" == p.state["staging_name"]
    assert p.staging.destination_type == "dlt.destinations.filesystem" == p.state["staging_type"]

    # config args should not contain self
    assert "self" not in p.destination.config_params

    # this information was lost and is not present in the config/secrets when pipeline is restored
    assert "environment" not in p.destination.config_params
    assert "environment" not in p.staging.config_params
    # for that reason dest client cannot be instantiated
    with pytest.raises(ConfigFieldMissingException):
        p.destination_client()
    assert p.default_schema_name == "dummy"
    assert p.schema_names == ["dummy"]

    # create new pipeline with the same name but different destination
    p = dlt.pipeline(pipeline_name="dummy_pipeline", destination="duckdb")
    assert p.destination.destination_name == "duckdb" == p.state["destination_name"]


def test_config_respects_dataset_name(environment: DictStrStr) -> None:
    environment["DESTINATION__ENVIRONMENT"] = "devel"
    environment["QUACK_PIPELINE_DEVEL__DATASET_NAME"] = "devel_dataset"

    environment["DESTINATION__DUCK1__ENVIRONMENT"] = "staging"
    environment["QUACK_PIPELINE_STAGING__DATASET_NAME"] = "staging_dataset"

    environment["DESTINATION__DUCK2__ENVIRONMENT"] = "production"
    environment["QUACK_PIPELINE_PRODUCTION__DATASET_NAME"] = "production_dataset"

    # default will pick from global destination settings
    duck = duckdb(credentials=os.path.join(TEST_STORAGE_ROOT, "quack.duckdb"))
    p = dlt.pipeline(pipeline_name="quack_pipeline_devel", destination=duck)
    load_info = p.run([1, 2, 3], table_name="table")
    with p.destination_client() as client:
        assert client.config.environment == "devel"
        assert client.config.dataset_name == "devel_dataset"  # type: ignore
    assert load_info.environment == "devel"

    # duck1 will be staging
    duck = duckdb(
        credentials=os.path.join(TEST_STORAGE_ROOT, "quack.duckdb"), destination_name="duck1"
    )
    p = dlt.pipeline(pipeline_name="quack_pipeline_staging", destination=duck)
    load_info = p.run([1, 2, 3], table_name="table")
    with p.destination_client() as client:
        assert client.config.environment == "staging"
        assert client.config.dataset_name == "staging_dataset"  # type: ignore
    assert load_info.environment == "staging"

    # duck2 will be production
    duck = duckdb(
        credentials=os.path.join(TEST_STORAGE_ROOT, "quack.duckdb"), destination_name="duck2"
    )
    p = dlt.pipeline(pipeline_name="quack_pipeline_production", destination=duck)
    load_info = p.run([1, 2, 3], table_name="table")
    with p.destination_client() as client:
        assert client.config.environment == "production"
        assert client.config.dataset_name == "production_dataset"  # type: ignore
    assert load_info.environment == "production"


def test_pipeline_config(environment: DictStrStr) -> None:
    environment["DESTINATION_TYPE"] = "redshift"
    p = dlt.pipeline(pipeline_name="p_" + uniq_id())
    assert p.config.destination_type == "redshift"
    assert p.destination.destination_name == "redshift"
    assert p.destination.destination_type == "dlt.destinations.redshift"
    assert p.staging is None

    del environment["DESTINATION_TYPE"]
    environment["DESTINATION_NAME"] = "duckdb"
    p = dlt.pipeline(pipeline_name="p_" + uniq_id())
    assert p.destination.destination_name == "duckdb"
    assert p.destination.destination_type == "dlt.destinations.duckdb"
    assert p.staging is None

    environment["DESTINATION_TYPE"] = "bigquery"
    environment["DESTINATION_NAME"] = "my_dest"
    p = dlt.pipeline(pipeline_name="p_" + uniq_id())
    assert p.destination.destination_name == "my_dest"
    assert p.destination.destination_type == "dlt.destinations.bigquery"
    assert p.staging is None

    environment["STAGING_TYPE"] = "filesystem"
    environment["STAGING_NAME"] = "my_staging"
    p = dlt.pipeline(pipeline_name="p_" + uniq_id())
    assert p.destination.destination_name == "my_dest"
    assert p.destination.destination_type == "dlt.destinations.bigquery"
    assert p.staging.destination_type == "dlt.destinations.filesystem"
    assert p.staging.destination_name == "my_staging"


def test_destination_config_in_name(environment: DictStrStr) -> None:
    environment["DESTINATION_TYPE"] = "filesystem"
    environment["DESTINATION_NAME"] = "filesystem-prod"

    p = dlt.pipeline(pipeline_name="p_" + uniq_id())

    # we do not have config for postgres-prod so getting destination client must fail
    with pytest.raises(ConfigFieldMissingException):
        p.destination_client()

    environment["DESTINATION__FILESYSTEM-PROD__BUCKET_URL"] = FilesystemConfiguration.make_file_url(
        "_storage"
    )
    assert p._fs_client().dataset_path.endswith(p.dataset_name)
