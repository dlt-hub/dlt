import dlt
import os
from dlt.destinations import duckdb
from dlt.common.utils import uniq_id


def test_default_name_to_type() -> None:
    duck = duckdb(credentials="quack.duckdb")
    p = dlt.pipeline(pipeline_name="quack_pipeline", destination=duck)
    load_info = p.run([1, 2, 3], table_name="table", dataset_name="dataset")

    assert p.destination.destination_name == "duckdb"
    assert p.destination.destination_type == "duckdb"
    assert load_info.destination_name == "duckdb"
    assert load_info.destination_type == "duckdb"
    assert load_info.environment is None


def test_set_name_and_environment() -> None:
    duck = duckdb(credentials="quack.duckdb", name="duck1", environment="production")
    p = dlt.pipeline(pipeline_name="quack_pipeline", destination=duck)
    load_info = p.run([1, 2, 3], table_name="table", dataset_name="dataset")

    assert p.destination.destination_name == "duck1"
    assert p.destination.destination_type == "duckdb"
    assert load_info.destination_name == "duck1"
    assert load_info.destination_type == "duckdb"
    assert load_info.environment == "production"


def test_config_respects_name() -> None:
    os.environ["DESTINATION__ENVIRONMENT"] = "devel"
    os.environ["DESTINATION__DATASET_NAME"] = "devel_dataset"

    os.environ["DESTINATION__DUCK1__ENVIRONMENT"] = "staging"
    os.environ["DESTINATION__DUCK1__DATASET_NAME"] = "staging_dataset"

    os.environ["DESTINATION__DUCK2__ENVIRONMENT"] = "production"
    os.environ["DESTINATION__DUCK2__DATASET_NAME"] = "production_dataset"

    # default will pick from global destination settings
    duck = duckdb(credentials="quack.duckdb")
    p = dlt.pipeline(pipeline_name="quack_pipeline", destination=duck)
    load_info = p.run([1, 2, 3], table_name="table", dataset_name="dataset")
    with p.destination_client() as client:
        assert client.config.environment == "devel"
        assert client.config.dataset_name == "devel_dataset"  # type: ignore
    assert load_info.environment == "devel"

    # duck1 will be staging
    duck = duckdb(credentials="quack.duckdb", name="duck1")
    p = dlt.pipeline(pipeline_name="quack_pipeline", destination=duck)
    load_info = p.run([1, 2, 3], table_name="table", dataset_name="dataset")
    with p.destination_client() as client:
        assert client.config.environment == "staging"
        assert client.config.dataset_name == "staging_dataset"  # type: ignore
    assert load_info.environment == "staging"

    # duck2 will be production
    duck = duckdb(credentials="quack.duckdb", name="duck2")
    p = dlt.pipeline(pipeline_name="quack_pipeline", destination=duck)
    load_info = p.run([1, 2, 3], table_name="table", dataset_name="dataset")
    with p.destination_client() as client:
        assert client.config.environment == "production"
        assert client.config.dataset_name == "production_dataset"  # type: ignore
    assert load_info.environment == "production"


def test_pipeline_config() -> None:
    os.environ["DESTINATION_TYPE"] = "redshift"
    p = dlt.pipeline(pipeline_name=uniq_id())
    assert p.config.destination_type == "redshift"
    assert p.destination.destination_name == "redshift"
    assert p.destination.destination_type == "dlt.destinations.redshift"
    assert p.staging is None

    del os.environ["DESTINATION_TYPE"]
    os.environ["DESTINATION_NAME"] = "duckdb"
    p = dlt.pipeline(pipeline_name=uniq_id())
    assert p.destination.destination_name == "duckdb"
    assert p.destination.destination_type == "dlt.destinations.duckdb"
    assert p.staging is None

    os.environ["DESTINATION_TYPE"] = "bigquery"
    os.environ["DESTINATION_NAME"] = "my_dest"
    p = dlt.pipeline(pipeline_name=uniq_id())
    assert p.destination.destination_name == "my_dest"
    assert p.destination.destination_type == "dlt.destinations.bigquery"
    assert p.staging is None

    os.environ["STAGING_TYPE"] = "filesystem"
    os.environ["STAGING_NAME"] = "my_staging"
    p = dlt.pipeline(pipeline_name=uniq_id())
    assert p.destination.destination_name == "my_dest"
    assert p.destination.destination_type == "dlt.destinations.bigquery"
    assert p.staging.destination_type == "dlt.destinations.filesystem"
    assert p.staging.destination_name == "my_staging"
