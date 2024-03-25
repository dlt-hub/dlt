import pytest
import os

pytest.importorskip("databricks")


from dlt.destinations.impl.databricks.configuration import DatabricksClientConfiguration
from dlt.common.configuration import resolve_configuration
from tests.utils import preserve_environ


def test_databricks_credentials_to_connector_params():
    os.environ["CREDENTIALS__SERVER_HOSTNAME"] = "my-databricks.example.com"
    os.environ["CREDENTIALS__HTTP_PATH"] = "/sql/1.0/warehouses/asdfe"
    os.environ["CREDENTIALS__ACCESS_TOKEN"] = "my-token"
    os.environ["CREDENTIALS__CATALOG"] = "my-catalog"
    # JSON encoded dict of extra args
    os.environ["CREDENTIALS__CONNECTION_PARAMETERS"] = '{"extra_a": "a", "extra_b": "b"}'

    config = resolve_configuration(
        DatabricksClientConfiguration()._bind_dataset_name(dataset_name="my-dataset")
    )

    credentials = config.credentials

    params = credentials.to_connector_params()

    assert params["server_hostname"] == "my-databricks.example.com"
    assert params["http_path"] == "/sql/1.0/warehouses/asdfe"
    assert params["access_token"] == "my-token"
    assert params["catalog"] == "my-catalog"
    assert params["extra_a"] == "a"
    assert params["extra_b"] == "b"
    assert params["_socket_timeout"] == credentials.socket_timeout
