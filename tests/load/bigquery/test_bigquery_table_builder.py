import pytest
from copy import deepcopy

from dlt.common.utils import custom_environ, uniq_id
from dlt.common.schema import Schema
from dlt.common.schema.utils import new_table
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import GcpClientCredentials

from dlt.destinations.bigquery.bigquery import BigQueryClient
from dlt.destinations.bigquery.configuration import BigQueryClientConfiguration
from dlt.destinations.exceptions import DestinationSchemaWillNotUpdate

from tests.load.utils import TABLE_UPDATE


@pytest.fixture
def schema() -> Schema:
    return Schema("event")


def test_configuration() -> None:
    # check names normalized
    with custom_environ({"CREDENTIALS__PRIVATE_KEY": "---NO NEWLINE---\n"}):
        C = resolve_configuration(GcpClientCredentials())
        assert C.private_key == "---NO NEWLINE---\n"

    with custom_environ({"CREDENTIALS__PRIVATE_KEY": "---WITH NEWLINE---\n"}):
        C = resolve_configuration(GcpClientCredentials())
        assert C.private_key == "---WITH NEWLINE---\n"


@pytest.fixture
def gcp_client(schema: Schema) -> BigQueryClient:
    # return client without opening connection
    return BigQueryClient(schema, BigQueryClientConfiguration(dataset_name="test_" + uniq_id(), credentials=GcpClientCredentials()))


def test_create_table(gcp_client: BigQueryClient) -> None:
    # non existing table
    sql = gcp_client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)
    assert sql.startswith("CREATE TABLE")
    assert "event_test_table" in sql
    assert "`col1` INTEGER NOT NULL" in sql
    assert "`col2` FLOAT64 NOT NULL" in sql
    assert "`col3` BOOLEAN NOT NULL" in sql
    assert "`col4` TIMESTAMP NOT NULL" in sql
    assert "`col5` STRING " in sql
    assert "`col6` NUMERIC(38,9) NOT NULL" in sql
    assert "`col7` BYTES" in sql
    assert "`col8` BIGNUMERIC" in sql
    assert "`col9` JSON NOT NULL)" in sql
    assert "CLUSTER BY" not in sql
    assert "PARTITION BY" not in sql


def test_alter_table(gcp_client: BigQueryClient) -> None:
    # existing table has no columns
    sql = gcp_client._get_table_update_sql("event_test_table", TABLE_UPDATE, True)
    assert sql.startswith("ALTER TABLE")
    assert "event_test_table" in sql
    assert "ADD COLUMN `col1` INTEGER NOT NULL" in sql
    assert "ADD COLUMN `col2` FLOAT64 NOT NULL" in sql
    assert "ADD COLUMN `col3` BOOLEAN NOT NULL" in sql
    assert "ADD COLUMN `col4` TIMESTAMP NOT NULL" in sql
    assert "ADD COLUMN `col5` STRING" in sql
    assert "ADD COLUMN `col6` NUMERIC(38,9) NOT NULL" in sql
    assert "ADD COLUMN `col7` BYTES" in sql
    assert "ADD COLUMN `col8` BIGNUMERIC" in sql
    assert "ADD COLUMN `col9` JSON NOT NULL" in sql
    # table has col1 already in storage
    mod_table = deepcopy(TABLE_UPDATE)
    mod_table.pop(0)
    sql = gcp_client._get_table_update_sql("event_test_table", mod_table, True)
    assert "ADD COLUMN `col1` INTEGER NOT NULL" not in sql
    assert "ADD COLUMN `col2` FLOAT64 NOT NULL" in sql


def test_create_table_with_partition_and_cluster(gcp_client: BigQueryClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[3]["partition"] = True
    mod_update[4]["cluster"] = True
    mod_update[1]["cluster"] = True
    sql = gcp_client._get_table_update_sql("event_test_table", mod_update, False)
    # clustering must be the last
    assert sql.endswith("CLUSTER BY `col2`,`col5`")
    assert "PARTITION BY DATE(`col4`)" in sql


def test_double_partition_exception(gcp_client: BigQueryClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[3]["partition"] = True
    mod_update[4]["partition"] = True
    # double partition
    with pytest.raises(DestinationSchemaWillNotUpdate) as excc:
        gcp_client._get_table_update_sql("event_test_table", mod_update, False)
    assert excc.value.columns == ["`col4`", "`col5`"]


def test_partition_alter_table_exception(gcp_client: BigQueryClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[3]["partition"] = True
    # double partition
    with pytest.raises(DestinationSchemaWillNotUpdate) as excc:
        gcp_client._get_table_update_sql("event_test_table", mod_update, True)
    assert excc.value.columns == ["`col4`"]


def test_cluster_alter_table_exception(gcp_client: BigQueryClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[3]["cluster"] = True
    # double cluster
    with pytest.raises(DestinationSchemaWillNotUpdate) as excc:
        gcp_client._get_table_update_sql("event_test_table", mod_update, True)
    assert excc.value.columns == ["`col4`"]
