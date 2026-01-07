import os
from typing import cast
import pytest

from dlt.destinations.adapters import clickhouse_adapter
from dlt.destinations.impl.clickhouse.configuration import ClickHouseCredentials
from dlt.destinations.impl.clickhouse.sql_client import ClickHouseSqlClient
from tests.load.clickhouse_cluster.utils import get_table_engine
from tests.load.utils import DestinationTestConfiguration, destinations_configs
from tests.pipeline.utils import assert_load_info


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["clickhouse_cluster"]),
    ids=lambda x: x.name,
)
def test_replication(destination_config: DestinationTestConfiguration) -> None:
    """Tests typical replication use case of ClickHouse cluster."""

    data = [{"foo": "bar"}]
    replicated = clickhouse_adapter(data, table_engine_type="replicated_merge_tree").apply_hints(
        table_name="replicated"
    )
    not_replicated = clickhouse_adapter(data, table_engine_type="merge_tree").apply_hints(
        table_name="not_replicated"
    )
    pipe = destination_config.setup_pipeline("test_replication", dev_mode=True)

    # check assumption that we are connecting to first cluster node
    credentials = cast(ClickHouseCredentials, pipe.destination_client().config.credentials)
    assert credentials.port == 9001
    assert credentials.http_port == 8124

    # run pipeline
    load_info = pipe.run([replicated, not_replicated], **destination_config.run_kwargs)
    assert_load_info(load_info)

    # assert table engines
    sql_client = cast(ClickHouseSqlClient, pipe.sql_client())
    assert get_table_engine(sql_client, table_name="replicated") == "ReplicatedMergeTree"
    assert get_table_engine(sql_client, table_name="not_replicated") == "MergeTree"

    # assert row counts on first cluster node
    first_node_dataset = pipe.dataset()
    assert len(first_node_dataset["replicated"].fetchall()) == 1
    assert len(first_node_dataset["not_replicated"].fetchall()) == 1

    # change ports to connect to second cluster node
    os.environ["DESTINATION__CLICKHOUSE_CLUSTER__CREDENTIALS__PORT"] = "9002"
    os.environ["DESTINATION__CLICKHOUSE_CLUSTER__CREDENTIALS__HTTP_PORT"] = "8125"

    # assert row counts on second cluster node
    second_node_dataset = pipe.dataset()
    assert len(second_node_dataset["replicated"].fetchall()) == 1  # row was replicated
    assert len(second_node_dataset["not_replicated"].fetchall()) == 0  # row was not replicated
