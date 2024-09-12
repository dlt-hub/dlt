import os
import pytest
import sqlfluff
from copy import deepcopy
from sqlfluff.api.simple import APIParsingError

from dlt.common.utils import uniq_id
from dlt.common.schema import Schema, TColumnHint

from dlt.destinations import synapse
from dlt.destinations.impl.synapse.synapse import (
    SynapseClient,
    HINT_TO_SYNAPSE_ATTR,
    TABLE_INDEX_TYPE_TO_SYNAPSE_ATTR,
)
from dlt.destinations.impl.synapse.configuration import (
    SynapseClientConfiguration,
    SynapseCredentials,
)

from tests.load.utils import TABLE_UPDATE, empty_schema

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture
def client(empty_schema: Schema) -> SynapseClient:
    # return client without opening connection
    client = synapse().client(
        empty_schema,
        SynapseClientConfiguration(credentials=SynapseCredentials())._bind_dataset_name(
            dataset_name="test_" + uniq_id()
        ),
    )
    assert client.config.create_indexes is False
    return client


@pytest.fixture
def client_with_indexes_enabled(empty_schema: Schema) -> SynapseClient:
    # return client without opening connection
    client = synapse().client(
        empty_schema,
        SynapseClientConfiguration(
            credentials=SynapseCredentials(), create_indexes=True
        )._bind_dataset_name(dataset_name="test_" + uniq_id()),
    )
    assert client.config.create_indexes is True
    return client


def test_create_table(client: SynapseClient) -> None:
    # non existing table
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)[0]
    sqlfluff.parse(sql, dialect="tsql")
    assert "event_test_table" in sql
    assert '"col1" bigint  NOT NULL' in sql
    assert '"col2" float  NOT NULL' in sql
    assert '"col3" bit  NOT NULL' in sql
    assert '"col4" datetimeoffset  NOT NULL' in sql
    assert '"col5" nvarchar(max)  NOT NULL' in sql
    assert '"col6" decimal(38,9)  NOT NULL' in sql
    assert '"col7" varbinary(max)  NOT NULL' in sql
    assert '"col8" decimal(38,0)' in sql
    assert '"col9" nvarchar(max)  NOT NULL' in sql
    assert '"col10" date  NOT NULL' in sql
    assert '"col11" time  NOT NULL' in sql
    assert '"col1_precision" smallint  NOT NULL' in sql
    assert '"col4_precision" datetimeoffset(3)  NOT NULL' in sql
    assert '"col5_precision" nvarchar(25)' in sql
    assert '"col6_precision" decimal(6,2)  NOT NULL' in sql
    assert '"col7_precision" varbinary(19)' in sql
    assert '"col11_precision" time(3)  NOT NULL' in sql
    table_index_type = client.config.default_table_index_type
    table_index_type_attr = TABLE_INDEX_TYPE_TO_SYNAPSE_ATTR[table_index_type]
    assert f"WITH ( {table_index_type_attr} )" in sql


def test_alter_table(client: SynapseClient) -> None:
    # existing table has no columns
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, True)[0]
    sqlfluff.parse(sql, dialect="tsql")
    canonical_name = client.sql_client.make_qualified_table_name("event_test_table")
    assert sql.count(f"ALTER TABLE {canonical_name}\nADD") == 1
    assert "event_test_table" in sql
    assert '"col1" bigint  NOT NULL' in sql
    assert '"col2" float  NOT NULL' in sql
    assert '"col3" bit  NOT NULL' in sql
    assert '"col4" datetimeoffset  NOT NULL' in sql
    assert '"col5" nvarchar(max)  NOT NULL' in sql
    assert '"col6" decimal(38,9)  NOT NULL' in sql
    assert '"col7" varbinary(max)  NOT NULL' in sql
    assert '"col8" decimal(38,0)' in sql
    assert '"col9" nvarchar(max)  NOT NULL' in sql
    assert '"col10" date  NOT NULL' in sql
    assert '"col11" time  NOT NULL' in sql
    assert '"col1_precision" smallint  NOT NULL' in sql
    assert '"col4_precision" datetimeoffset(3)  NOT NULL' in sql
    assert '"col5_precision" nvarchar(25)' in sql
    assert '"col6_precision" decimal(6,2)  NOT NULL' in sql
    assert '"col7_precision" varbinary(19)' in sql
    assert '"col11_precision" time(3)  NOT NULL' in sql
    table_index_type = client.config.default_table_index_type
    table_index_type_attr = TABLE_INDEX_TYPE_TO_SYNAPSE_ATTR[table_index_type]
    assert f"WITH ( {table_index_type_attr} )" not in sql


@pytest.mark.parametrize("hint", ["primary_key", "unique"])
def test_create_table_with_column_hint(
    client: SynapseClient, client_with_indexes_enabled: SynapseClient, hint: TColumnHint
) -> None:
    attr = HINT_TO_SYNAPSE_ATTR[hint]

    # Case: table without hint.
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)[0]
    sqlfluff.parse(sql, dialect="tsql")
    assert f" {attr} " not in sql

    # Case: table with hint, but client does not have indexes enabled.
    mod_update = deepcopy(TABLE_UPDATE)
    mod_update[0][hint] = True
    sql = client._get_table_update_sql("event_test_table", mod_update, False)[0]
    sqlfluff.parse(sql, dialect="tsql")
    assert f" {attr} " not in sql

    # Case: table with hint, client has indexes enabled.
    sql = client_with_indexes_enabled._get_table_update_sql("event_test_table", mod_update, False)[
        0
    ]
    # We expect an error because "PRIMARY KEY NONCLUSTERED NOT ENFORCED" and
    # "UNIQUE NOT ENFORCED" are invalid in the generic "tsql" dialect.
    # They are however valid in the Synapse variant of the dialect.
    with pytest.raises(APIParsingError):
        sqlfluff.parse(sql, dialect="tsql")
    assert f'"col1" bigint {attr} NOT NULL' in sql
