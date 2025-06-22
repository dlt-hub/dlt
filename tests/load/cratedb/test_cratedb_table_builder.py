from copy import deepcopy
from typing import cast

import pytest
import sqlfluff

from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import Schema, utils
from dlt.common.utils import uniq_id
from dlt.destinations import cratedb
from dlt.destinations.impl.cratedb.configuration import (
    CrateDbClientConfiguration,
    CrateDbCredentials,
)
from dlt.destinations.impl.cratedb.cratedb import (
    CrateDbClient,
)
from dlt.destinations.impl.postgres.postgres import PostgresClient
from tests.cases import (
    TABLE_UPDATE,
    TABLE_UPDATE_ALL_INT_PRECISIONS,
)


@pytest.fixture
def client(empty_schema: Schema, credentials: CrateDbCredentials) -> CrateDbClient:
    return create_client(empty_schema, credentials=credentials)


@pytest.fixture
def cs_client(empty_schema: Schema, credentials: CrateDbCredentials) -> CrateDbClient:
    # change normalizer to case sensitive
    empty_schema._normalizers_config["names"] = "tests.common.cases.normalizers.title_case"
    empty_schema.update_normalizers()
    return create_client(empty_schema, credentials=credentials)


def create_client(empty_schema: Schema, credentials: CrateDbCredentials) -> CrateDbClient:
    # return client without opening connection
    config = CrateDbClientConfiguration(credentials=credentials)._bind_dataset_name(
        dataset_name="test_" + uniq_id()
    )
    return cast(CrateDbClient, cratedb().client(empty_schema, config))


def test_create_table(client: CrateDbClient) -> None:
    # make sure we are in case insensitive mode
    assert client.capabilities.generates_case_sensitive_identifiers() is False
    # check if dataset name is properly folded
    assert client.sql_client.dataset_name == client.config.dataset_name  # identical to config
    assert (
        client.sql_client.staging_dataset_name
        == client.config.staging_dataset_name_layout % client.config.dataset_name
    )
    # non existing table
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)[0]
    # FIXME: SQLFluff does not support CrateDB yet, failing on its special data types.
    # sqlfluff.parse(sql, dialect="postgres")
    qualified_name = client.sql_client.make_qualified_table_name("event_test_table")
    assert f"CREATE TABLE {qualified_name}" in sql
    assert '"col1" bigint  NOT NULL' in sql
    assert '"col2" double precision  NOT NULL' in sql
    assert '"col3" boolean  NOT NULL' in sql
    assert '"col4" timestamp with time zone  NOT NULL' in sql
    assert '"col5" varchar' in sql
    assert '"col6" numeric(38,9)  NOT NULL' in sql
    assert '"col7" text' in sql
    assert '"col8" numeric(156,78)' in sql
    assert '"col9" object(dynamic)  NOT NULL' in sql
    assert '"col10" date  NOT NULL' in sql
    assert '"col11" time without time zone  NOT NULL' in sql
    assert '"col1_precision" smallint  NOT NULL' in sql
    assert '"col4_precision" timestamp with time zone  NOT NULL' in sql
    assert '"col5_precision" varchar(25)' in sql
    assert '"col6_precision" numeric(6,2)  NOT NULL' in sql
    assert '"col7_precision" text' in sql
    assert '"col11_precision" time without time zone  NOT NULL' in sql


def test_create_table_all_precisions(client: CrateDbClient) -> None:
    # 128 bit integer will fail
    table_update = list(TABLE_UPDATE_ALL_INT_PRECISIONS)
    with pytest.raises(TerminalValueError) as tv_ex:
        sql = client._get_table_update_sql("event_test_table", table_update, False)[0]
    assert "128" in str(tv_ex.value)

    # remove col5 HUGEINT which is last
    table_update.pop()
    sql = client._get_table_update_sql("event_test_table", table_update, False)[0]
    sqlfluff.parse(sql, dialect="postgres")
    assert '"col1_int" smallint ' in sql
    assert '"col2_int" smallint ' in sql
    assert '"col3_int" integer ' in sql
    assert '"col4_int" bigint ' in sql


def test_alter_table(client: CrateDbClient) -> None:
    # existing table has no columns
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, True)[0]
    # FIXME: SQLFluff does not support CrateDB yet, failing on its special data types.
    # sqlfluff.parse(sql, dialect="postgres")
    canonical_name = client.sql_client.make_qualified_table_name("event_test_table")
    # must have several ALTER TABLE statements
    assert sql.count(f"ALTER TABLE {canonical_name}\nADD COLUMN") == 1
    assert "event_test_table" in sql
    assert '"col1" bigint  NOT NULL' in sql
    assert '"col2" double precision  NOT NULL' in sql
    assert '"col3" boolean  NOT NULL' in sql
    assert '"col4" timestamp with time zone  NOT NULL' in sql
    assert '"col5" varchar' in sql
    assert '"col6" numeric(38,9)  NOT NULL' in sql
    assert '"col7" text' in sql
    assert '"col8" numeric(156,78)' in sql
    assert '"col9" object(dynamic)  NOT NULL' in sql
    assert '"col10" date  NOT NULL' in sql
    assert '"col11" time without time zone  NOT NULL' in sql
    assert '"col1_precision" smallint  NOT NULL' in sql
    assert '"col4_precision" timestamp with time zone  NOT NULL' in sql
    assert '"col5_precision" varchar(25)' in sql
    assert '"col6_precision" numeric(6,2)  NOT NULL' in sql
    assert '"col7_precision" text' in sql
    assert '"col11_precision" time without time zone  NOT NULL' in sql


def test_create_table_with_hints(
    client: CrateDbClient, empty_schema: Schema, credentials: CrateDbCredentials
) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[0]["primary_key"] = True
    mod_update[0]["sort"] = True
    mod_update[1]["unique"] = True
    mod_update[4]["parent_key"] = True
    sql = client._get_table_update_sql("event_test_table", mod_update, False)[0]
    # FIXME: SQLFluff does not support CrateDB yet, failing on its special data types.
    # sqlfluff.parse(sql, dialect="postgres")
    assert '"col1" bigint  NOT NULL' in sql
    assert '"col2" double precision  NOT NULL' in sql
    assert '"col5" varchar ' in sql
    # no hints
    assert '"col3" boolean  NOT NULL' in sql
    assert '"col4" timestamp with time zone  NOT NULL' in sql

    # same thing without indexes
    client = cast(
        CrateDbClient,
        cratedb().client(
            empty_schema,
            CrateDbClientConfiguration(
                create_indexes=False,
                credentials=credentials,
            )._bind_dataset_name(dataset_name="test_" + uniq_id()),
        ),
    )
    sql = client._get_table_update_sql("event_test_table", mod_update, False)[0]
    # FIXME: SQLFluff does not support CrateDB yet, failing on its special data types.
    # sqlfluff.parse(sql, dialect="postgres")
    assert '"col2" double precision  NOT NULL' in sql


def test_create_table_case_sensitive(cs_client: CrateDbClient) -> None:
    # did we switch to case sensitive
    assert cs_client.capabilities.generates_case_sensitive_identifiers() is True
    # check dataset names
    assert cs_client.sql_client.dataset_name.startswith("Test")
    with cs_client.with_staging_dataset():
        assert cs_client.sql_client.dataset_name.endswith("staginG")
    assert cs_client.sql_client.staging_dataset_name.endswith("staginG")
    # check tables
    cs_client.schema.update_table(
        utils.new_table("event_test_table", columns=deepcopy(TABLE_UPDATE))
    )
    sql = cs_client._get_table_update_sql(
        "Event_test_tablE",
        list(cs_client.schema.get_table_columns("Event_test_tablE").values()),
        False,
    )[0]
    # FIXME: SQLFluff does not support CrateDB yet, failing on its special data types.
    # sqlfluff.parse(sql, dialect="postgres")
    # everything capitalized
    assert cs_client.sql_client.fully_qualified_dataset_name(escape=False)[0] == "T"  # Test
    # every line starts with "Col"
    for line in sql.split("\n")[1:]:
        assert line.startswith('"Col')


def test_create_dlt_table(client: CrateDbClient) -> None:
    # non existing table
    sql = client._get_table_update_sql("_dlt_version", TABLE_UPDATE, False)[0]
    # FIXME: SQLFluff does not support CrateDB yet, failing on its special data types.
    # sqlfluff.parse(sql, dialect="postgres")
    qualified_name = client.sql_client.make_qualified_table_name("_dlt_version")
    assert f"CREATE TABLE IF NOT EXISTS {qualified_name}" in sql
