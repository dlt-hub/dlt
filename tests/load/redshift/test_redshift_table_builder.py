import pytest
import sqlfluff
from copy import deepcopy

from dlt.common.utils import uniq_id, custom_environ, digest128
from dlt.common.schema import Schema
from dlt.common.configuration import resolve_configuration

from dlt.destinations.impl.redshift.redshift import RedshiftClient
from dlt.destinations.impl.redshift.configuration import (
    RedshiftClientConfiguration,
    RedshiftCredentials,
)

from tests.load.utils import TABLE_UPDATE


@pytest.fixture
def schema() -> Schema:
    return Schema("event")


@pytest.fixture
def client(schema: Schema) -> RedshiftClient:
    # return client without opening connection
    return RedshiftClient(
        schema,
        RedshiftClientConfiguration(
            dataset_name="test_" + uniq_id(), credentials=RedshiftCredentials()
        ),
    )


def test_redshift_configuration() -> None:
    # check names normalized
    with custom_environ(
        {
            "DESTINATION__MY_REDSHIFT__CREDENTIALS__USERNAME": "username",
            "DESTINATION__MY_REDSHIFT__CREDENTIALS__HOST": "host",
            "DESTINATION__MY_REDSHIFT__CREDENTIALS__DATABASE": "UPPER_CASE_DATABASE",
            "DESTINATION__MY_REDSHIFT__CREDENTIALS__PASSWORD": " pass\n",
        }
    ):
        C = resolve_configuration(RedshiftCredentials(), sections=("destination", "my_redshift"))
        assert C.database == "upper_case_database"
        assert C.password == "pass"

    # check fingerprint
    assert RedshiftClientConfiguration().fingerprint() == ""
    # based on host
    c = resolve_configuration(
        RedshiftCredentials(),
        explicit_value="postgres://user1:pass@host1/db1?warehouse=warehouse1&role=role1",
    )
    assert RedshiftClientConfiguration(credentials=c).fingerprint() == digest128("host1")


def test_create_table(client: RedshiftClient) -> None:
    # non existing table
    sql = ";".join(client._get_table_update_sql("event_test_table", TABLE_UPDATE, False))
    sqlfluff.parse(sql, dialect="redshift")
    assert "event_test_table" in sql
    assert '"col1" bigint  NOT NULL' in sql
    assert '"col2" double precision  NOT NULL' in sql
    assert '"col3" boolean  NOT NULL' in sql
    assert '"col4" timestamp with time zone  NOT NULL' in sql
    assert '"col5" varchar(max)' in sql
    assert '"col6" numeric(38,9)  NOT NULL' in sql
    assert '"col7" varbinary' in sql
    assert '"col8" numeric(38,0)' in sql
    assert '"col9" super  NOT NULL' in sql
    assert '"col10" date  NOT NULL' in sql
    assert '"col11" time without time zone  NOT NULL' in sql
    assert '"col1_precision" smallint  NOT NULL' in sql
    assert '"col4_precision" timestamp with time zone  NOT NULL' in sql
    assert '"col5_precision" varchar(25)' in sql
    assert '"col6_precision" numeric(6,2)  NOT NULL' in sql
    assert '"col7_precision" varbinary(19)' in sql
    assert '"col11_precision" time without time zone  NOT NULL' in sql


def test_alter_table(client: RedshiftClient) -> None:
    # existing table has no columns
    sql = ";".join(client._get_table_update_sql("event_test_table", TABLE_UPDATE, True))
    sqlfluff.parse(sql, dialect="redshift")
    canonical_name = client.sql_client.make_qualified_table_name("event_test_table")
    # must have several ALTER TABLE statements
    assert sql.count(f"ALTER TABLE {canonical_name}\nADD COLUMN") == len(TABLE_UPDATE)
    assert "event_test_table" in sql
    assert '"col1" bigint  NOT NULL' in sql
    assert '"col2" double precision  NOT NULL' in sql
    assert '"col3" boolean  NOT NULL' in sql
    assert '"col4" timestamp with time zone  NOT NULL' in sql
    assert '"col5" varchar(max)' in sql
    assert '"col6" numeric(38,9)  NOT NULL' in sql
    assert '"col7" varbinary' in sql
    assert '"col8" numeric(38,0)' in sql
    assert '"col9" super  NOT NULL' in sql
    assert '"col10" date  NOT NULL' in sql
    assert '"col11" time without time zone  NOT NULL' in sql
    assert '"col1_precision" smallint  NOT NULL' in sql
    assert '"col4_precision" timestamp with time zone  NOT NULL' in sql
    assert '"col5_precision" varchar(25)' in sql
    assert '"col6_precision" numeric(6,2)  NOT NULL' in sql
    assert '"col7_precision" varbinary(19)' in sql
    assert '"col11_precision" time without time zone  NOT NULL' in sql


def test_create_table_with_hints(client: RedshiftClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[0]["primary_key"] = True
    mod_update[0]["sort"] = True
    mod_update[1]["cluster"] = True
    mod_update[4]["cluster"] = True
    sql = ";".join(client._get_table_update_sql("event_test_table", mod_update, False))
    sqlfluff.parse(sql, dialect="redshift")
    # PRIMARY KEY will not be present https://heap.io/blog/redshift-pitfalls-avoid
    assert '"col1" bigint SORTKEY NOT NULL' in sql
    assert '"col2" double precision DISTKEY NOT NULL' in sql
    assert '"col5" varchar(max) DISTKEY' in sql
    # no hints
    assert '"col3" boolean  NOT NULL' in sql
    assert '"col4" timestamp with time zone  NOT NULL' in sql
