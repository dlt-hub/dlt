import pytest
import sqlfluff
from copy import deepcopy

from dlt.common.utils import uniq_id, custom_environ, digest128
from dlt.common.schema import Schema, utils
from dlt.common.configuration import resolve_configuration

from dlt.destinations import redshift
from dlt.destinations.impl.redshift.redshift import RedshiftClient
from dlt.destinations.impl.redshift.configuration import (
    RedshiftClientConfiguration,
    RedshiftCredentials,
)

from tests.load.utils import TABLE_UPDATE, empty_schema

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture
def client(empty_schema: Schema) -> RedshiftClient:
    return create_client(empty_schema)


@pytest.fixture
def cs_client(empty_schema: Schema) -> RedshiftClient:
    empty_schema._normalizers_config["names"] = "tests.common.cases.normalizers.title_case"
    empty_schema.update_normalizers()
    # make the destination case sensitive
    return create_client(empty_schema, has_case_sensitive_identifiers=True)


def create_client(schema: Schema, has_case_sensitive_identifiers: bool = False) -> RedshiftClient:
    # return client without opening connection
    return redshift().client(
        schema,
        RedshiftClientConfiguration(
            credentials=RedshiftCredentials(),
            has_case_sensitive_identifiers=has_case_sensitive_identifiers,
        )._bind_dataset_name(dataset_name="test_" + uniq_id()),
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
        assert C.database == "UPPER_CASE_DATABASE"
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
    assert client.capabilities.generates_case_sensitive_identifiers() is False
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


def test_create_table_case_sensitive(cs_client: RedshiftClient) -> None:
    # did we switch to case sensitive
    assert cs_client.capabilities.generates_case_sensitive_identifiers() is True
    # check dataset names
    assert cs_client.sql_client.dataset_name.startswith("Test")

    # check tables
    cs_client.schema.update_table(
        utils.new_table("event_test_table", columns=deepcopy(TABLE_UPDATE))
    )
    sql = cs_client._get_table_update_sql(
        "Event_test_tablE",
        list(cs_client.schema.get_table_columns("Event_test_tablE").values()),
        False,
    )[0]
    sqlfluff.parse(sql, dialect="redshift")
    # everything capitalized
    assert cs_client.sql_client.fully_qualified_dataset_name(escape=False)[0] == "T"  # Test
    # every line starts with "Col"
    for line in sql.split("\n")[1:]:
        assert line.startswith('"Col')


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
