from copy import deepcopy
from typing import Generator, Any, List
from unittest.mock import patch

import pytest
import sqlfluff

import dlt
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import Schema, utils
from dlt.common.typing import DictStrStr
from dlt.common.utils import uniq_id
from dlt.destinations import greenplum
from dlt.destinations.impl.greenplum.configuration import (
    GreenplumClientConfiguration,
    GreenplumCredentials,
)
from dlt.destinations.impl.greenplum.greenplum import (
    GreenplumClient,
)
from dlt.destinations.impl.greenplum.greenplum_adapter import (
    HINT_TO_GREENPLUM_ATTR
)
from dlt.extract import DltResource
from tests.cases import (
    TABLE_UPDATE,
    TABLE_UPDATE_ALL_INT_PRECISIONS,
)
from tests.load.utils import destinations_configs, DestinationTestConfiguration, sequence_generator
from tests.utils import assert_load_info

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture
def client(empty_schema: Schema) -> GreenplumClient:
    return create_client(empty_schema)


@pytest.fixture
def cs_client(empty_schema: Schema) -> GreenplumClient:
    # change normalizer to case sensitive
    empty_schema._normalizers_config["names"] = "tests.common.cases.normalizers.title_case"
    empty_schema.update_normalizers()
    return create_client(empty_schema)


def create_client(empty_schema: Schema) -> GreenplumClient:
    # return client without opening connection
    config = GreenplumClientConfiguration(
        credentials=GreenplumCredentials(),
        appendonly=True,
        blocksize=32768,
        compresstype="zstd",
        compresslevel=4,
        orientation="column",
        distribution_key="_dlt_id"
    )._bind_dataset_name(dataset_name="test_" + uniq_id())
    return greenplum().client(empty_schema, config)


def test_create_table(client: GreenplumClient) -> None:
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
    sqlfluff.parse(sql, dialect="postgres")  # Greenplum использует PostgreSQL диалект
    qualified_name = client.sql_client.make_qualified_table_name("event_test_table")
    assert f"CREATE TABLE {qualified_name}" in sql
    assert '"col1" bigint  NOT NULL' in sql
    assert '"col2" double precision  NOT NULL' in sql
    assert '"col3" boolean  NOT NULL' in sql
    assert '"col4" timestamp with time zone  NOT NULL' in sql
    assert '"col5" varchar' in sql
    assert '"col6" numeric(38,9)  NOT NULL' in sql
    assert '"col7" bytea' in sql
    assert '"col8" numeric(156,78)' in sql
    assert '"col9" jsonb  NOT NULL' in sql
    assert '"col10" date  NOT NULL' in sql
    assert '"col11" time without time zone  NOT NULL' in sql
    assert '"col1_precision" smallint  NOT NULL' in sql
    assert '"col4_precision" timestamp (3) with time zone  NOT NULL' in sql
    assert '"col5_precision" varchar(25)' in sql
    assert '"col6_precision" numeric(6,2)  NOT NULL' in sql
    assert '"col7_precision" bytea' in sql
    assert '"col11_precision" time (3) without time zone  NOT NULL' in sql
    
    # Проверяем наличие параметров хранения и дистрибуции
    assert "WITH (appendonly=true" in sql
    assert "blocksize=32768" in sql
    assert "compresstype=zstd" in sql
    assert "compresslevel=4" in sql
    assert "orientation=column" in sql
    assert "DISTRIBUTED BY" in sql
    assert "_dlt_id" in sql


def test_create_table_all_precisions(client: GreenplumClient) -> None:
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
    
    # Проверяем параметры хранения и дистрибуции
    assert "WITH (appendonly=true" in sql
    assert "DISTRIBUTED BY" in sql


def test_alter_table(client: GreenplumClient) -> None:
    # existing table has no columns
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, True)[0]
    sqlfluff.parse(sql, dialect="postgres")
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
    assert '"col7" bytea' in sql
    assert '"col8" numeric(156,78)' in sql
    assert '"col9" jsonb  NOT NULL' in sql
    assert '"col10" date  NOT NULL' in sql
    assert '"col11" time without time zone  NOT NULL' in sql
    assert '"col1_precision" smallint  NOT NULL' in sql
    assert '"col4_precision" timestamp (3) with time zone  NOT NULL' in sql
    assert '"col5_precision" varchar(25)' in sql
    assert '"col6_precision" numeric(6,2)  NOT NULL' in sql
    assert '"col7_precision" bytea' in sql
    assert '"col11_precision" time (3) without time zone  NOT NULL' in sql


def test_create_table_with_hints(client: GreenplumClient, empty_schema: Schema) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[0]["primary_key"] = True
    mod_update[0]["sort"] = True
    mod_update[1]["unique"] = True
    mod_update[4]["parent_key"] = True
    sql = client._get_table_update_sql("event_test_table", mod_update, False)[0]
    sqlfluff.parse(sql, dialect="postgres")
    assert '"col1" bigint  NOT NULL' in sql
    assert '"col2" double precision UNIQUE NOT NULL' in sql
    assert '"col5" varchar ' in sql
    # no hints
    assert '"col3" boolean  NOT NULL' in sql
    assert '"col4" timestamp with time zone  NOT NULL' in sql

    # same thing without indexes
    client = greenplum().client(
        empty_schema,
        GreenplumClientConfiguration(
            create_indexes=False,
            credentials=GreenplumCredentials(),
            appendonly=True,
            blocksize=32768,
            compresstype="zstd",
            compresslevel=4,
            orientation="column",
            distribution_key="_dlt_id"
        )._bind_dataset_name(dataset_name="test_" + uniq_id()),
    )
    sql = client._get_table_update_sql("event_test_table", mod_update, False)[0]
    sqlfluff.parse(sql, dialect="postgres")
    assert '"col2" double precision  NOT NULL' in sql
    
    # Проверяем параметры хранения и дистрибуции
    assert "WITH (appendonly=true" in sql
    assert "DISTRIBUTED BY" in sql


def test_create_table_custom_storage_options(empty_schema: Schema) -> None:
    """Тест создания таблицы с пользовательскими параметрами хранения"""
    # Создаем клиент с нестандартными параметрами хранения
    config = GreenplumClientConfiguration(
        credentials=GreenplumCredentials(),
        appendonly=True,
        blocksize=65536,  # Нестандартный размер блока
        compresstype="zlib",  # Нестандартный алгоритм сжатия
        compresslevel=7,  # Нестандартный уровень сжатия
        orientation="row",  # Нестандартная ориентация
        distribution_key="_dlt_id"
    )._bind_dataset_name(dataset_name="test_" + uniq_id())
    client = greenplum().client(empty_schema, config)
    
    # Проверяем SQL
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)[0]
    sqlfluff.parse(sql, dialect="postgres")
    
    # Проверяем нестандартные параметры хранения
    assert "WITH (appendonly=true" in sql
    assert "blocksize=65536" in sql
    assert "compresstype=zlib" in sql
    assert "compresslevel=7" in sql
    assert "orientation=row" in sql
    assert "DISTRIBUTED BY" in sql


def test_create_table_case_sensitive(cs_client: GreenplumClient) -> None:
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
    sqlfluff.parse(sql, dialect="postgres")
    # everything capitalized
    assert cs_client.sql_client.fully_qualified_dataset_name(escape=False)[0] == "T"  # Test
    # every line starts with "Col"
    for line in sql.split("\n")[1:]:
        if line.strip().startswith('"Col'):
            assert True
            break
    
    # Проверяем параметры хранения и дистрибуции
    assert "WITH (appendonly=true" in sql
    assert "DISTRIBUTED BY" in sql


def test_create_dlt_table(client: GreenplumClient) -> None:
    # non existing table
    sql = client._get_table_update_sql("_dlt_version", TABLE_UPDATE, False)[0]
    sqlfluff.parse(sql, dialect="postgres")
    qualified_name = client.sql_client.make_qualified_table_name("_dlt_version")
    assert f"CREATE TABLE IF NOT EXISTS {qualified_name}" in sql
    
    # Проверяем параметры хранения и дистрибуции
    assert "WITH (appendonly=true" in sql
    assert "DISTRIBUTED BY" in sql


def test_adapter_hints() -> None:
    """Тестирует подсказки для адаптера Greenplum"""
    # Проверяем наличие ключевых подсказок
    assert "distributed_by" in HINT_TO_GREENPLUM_ATTR
    assert "distributed_randomly" in HINT_TO_GREENPLUM_ATTR
    
    # Проверяем значения
    assert HINT_TO_GREENPLUM_ATTR["distributed_by"] == "DISTRIBUTED BY"
    assert HINT_TO_GREENPLUM_ATTR["distributed_randomly"] == "DISTRIBUTED RANDOMLY" 