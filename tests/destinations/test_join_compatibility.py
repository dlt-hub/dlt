"""Tests for destination configuration-level join-compatibility semantics."""

from typing import Callable, cast

from typing_extensions import TypeAlias

import pytest

from dlt.common.utils import digest128
from dlt.common.configuration.specs import (
    AwsCredentials,
    ConnectionStringCredentials,
    GcpServiceAccountCredentials,
)
from dlt.common.destination.client import DestinationClientConfiguration
from dlt.dataset.dataset import Dataset, is_same_physical_destination
from dlt.destinations.impl.postgres.configuration import (
    PostgresClientConfiguration,
    PostgresCredentials,
)
from dlt.destinations.impl.redshift.configuration import (
    RedshiftClientConfiguration,
    RedshiftCredentials,
)
from dlt.destinations.impl.snowflake.configuration import (
    SnowflakeClientConfiguration,
    SnowflakeCredentials,
)
from dlt.destinations.impl.bigquery.configuration import BigQueryClientConfiguration
from dlt.destinations.impl.mssql.configuration import (
    MsSqlClientConfiguration,
    MsSqlCredentials,
)
from dlt.destinations.impl.synapse.configuration import (
    SynapseClientConfiguration,
    SynapseCredentials,
)
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseClientConfiguration,
    ClickHouseCredentials,
)
from dlt.destinations.impl.databricks.configuration import (
    DatabricksClientConfiguration,
    DatabricksCredentials,
)
from dlt.destinations.impl.athena.configuration import AthenaClientConfiguration
from dlt.destinations.impl.dremio.configuration import (
    DremioClientConfiguration,
    DremioCredentials,
)
from dlt.destinations.impl.duckdb.configuration import (
    DuckDbClientConfiguration,
    DuckDbCredentials,
)
from dlt.destinations.impl.filesystem.configuration import (
    FilesystemDestinationClientConfiguration,
)
from dlt.destinations.impl.ducklake.configuration import (
    DuckLakeClientConfiguration,
    DuckLakeCredentials,
    DEFAULT_DUCKLAKE_NAME,
)
from dlt.destinations.impl.fabric.configuration import (
    FabricClientConfiguration,
    FabricCredentials,
)
from dlt.destinations.impl.motherduck.configuration import (
    MotherDuckClientConfiguration,
    MotherDuckCredentials,
)
from dlt.destinations.impl.sqlalchemy.configuration import (
    SqlalchemyClientConfiguration,
    SqlalchemyCredentials,
)
from dlt.destinations.impl.lancedb.configuration import (
    LanceDBClientConfiguration,
    LanceDBCredentials,
)
from dlt.destinations.impl.lance.configuration import (
    DirectoryCatalogCredentials,
    LanceClientConfiguration,
    LanceStorageConfiguration,
)
from dlt.destinations.impl.qdrant.configuration import QdrantClientConfiguration
from dlt.destinations.impl.weaviate.configuration import (
    WeaviateClientConfiguration,
    WeaviateCredentials,
)


ConfigFactory: TypeAlias = Callable[[], DestinationClientConfiguration]


class _PhysicalDestinationConfig(DestinationClientConfiguration):
    def __init__(self, physical_destination: str = "") -> None:
        super().__init__()
        self._physical_destination = physical_destination

    def physical_destination(self) -> str:
        return self._physical_destination


class _StringyPhysicalDestinationConfig(_PhysicalDestinationConfig):
    def __init__(self, physical_destination: str, display_value: str) -> None:
        super().__init__(physical_destination)
        self._display_value = display_value

    def __str__(self) -> str:
        return self._display_value


class _DestinationClientStub:
    def __init__(self, config: DestinationClientConfiguration) -> None:
        self.config = config


class _DatasetStub:
    def __init__(self, config: DestinationClientConfiguration) -> None:
        self.destination_client = _DestinationClientStub(config)


def assert_joinable(
    config1: DestinationClientConfiguration, config2: DestinationClientConfiguration
) -> None:
    assert config1.can_join_with(config2)
    assert config2.can_join_with(config1)


def assert_not_joinable(
    config1: DestinationClientConfiguration, config2: DestinationClientConfiguration
) -> None:
    assert not config1.can_join_with(config2)
    assert not config2.can_join_with(config1)


def assert_join_result(
    config1: DestinationClientConfiguration,
    config2: DestinationClientConfiguration,
    expected: bool,
) -> None:
    if expected:
        assert_joinable(config1, config2)
    else:
        assert_not_joinable(config1, config2)


def _athena_config(region: str, catalog: str = "awsdatacatalog") -> AthenaClientConfiguration:
    """Build Athena config."""
    return AthenaClientConfiguration(
        credentials=AwsCredentials(region_name=region),
        aws_data_catalog=catalog,
    )


def _ducklake_creds(catalog_str: str, name: str = DEFAULT_DUCKLAKE_NAME) -> DuckLakeCredentials:
    """Build DuckLake credentials."""
    return DuckLakeCredentials(
        ducklake_name=name,
        catalog=ConnectionStringCredentials(catalog_str),
    )


def _fabric_creds(host: str, database: str) -> FabricCredentials:
    """Build Fabric credentials."""
    # Fabric is normally configured via structured fields, not a connection string.
    credentials = FabricCredentials()
    credentials.host = host
    credentials.database = database
    return credentials


def _sqla_creds(connection_string: str) -> SqlalchemyCredentials:
    """Parse SQLAlchemy credentials."""
    creds = SqlalchemyCredentials()
    creds.parse_native_representation(connection_string)
    return creds


def _sqla_config(conn_str: str) -> SqlalchemyClientConfiguration:
    """Build SQLAlchemy config."""
    c = SqlalchemyClientConfiguration()
    c.credentials = _sqla_creds(conn_str)
    return c


def _lancedb_config(
    lance_uri: str,
    dataset_name: str = "dataset",
    dataset_separator: str = "___",
) -> LanceDBClientConfiguration:
    """Build resolved LanceDB config."""
    c = LanceDBClientConfiguration(
        lance_uri=lance_uri,
        credentials=LanceDBCredentials(uri=lance_uri),
        dataset_separator=dataset_separator,
    )
    c._bind_dataset_name(dataset_name)
    return c


def _lance_config(catalog_root: str, dataset_name: str = "dataset") -> LanceClientConfiguration:
    """Build resolved Lance config."""
    c = LanceClientConfiguration(
        credentials=DirectoryCatalogCredentials(bucket_url=catalog_root),
        storage=LanceStorageConfiguration(bucket_url=catalog_root),
    )
    c._bind_dataset_name(dataset_name)
    c.credentials.bucket_url = catalog_root
    return c


# Base DestinationClientConfiguration contract
def test_base_fingerprint_derived_from_physical_destination() -> None:
    config = _PhysicalDestinationConfig("test-host:5432")
    assert config.fingerprint() == digest128("test-host:5432")


def test_base_fingerprint_empty_when_physical_destination_empty() -> None:
    config = DestinationClientConfiguration()
    assert config.physical_destination() == ""
    assert config.fingerprint() == ""


def test_base_can_join_with_default_false_when_physical_destinations_differ() -> None:
    config1 = _PhysicalDestinationConfig("host1")
    config2 = _PhysicalDestinationConfig("host2")
    assert_not_joinable(config1, config2)


def test_base_can_join_with_default_true_when_same_physical_destination() -> None:
    config1 = _PhysicalDestinationConfig("host1")
    config2 = _PhysicalDestinationConfig("host1")
    assert_joinable(config1, config2)


def test_base_can_join_with_default_false_when_empty_physical_destination() -> None:
    config1 = DestinationClientConfiguration()
    config2 = _PhysicalDestinationConfig("host1")
    assert_not_joinable(config1, config2)


def test_base_can_join_with_returns_false_for_non_config() -> None:
    config = _PhysicalDestinationConfig("host1")
    assert not config.can_join_with("not a config")  # type: ignore[arg-type]
    assert not config.can_join_with(None)
    assert not config.can_join_with(42)  # type: ignore[arg-type]


def test_is_same_physical_destination_delegates_to_can_join_with() -> None:
    config1 = _StringyPhysicalDestinationConfig("host1", "first-display")
    config2 = _StringyPhysicalDestinationConfig("host1", "second-display")
    assert str(config1) != str(config2)
    assert is_same_physical_destination(
        cast(Dataset, _DatasetStub(config1)), cast(Dataset, _DatasetStub(config2))
    )


# physical_destination() extraction across destinations

PHYSICAL_DEST_CASES = [
    # Postgres: host:port format
    pytest.param(
        lambda: PostgresClientConfiguration(
            credentials=PostgresCredentials("postgresql://u:p@h:5432/db")
        ),
        "h:5432",
        id="pg_explicit_port",
    ),
    pytest.param(
        lambda: PostgresClientConfiguration(credentials=PostgresCredentials("postgresql://h")),
        "h:5432",
        id="pg_default_port",
    ),
    pytest.param(
        lambda: PostgresClientConfiguration(credentials=PostgresCredentials()), "", id="pg_no_host"
    ),
    # Redshift
    pytest.param(
        lambda: RedshiftClientConfiguration(
            credentials=RedshiftCredentials("redshift://u:p@h:5439/db")
        ),
        "h:5439",
        id="rs_explicit_port",
    ),
    pytest.param(
        lambda: RedshiftClientConfiguration(credentials=RedshiftCredentials("redshift://h")),
        "h:5439",
        id="rs_default_port",
    ),
    # Snowflake
    pytest.param(
        lambda: SnowflakeClientConfiguration(
            credentials=SnowflakeCredentials("snowflake://u:p@sf.snowflakecomputing.com/db")
        ),
        "sf.snowflakecomputing.com",
        id="sf_host",
    ),
    pytest.param(
        lambda: SnowflakeClientConfiguration(credentials=SnowflakeCredentials()),
        "",
        id="sf_no_host",
    ),
    # BigQuery: project_id from config or credentials
    pytest.param(
        lambda: BigQueryClientConfiguration(
            credentials=GcpServiceAccountCredentials(project_id="cred-proj"),
            project_id="cfg-proj",
        ),
        "cfg-proj",
        id="bq_config_project",
    ),
    pytest.param(
        lambda: BigQueryClientConfiguration(
            credentials=GcpServiceAccountCredentials(project_id="cred-proj")
        ),
        "cred-proj",
        id="bq_cred_project",
    ),
    pytest.param(lambda: BigQueryClientConfiguration(), "", id="bq_no_project"),
    # MSSQL / Synapse
    pytest.param(
        lambda: MsSqlClientConfiguration(credentials=MsSqlCredentials("mssql://h")),
        "h:1433",
        id="mssql_host",
    ),
    pytest.param(
        lambda: MsSqlClientConfiguration(credentials=MsSqlCredentials()), "", id="mssql_no_host"
    ),
    pytest.param(
        lambda: SynapseClientConfiguration(credentials=SynapseCredentials("mssql://h")),
        "h:1433",
        id="synapse_host",
    ),
    # ClickHouse
    pytest.param(
        lambda: ClickHouseClientConfiguration(credentials=ClickHouseCredentials("clickhouse://h")),
        "h:9440",
        id="ch_host",
    ),
    # Databricks
    pytest.param(
        lambda: DatabricksClientConfiguration(
            credentials=DatabricksCredentials(server_hostname="w.cloud.databricks.com")
        ),
        "w.cloud.databricks.com",
        id="dbr_server",
    ),
    # Athena
    pytest.param(
        lambda: _athena_config("us-west-2", "cat"), "us-west-2/cat", id="athena_region_catalog"
    ),
    pytest.param(
        lambda: AthenaClientConfiguration(
            credentials=AwsCredentials(),
            aws_data_catalog="cat",
        ),
        "",
        id="athena_no_region",
    ),
    pytest.param(
        lambda: _athena_config("eu-central-1"),
        "eu-central-1/awsdatacatalog",
        id="athena_default_catalog",
    ),
    # Dremio
    pytest.param(
        lambda: DremioClientConfiguration(credentials=DremioCredentials("grpc://h")),
        "h:32010",
        id="dremio_host",
    ),
    # DuckDB
    pytest.param(
        lambda: DuckDbClientConfiguration(credentials=DuckDbCredentials("/p/db.duckdb")),
        "/p/db.duckdb",
        id="duckdb_path",
    ),
    pytest.param(
        lambda: FilesystemDestinationClientConfiguration(bucket_url="s3://b/p"),
        "s3://b",
        id="fs_remote",
    ),
    pytest.param(
        lambda: FilesystemDestinationClientConfiguration(bucket_url="/local/p"), "", id="fs_local"
    ),
    # DuckLake
    pytest.param(
        lambda: DuckLakeClientConfiguration(
            credentials=_ducklake_creds("pg://u@h:5432/db", "lake")
        ),
        "pg://h:5432/db#lake",
        id="dl_remote_cat",
    ),
    pytest.param(
        lambda: DuckLakeClientConfiguration(
            credentials=_ducklake_creds("sqlite:///cat.sqlite", "lake")
        ),
        "sqlite://cat.sqlite#lake",
        id="dl_local_cat",
    ),
    pytest.param(
        lambda: DuckLakeClientConfiguration(credentials=_ducklake_creds("sqlite:///cat.sqlite")),
        f"sqlite://cat.sqlite#{DEFAULT_DUCKLAKE_NAME}",
        id="dl_default_name",
    ),
    # Fabric
    pytest.param(
        lambda: FabricClientConfiguration(
            credentials=_fabric_creds("h.fabric.microsoft.com", "db")
        ),
        "h.fabric.microsoft.com:1433",
        id="fabric_port",
    ),
    pytest.param(
        lambda: FabricClientConfiguration(
            credentials=_fabric_creds("h.fabric.microsoft.com", "db")
        ),
        "h.fabric.microsoft.com:1433",
        id="fabric_default_port",
    ),
    pytest.param(
        lambda: MotherDuckClientConfiguration(
            credentials=MotherDuckCredentials("md:db?motherduck_token=token")
        ),
        "",
        id="md_empty",
    ),
]


@pytest.mark.parametrize("factory,expected", PHYSICAL_DEST_CASES)
def test_physical_destination(factory: ConfigFactory, expected: str) -> None:
    assert factory().physical_destination() == expected


@pytest.mark.parametrize(
    "factory,expected_fp",
    [
        pytest.param(
            lambda: PostgresClientConfiguration(credentials=PostgresCredentials("postgresql://h")),
            digest128("h:5432"),
            id="pg",
        ),
        pytest.param(
            lambda: SnowflakeClientConfiguration(
                credentials=SnowflakeCredentials("snowflake://u:p@h/db")
            ),
            digest128("h"),
            id="sf",
        ),
        pytest.param(
            lambda: BigQueryClientConfiguration(
                credentials=GcpServiceAccountCredentials(project_id="p")
            ),
            digest128("p"),
            id="bq",
        ),
        pytest.param(
            lambda: FilesystemDestinationClientConfiguration(bucket_url="s3://b/p"),
            digest128("s3://b"),
            id="fs",
        ),
        pytest.param(
            lambda: MotherDuckClientConfiguration(
                credentials=MotherDuckCredentials("md:db?motherduck_token=token")
            ),
            digest128("token"),
            id="md_token_hash",
        ),
    ],
)
def test_fingerprint(factory: ConfigFactory, expected_fp: str) -> None:
    assert factory().fingerprint() == expected_fp


# can_join_with() matrices (symmetric)

MSSQL_JOIN_CASES = [
    pytest.param(
        lambda: MsSqlClientConfiguration(credentials=MsSqlCredentials("mssql://u:p@h:1433/db1")),
        lambda: MsSqlClientConfiguration(credentials=MsSqlCredentials("mssql://u:p@h:1433/db2")),
        True,
        id="mssql_same_host_diff_db",
    ),
    pytest.param(
        lambda: MsSqlClientConfiguration(credentials=MsSqlCredentials("mssql://h1")),
        lambda: MsSqlClientConfiguration(credentials=MsSqlCredentials("mssql://h2")),
        False,
        id="mssql_diff_host",
    ),
    pytest.param(
        lambda: MsSqlClientConfiguration(credentials=MsSqlCredentials("mssql://h:1433/db")),
        lambda: MsSqlClientConfiguration(credentials=MsSqlCredentials("mssql://h:1434/db")),
        False,
        id="mssql_same_host_diff_port",
    ),
]

SYNAPSE_JOIN_CASES = [
    pytest.param(
        lambda: SynapseClientConfiguration(
            credentials=SynapseCredentials("mssql://u:p@h:1433/db1")
        ),
        lambda: SynapseClientConfiguration(
            credentials=SynapseCredentials("mssql://u:p@h:1433/db2")
        ),
        True,
        id="synapse_same_host_diff_db",
    ),
    pytest.param(
        lambda: SynapseClientConfiguration(credentials=SynapseCredentials("mssql://h:1433/db")),
        lambda: SynapseClientConfiguration(credentials=SynapseCredentials("mssql://h:1434/db")),
        False,
        id="synapse_same_host_diff_port",
    ),
]

CLICKHOUSE_JOIN_CASES = [
    pytest.param(
        lambda: ClickHouseClientConfiguration(
            credentials=ClickHouseCredentials("clickhouse://u:p@h/db1")
        ),
        lambda: ClickHouseClientConfiguration(
            credentials=ClickHouseCredentials("clickhouse://u:p@h/db2")
        ),
        True,
        id="ch_same_host_diff_db",
    ),
    pytest.param(
        lambda: ClickHouseClientConfiguration(credentials=ClickHouseCredentials("clickhouse://h1")),
        lambda: ClickHouseClientConfiguration(credentials=ClickHouseCredentials("clickhouse://h2")),
        False,
        id="ch_diff_host",
    ),
    pytest.param(
        lambda: ClickHouseClientConfiguration(
            credentials=ClickHouseCredentials("clickhouse://h:9440/db")
        ),
        lambda: ClickHouseClientConfiguration(
            credentials=ClickHouseCredentials("clickhouse://h:9000/db")
        ),
        False,
        id="ch_same_host_diff_port",
    ),
]

DREMIO_JOIN_CASES = [
    pytest.param(
        lambda: DremioClientConfiguration(credentials=DremioCredentials("grpc://h")),
        lambda: DremioClientConfiguration(credentials=DremioCredentials("grpc://h")),
        True,
        id="dremio_same_host",
    ),
    pytest.param(
        lambda: DremioClientConfiguration(credentials=DremioCredentials("grpc://h:32010")),
        lambda: DremioClientConfiguration(credentials=DremioCredentials("grpc://h:32011")),
        False,
        id="dremio_same_host_diff_port",
    ),
]

FABRIC_JOIN_CASES = [
    pytest.param(
        lambda: FabricClientConfiguration(credentials=_fabric_creds("h", "db1")),
        lambda: FabricClientConfiguration(credentials=_fabric_creds("h", "db2")),
        True,
        id="fabric_same_host_diff_db",
    ),
]

POSTGRES_JOIN_CASES = [
    pytest.param(
        lambda: PostgresClientConfiguration(
            credentials=PostgresCredentials("postgresql://u:p@h:5432/db")
        ),
        lambda: PostgresClientConfiguration(
            credentials=PostgresCredentials("postgresql://u:p@h:5432/db")
        ),
        True,
        id="pg_same_host_db",
    ),
    pytest.param(
        lambda: PostgresClientConfiguration(
            credentials=PostgresCredentials("postgresql://u:p@h:5432/db1")
        ),
        lambda: PostgresClientConfiguration(
            credentials=PostgresCredentials("postgresql://u:p@h:5432/db2")
        ),
        False,
        id="pg_same_host_diff_db",
    ),
    pytest.param(
        lambda: PostgresClientConfiguration(
            credentials=PostgresCredentials("postgresql://u:p@h1:5432/db")
        ),
        lambda: PostgresClientConfiguration(
            credentials=PostgresCredentials("postgresql://u:p@h2:5432/db")
        ),
        False,
        id="pg_diff_host",
    ),
]

REDSHIFT_JOIN_CASES = [
    pytest.param(
        lambda: RedshiftClientConfiguration(
            credentials=RedshiftCredentials("redshift://u:p@h:5439/db")
        ),
        lambda: RedshiftClientConfiguration(
            credentials=RedshiftCredentials("redshift://u:p@h:5439/db")
        ),
        True,
        id="rs_same_host_db",
    ),
    pytest.param(
        lambda: RedshiftClientConfiguration(
            credentials=RedshiftCredentials("redshift://u:p@h:5439/db1")
        ),
        lambda: RedshiftClientConfiguration(
            credentials=RedshiftCredentials("redshift://u:p@h:5439/db2")
        ),
        False,
        id="rs_same_host_diff_db",
    ),
]

DUCKDB_JOIN_CASES = [
    pytest.param(
        lambda: DuckDbClientConfiguration(credentials=DuckDbCredentials("/p/db.duckdb")),
        lambda: DuckDbClientConfiguration(credentials=DuckDbCredentials("/p/db.duckdb")),
        True,
        id="duckdb_same_path",
    ),
    pytest.param(
        lambda: DuckDbClientConfiguration(credentials=DuckDbCredentials("/p/db1.duckdb")),
        lambda: DuckDbClientConfiguration(credentials=DuckDbCredentials("/p/db2.duckdb")),
        False,
        id="duckdb_diff_path",
    ),
]

DUCKLAKE_JOIN_CASES = [
    pytest.param(
        lambda: DuckLakeClientConfiguration(
            credentials=_ducklake_creds("sqlite:///cat.sqlite", "lake1")
        ),
        lambda: DuckLakeClientConfiguration(
            credentials=_ducklake_creds("sqlite:///cat.sqlite", "lake1")
        ),
        True,
        id="dl_same_cat_name",
    ),
    pytest.param(
        lambda: DuckLakeClientConfiguration(
            credentials=_ducklake_creds("sqlite:///cat.sqlite", "lake1")
        ),
        lambda: DuckLakeClientConfiguration(
            credentials=_ducklake_creds("sqlite:///cat.sqlite", "lake2")
        ),
        False,
        id="dl_same_cat_diff_name",
    ),
]

SNOWFLAKE_JOIN_CASES = [
    pytest.param(
        lambda: SnowflakeClientConfiguration(
            credentials=SnowflakeCredentials("snowflake://u:p@a.snowflake.com/db1")
        ),
        lambda: SnowflakeClientConfiguration(
            credentials=SnowflakeCredentials("snowflake://u:p@a.snowflake.com/db2")
        ),
        True,
        id="sf_same_account",
    ),
    pytest.param(
        lambda: SnowflakeClientConfiguration(
            credentials=SnowflakeCredentials("snowflake://u:p@a1.snowflake.com/db")
        ),
        lambda: SnowflakeClientConfiguration(
            credentials=SnowflakeCredentials("snowflake://u:p@a2.snowflake.com/db")
        ),
        False,
        id="sf_diff_account",
    ),
]

BIGQUERY_JOIN_CASES = [
    pytest.param(
        lambda: BigQueryClientConfiguration(
            credentials=GcpServiceAccountCredentials(project_id="proj")
        ),
        lambda: BigQueryClientConfiguration(
            credentials=GcpServiceAccountCredentials(project_id="proj")
        ),
        True,
        id="bq_same_project",
    ),
    pytest.param(
        lambda: BigQueryClientConfiguration(
            credentials=GcpServiceAccountCredentials(project_id="p1")
        ),
        lambda: BigQueryClientConfiguration(
            credentials=GcpServiceAccountCredentials(project_id="p2")
        ),
        False,
        id="bq_diff_project",
    ),
]

DATABRICKS_JOIN_CASES = [
    pytest.param(
        lambda: DatabricksClientConfiguration(
            credentials=DatabricksCredentials(server_hostname="w.databricks.com")
        ),
        lambda: DatabricksClientConfiguration(
            credentials=DatabricksCredentials(server_hostname="w.databricks.com")
        ),
        True,
        id="dbr_same_server",
    ),
]

# Athena Glue catalogs are regional, so physical identity includes region and catalog.
ATHENA_JOIN_CASES = [
    pytest.param(
        lambda: _athena_config("us-west-2", "cat"),
        lambda: _athena_config("us-west-2", "cat"),
        True,
        id="athena_same_region_catalog",
    ),
    pytest.param(
        lambda: _athena_config("us-west-2", "cat"),
        lambda: _athena_config("eu-central-1", "cat"),
        False,
        id="athena_diff_region",
    ),
    pytest.param(
        lambda: _athena_config("us-west-2", "c1"),
        lambda: _athena_config("us-west-2", "c2"),
        False,
        id="athena_diff_catalog",
    ),
    pytest.param(
        lambda: AthenaClientConfiguration(
            credentials=AwsCredentials(),
            aws_data_catalog="cat",
        ),
        lambda: AthenaClientConfiguration(
            credentials=AwsCredentials(),
            aws_data_catalog="cat",
        ),
        False,
        id="athena_no_region",
    ),
]

CAN_JOIN_WITH_CASES = (
    POSTGRES_JOIN_CASES
    + REDSHIFT_JOIN_CASES
    + MSSQL_JOIN_CASES
    + SYNAPSE_JOIN_CASES
    + CLICKHOUSE_JOIN_CASES
    + DREMIO_JOIN_CASES
    + FABRIC_JOIN_CASES
    + SNOWFLAKE_JOIN_CASES
    + BIGQUERY_JOIN_CASES
    + DATABRICKS_JOIN_CASES
    + ATHENA_JOIN_CASES
    + DUCKDB_JOIN_CASES
    + DUCKLAKE_JOIN_CASES
)


@pytest.mark.parametrize("f1,f2,expected", CAN_JOIN_WITH_CASES)
def test_can_join_with_matrix(f1: ConfigFactory, f2: ConfigFactory, expected: bool) -> None:
    c1, c2 = f1(), f2()
    assert_join_result(c1, c2, expected)


# Cross-type rejection


@pytest.mark.parametrize(
    "f1,f2",
    [
        pytest.param(
            lambda: PostgresClientConfiguration(credentials=PostgresCredentials("postgresql://h")),
            lambda: _PhysicalDestinationConfig("h:5432"),
            id="pg_vs_base",
        ),
        pytest.param(
            lambda: PostgresClientConfiguration(credentials=PostgresCredentials("postgresql://h")),
            lambda: MsSqlClientConfiguration(credentials=MsSqlCredentials("mssql://h")),
            id="pg_vs_mssql",
        ),
        pytest.param(
            lambda: SnowflakeClientConfiguration(
                credentials=SnowflakeCredentials("snowflake://u:p@h/db")
            ),
            lambda: ClickHouseClientConfiguration(
                credentials=ClickHouseCredentials("clickhouse://h")
            ),
            id="default_same_identity_different_type",
        ),
        pytest.param(
            lambda: PostgresClientConfiguration(
                credentials=PostgresCredentials("postgresql://u:p@h:5439/db")
            ),
            lambda: RedshiftClientConfiguration(
                credentials=RedshiftCredentials("redshift://u:p@h:5439/db")
            ),
            id="postgres_vs_redshift_same_identity",
        ),
        pytest.param(
            lambda: MsSqlClientConfiguration(credentials=MsSqlCredentials("mssql://h")),
            lambda: SynapseClientConfiguration(credentials=SynapseCredentials("mssql://h")),
            id="mssql_vs_synapse_same_identity",
        ),
    ],
)
def test_cross_type_rejection(f1: ConfigFactory, f2: ConfigFactory) -> None:
    c1, c2 = f1(), f2()
    if isinstance(c2, _PhysicalDestinationConfig):
        c2._physical_destination = c1.physical_destination()
    assert_not_joinable(c1, c2)


def test_cross_type_different_physical_destinations() -> None:
    sf = SnowflakeClientConfiguration(
        credentials=SnowflakeCredentials("snowflake://u:p@a1.snowflake.com/db")
    )
    bq = BigQueryClientConfiguration(credentials=GcpServiceAccountCredentials(project_id="p2"))
    assert sf.physical_destination() != bq.physical_destination()
    assert_not_joinable(sf, bq)


# Filesystem special cases


def test_filesystem_joinability_is_engine_based_not_location_based() -> None:
    c1 = FilesystemDestinationClientConfiguration(bucket_url="s3://b1/p")
    c2 = FilesystemDestinationClientConfiguration(bucket_url="s3://b2/p")
    c3 = FilesystemDestinationClientConfiguration(bucket_url="/local/p")
    c4 = FilesystemDestinationClientConfiguration(bucket_url="gs://b/p")
    assert_joinable(c1, c2)
    assert_joinable(c1, c3)
    assert_joinable(c1, c4)


def test_filesystem_cannot_join_with_non_filesystem() -> None:
    c = FilesystemDestinationClientConfiguration(bucket_url="s3://b/p")
    other = _PhysicalDestinationConfig("s3://b")
    assert_not_joinable(c, other)


def test_filesystem_fingerprint_empty_for_local() -> None:
    c = FilesystemDestinationClientConfiguration(bucket_url="/local/p")
    assert c.physical_destination() == ""
    assert c.fingerprint() == ""


# MotherDuck token-based joinability


def test_motherduck_token_not_exposed_as_physical_destination() -> None:
    md = MotherDuckClientConfiguration(
        credentials=MotherDuckCredentials("md:db?motherduck_token=token")
    )
    assert md.physical_destination() == ""


def test_motherduck_fingerprint_hashes_token() -> None:
    md = MotherDuckClientConfiguration(
        credentials=MotherDuckCredentials("md:db?motherduck_token=token")
    )
    assert md.fingerprint() == digest128("token")


def test_motherduck_can_join_with_same_token_without_exposing_location() -> None:
    """Same token can join without exposing token via physical destination."""
    c1 = MotherDuckClientConfiguration(
        credentials=MotherDuckCredentials("md:db?motherduck_token=token")
    )
    c2 = MotherDuckClientConfiguration(
        credentials=MotherDuckCredentials("md:db?motherduck_token=token")
    )
    assert_joinable(c1, c2)


def test_motherduck_different_tokens_are_not_proven_joinable() -> None:
    """Different tokens are treated as not joinable."""
    c1 = MotherDuckClientConfiguration(
        credentials=MotherDuckCredentials("md:db?motherduck_token=token1")
    )
    c2 = MotherDuckClientConfiguration(
        credentials=MotherDuckCredentials("md:db?motherduck_token=token2")
    )
    # Tokens may belong to the same account, but we do not have a safe account id to prove it.
    assert_not_joinable(c1, c2)


def test_motherduck_can_join_with_missing_token() -> None:
    """Missing token cannot join."""
    with_token = MotherDuckClientConfiguration(
        credentials=MotherDuckCredentials("md:db?motherduck_token=token")
    )
    without_token = MotherDuckClientConfiguration(credentials=MotherDuckCredentials("md:db"))
    assert_not_joinable(with_token, without_token)
    w1 = MotherDuckClientConfiguration(credentials=MotherDuckCredentials("md:db1"))
    w2 = MotherDuckClientConfiguration(credentials=MotherDuckCredentials("md:db2"))
    assert_not_joinable(w1, w2)


def test_motherduck_can_join_with_non_motherduck() -> None:
    """MotherDuck cannot join with other destination types."""
    md = MotherDuckClientConfiguration(
        credentials=MotherDuckCredentials("md:db?motherduck_token=token")
    )
    pg = PostgresClientConfiguration(credentials=PostgresCredentials("postgresql://h"))
    assert_not_joinable(md, pg)


# SQLAlchemy dialect-specific cases


SQLA_CASES = [
    pytest.param("postgresql://u@h:5432/db", "postgresql://u@h:5432/db", True, id="pg_same"),
    pytest.param("postgresql://u@h:5432/db1", "postgresql://u@h:5432/db2", False, id="pg_diff_db"),
    pytest.param(
        "postgresql://u@h1:5432/db", "postgresql://u@h2:5432/db", False, id="pg_diff_host"
    ),
    pytest.param("mysql://u@h:3306/db1", "mysql://u@h:3306/db2", True, id="mysql_same_host"),
    pytest.param("mysql://u@h1:3306/db", "mysql://u@h2:3306/db", False, id="mysql_diff_host"),
    pytest.param("sqlite:////p/db.sqlite", "sqlite:////p/db.sqlite", True, id="sqlite_same"),
    pytest.param("sqlite:////p/db1.sqlite", "sqlite:////p/db2.sqlite", False, id="sqlite_diff"),
    pytest.param("postgresql://u@h:5432/db", "mysql://u@h:3306/db", False, id="diff_dialects"),
    pytest.param("unknown://u@h:1234/db", "unknown://u@h:1234/db", True, id="unknown_same"),
    pytest.param("unknown://u@h:1234/db1", "unknown://u@h:1234/db2", False, id="unknown_diff_db"),
]


@pytest.mark.parametrize("conn1,conn2,expected", SQLA_CASES)
def test_sqlalchemy_can_join_with(conn1: str, conn2: str, expected: bool) -> None:
    c1 = _sqla_config(conn1)
    c2 = _sqla_config(conn2)
    assert_join_result(c1, c2, expected)


@pytest.mark.parametrize(
    "f1,f2,expected",
    [
        pytest.param(
            lambda: _lancedb_config("/tmp/db.lancedb"),
            lambda: _lancedb_config("/tmp/db.lancedb"),
            True,
            id="same_uri_dataset_separator",
        ),
        pytest.param(
            lambda: _lancedb_config("/tmp/db1.lancedb"),
            lambda: _lancedb_config("/tmp/db2.lancedb"),
            False,
            id="different_uri",
        ),
        pytest.param(
            lambda: _lancedb_config("/tmp/db.lancedb", dataset_name="dataset1"),
            lambda: _lancedb_config("/tmp/db.lancedb", dataset_name="dataset2"),
            True,
            id="different_dataset_same_uri",
        ),
        pytest.param(
            lambda: _lancedb_config("/tmp/db.lancedb", dataset_separator="___"),
            lambda: _lancedb_config("/tmp/db.lancedb", dataset_separator="__"),
            False,
            id="different_separator",
        ),
        pytest.param(
            lambda: _lancedb_config(":external:"),
            lambda: _lancedb_config(":external:"),
            False,
            id="external_native_client",
        ),
    ],
)
def test_lancedb_can_join_with(f1: ConfigFactory, f2: ConfigFactory, expected: bool) -> None:
    assert_join_result(f1(), f2(), expected)


@pytest.mark.parametrize(
    "f1,f2,expected",
    [
        pytest.param(
            lambda: _lance_config("file:///tmp/lance"),
            lambda: _lance_config("file:///tmp/lance"),
            True,
            id="same_catalog_dataset",
        ),
        pytest.param(
            lambda: _lance_config("file:///tmp/lance1"),
            lambda: _lance_config("file:///tmp/lance2"),
            False,
            id="different_catalog",
        ),
        pytest.param(
            lambda: _lance_config("file:///tmp/lance", dataset_name="dataset1"),
            lambda: _lance_config("file:///tmp/lance", dataset_name="dataset2"),
            False,
            id="different_dataset",
        ),
    ],
)
def test_lance_can_join_with(f1: ConfigFactory, f2: ConfigFactory, expected: bool) -> None:
    assert_join_result(f1(), f2(), expected)


def test_lance_and_lancedb_cannot_join_with_each_other() -> None:
    lance = _lance_config("file:///tmp/lance")
    lancedb = _lancedb_config("file:///tmp/lance")
    assert_not_joinable(lance, lancedb)


def test_weaviate_physical_destination_but_not_joinable() -> None:
    c1 = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="https://cluster.weaviate.cloud")
    )
    c2 = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="https://cluster.weaviate.cloud")
    )
    assert c1.physical_destination() == "cluster.weaviate.cloud"
    assert c1.fingerprint() == digest128("cluster.weaviate.cloud")
    assert_not_joinable(c1, c2)


def test_qdrant_physical_destination_but_not_joinable() -> None:
    c1 = QdrantClientConfiguration(qd_location="https://cluster.qdrant.io")
    c2 = QdrantClientConfiguration(qd_location="https://cluster.qdrant.io")
    assert c1.physical_destination() == "https://cluster.qdrant.io"
    assert c1.fingerprint() == digest128("https://cluster.qdrant.io")
    assert_not_joinable(c1, c2)
