"""Tests for destination configuration-level join-compatibility semantics."""

import os
from typing import Callable, cast, Optional, Union

from typing_extensions import TypeAlias

import duckdb
import pytest

from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration.specs import (
    AwsCredentials,
    ConnectionStringCredentials,
    GcpServiceAccountCredentials,
)
from dlt.common.destination.client import DestinationClientConfiguration
from dlt.destinations.impl.destination.configuration import CustomDestinationClientConfiguration
from dlt.destinations.impl.dummy.configuration import DummyClientConfiguration
from dlt.common.runtime.run_context import active
from dlt.common.storages import FilesystemConfigurationWithLocalFiles
from dlt.common.utils import digest128
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
    DEFAULT_LANCE_NAMESPACE_NAME,
    DirectoryCatalogCredentials,
    LanceClientConfiguration,
    LanceStorageConfiguration,
    RestCatalogCredentials,
)
from dlt.destinations.impl.qdrant.configuration import QdrantClientConfiguration
from dlt.destinations.impl.weaviate.configuration import (
    WeaviateClientConfiguration,
    WeaviateCredentials,
)


ConfigFactory: TypeAlias = Callable[[], DestinationClientConfiguration]
ExpectedLocation: TypeAlias = Union[str, Callable[[], str]]


class _PhysicalDestinationConfig(DestinationClientConfiguration):
    def __init__(self, data_location: str = "", display_value: Optional[str] = None) -> None:
        super().__init__()
        self._data_location = data_location
        self._display_value = display_value

    def data_location(self) -> str:
        return self._data_location

    def __str__(self) -> str:
        if self._display_value is not None:
            return self._display_value
        return super().__str__()


def assert_joinable(
    config1: DestinationClientConfiguration, config2: DestinationClientConfiguration
) -> None:
    assert config1.can_read_from(config2)
    assert config2.can_read_from(config1)


def assert_not_joinable(
    config1: DestinationClientConfiguration, config2: DestinationClientConfiguration
) -> None:
    assert not config1.can_read_from(config2)
    assert not config2.can_read_from(config1)


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


def _ducklake_creds(
    catalog_str: str,
    name: str = DEFAULT_DUCKLAKE_NAME,
    storage_url: Optional[str] = None,
    metadata_schema: Optional[str] = None,
) -> DuckLakeCredentials:
    """Build DuckLake credentials."""
    return DuckLakeCredentials(
        ducklake_name=name,
        metadata_schema=metadata_schema,
        catalog=ConnectionStringCredentials(catalog_str),
        storage=(
            FilesystemConfigurationWithLocalFiles(bucket_url=storage_url) if storage_url else None
        ),
    )


def _fabric_creds(host: str, database: str, port: Optional[int] = None) -> FabricCredentials:
    """Build Fabric credentials."""
    # Fabric is normally configured via structured fields, not a connection string.
    credentials = FabricCredentials()
    credentials.host = host
    credentials.database = database
    if port is not None:
        credentials.port = port
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
    credentials = DirectoryCatalogCredentials(bucket_url=catalog_root)
    c = LanceClientConfiguration(
        credentials=credentials,
        storage=LanceStorageConfiguration(bucket_url=catalog_root),
    )
    c._bind_dataset_name(dataset_name)
    credentials.bucket_url = catalog_root
    return c


def _lance_rest_config(
    uri: Optional[str], dataset_name: str = "dataset"
) -> LanceClientConfiguration:
    """Build Lance config with REST namespace catalog."""
    c = LanceClientConfiguration(
        catalog_type="rest",
        credentials=RestCatalogCredentials(uri=uri),
    )
    c._bind_dataset_name(dataset_name)
    return c


def _lance_multi_base_config(
    catalog_root: Optional[str], storage_root: str, dataset_name: str = "dataset"
) -> LanceClientConfiguration:
    """Build Lance config with manifest catalog and data storage in separate locations."""
    c = LanceClientConfiguration(
        credentials=DirectoryCatalogCredentials(bucket_url=catalog_root) if catalog_root else None,
        storage=LanceStorageConfiguration(bucket_url=storage_root),
    )
    c._bind_dataset_name(dataset_name)
    return c


def test_base_can_read_from_default_false_when_data_locations_differ() -> None:
    config1 = _PhysicalDestinationConfig("host1")
    config2 = _PhysicalDestinationConfig("host2")
    assert_not_joinable(config1, config2)


def test_base_can_read_from_default_true_when_same_data_location() -> None:
    config1 = _PhysicalDestinationConfig("host1")
    config2 = _PhysicalDestinationConfig("host1")
    assert_joinable(config1, config2)


def test_destination_without_data_location_accesses_no_data() -> None:
    """`None` is not a data location, so it never matches. Not even another `None` matches.
    A sink is inaccessible for this reason, and no such destination overrides `can_read_from`."""
    sink = DummyClientConfiguration()
    assert sink.data_location() is None
    assert sink.can_read_from(DummyClientConfiguration()) is False
    assert sink.can_read_from(sink) is False
    assert sink.can_write_from(sink) is False

    custom = CustomDestinationClientConfiguration()
    assert custom.data_location() is None
    assert custom.can_read_from(CustomDestinationClientConfiguration()) is False
    assert not sink.can_read_from(_PhysicalDestinationConfig("host1"))


def test_base_can_read_from_returns_false_for_non_config() -> None:
    config = _PhysicalDestinationConfig("host1")
    assert not config.can_read_from("not a config")  # type: ignore[arg-type]
    assert not config.can_read_from(None)
    assert not config.can_read_from(42)  # type: ignore[arg-type]


def test_is_same_location_ignores_the_credentials_display() -> None:
    """Two configs that name one place are the same place, even when their text differs."""
    config1 = _PhysicalDestinationConfig("host1", "first-display")
    config2 = _PhysicalDestinationConfig("host1", "second-display")
    assert str(config1) != str(config2)

    assert config1.is_same_location(config2)
    assert config1.can_read_from(config2)
    assert not config1.is_same_location(_PhysicalDestinationConfig("host2"))


# data_location() extraction across destinations

PHYSICAL_DEST_CASES = [
    # Postgres: host:port format
    pytest.param(
        lambda: PostgresClientConfiguration(
            credentials=PostgresCredentials("postgresql://u:p@h:5432/db")
        ),
        "h:5432/db",
        id="pg_explicit_port",
    ),
    pytest.param(
        lambda: PostgresClientConfiguration(credentials=PostgresCredentials("postgresql://h/db")),
        "h:5432/db",
        id="pg_default_port",
    ),
    # Redshift
    pytest.param(
        lambda: RedshiftClientConfiguration(
            credentials=RedshiftCredentials("redshift://u:p@h:5439/db")
        ),
        "h:5439/db",
        id="rs_explicit_port",
    ),
    pytest.param(
        lambda: RedshiftClientConfiguration(credentials=RedshiftCredentials("redshift://h/db")),
        "h:5439/db",
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
    # BigQuery: joinability is determined by location
    pytest.param(
        lambda: BigQueryClientConfiguration(
            credentials=GcpServiceAccountCredentials(project_id="cred-proj"),
            project_id="cfg-proj",
            location="EU",
        ),
        "EU",
        id="bq_config_location",
    ),
    pytest.param(
        lambda: BigQueryClientConfiguration(
            credentials=GcpServiceAccountCredentials(project_id="cred-proj")
        ),
        "US",
        id="bq_default_location",
    ),
    # MSSQL / Synapse
    pytest.param(
        lambda: MsSqlClientConfiguration(credentials=MsSqlCredentials("mssql://h")),
        "h:1433",
        id="mssql_host",
    ),
    pytest.param(
        lambda: SynapseClientConfiguration(credentials=SynapseCredentials("mssql://h/db")),
        "h:1433/db",
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
    # no region available: fall back to the catalog so same-catalog datasets stay joinable
    pytest.param(
        lambda: AthenaClientConfiguration(
            credentials=AwsCredentials(),
            aws_data_catalog="cat",
        ),
        "cat",
        id="athena_no_region",
    ),
    pytest.param(
        lambda: _athena_config("eu-central-1"),
        "eu-central-1/awsdatacatalog",
        id="athena_default_catalog",
    ),
    # catalog names are case-insensitive, AWS docs spell the default `AwsDataCatalog`
    pytest.param(
        lambda: _athena_config("eu-central-1", "AwsDataCatalog"),
        "eu-central-1/awsdatacatalog",
        id="athena_catalog_casefolded",
    ),
    # Dremio
    pytest.param(
        lambda: DremioClientConfiguration(credentials=DremioCredentials("grpc://h")),
        "h:32010",
        id="dremio_host",
    ),
    # DuckDB
    pytest.param(
        lambda: DuckDbClientConfiguration(credentials=DuckDbCredentials("p/db.duckdb")),
        "p/db.duckdb",
        id="duckdb_path",
    ),
    pytest.param(
        lambda: FilesystemDestinationClientConfiguration(bucket_url="s3://b/p"),
        "s3://b",
        id="fs_remote",
    ),
    pytest.param(
        lambda: FilesystemDestinationClientConfiguration(bucket_url="local/p"),
        lambda: os.path.join(os.path.abspath(active().local_dir), "local", "p"),
        id="fs_local",
    ),
    # DuckLake: sql catalogs host one lake per metadata schema (defaults to ducklake name)
    pytest.param(
        lambda: DuckLakeClientConfiguration(
            credentials=_ducklake_creds("postgresql://u@h:5432/db", "lake")
        ),
        "postgres://h:5432/db#lake",
        id="dl_remote_cat",
    ),
    pytest.param(
        lambda: DuckLakeClientConfiguration(credentials=_ducklake_creds("postgres://u@h:5432/db")),
        f"postgres://h:5432/db#{DEFAULT_DUCKLAKE_NAME}",
        id="dl_remote_cat_default_name",
    ),
    pytest.param(
        lambda: DuckLakeClientConfiguration(
            credentials=_ducklake_creds("postgres://u@h:5432/db", "lake", metadata_schema="meta")
        ),
        "postgres://h:5432/db#meta",
        id="dl_remote_cat_explicit_metadata_schema",
    ),
    # DuckLake: file catalogs are the lake themselves, attach name is just an alias
    pytest.param(
        lambda: DuckLakeClientConfiguration(
            credentials=_ducklake_creds("sqlite:///cat.sqlite", "lake")
        ),
        "sqlite://cat.sqlite",
        id="dl_local_cat",
    ),
    pytest.param(
        lambda: DuckLakeClientConfiguration(credentials=_ducklake_creds("md:///md_db", "lake")),
        # no credential-free identity, so dlt digests the native catalog, never a blank location
        lambda: "md://"
        + digest128(
            f"{_ducklake_creds('md:///md_db', 'lake').catalog.to_native_representation()}#lake"
        ),
        id="dl_md_cat_digest",
    ),
    # Fabric
    pytest.param(
        lambda: FabricClientConfiguration(
            credentials=_fabric_creds("h.fabric.microsoft.com", "db", port=1433)
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
        lambda: f"md://{digest128('token')}",
        id="md_token_digest",
    ),
]


@pytest.mark.parametrize("factory,expected", PHYSICAL_DEST_CASES)
def test_data_location(factory: ConfigFactory, expected: ExpectedLocation) -> None:
    if callable(expected):
        expected = expected()
    assert factory().data_location() == expected


# a config that cannot name its location raises an error. It does not report a blank
# location. A blank location compares equal to the next blank one, which reads as
# "these two are the same place"

NO_LOCATION_DEST_CASES = [
    pytest.param(
        lambda: PostgresClientConfiguration(credentials=PostgresCredentials()), id="pg_no_host"
    ),
    pytest.param(
        lambda: PostgresClientConfiguration(credentials=PostgresCredentials("postgresql://h")),
        id="pg_no_database",
    ),
    pytest.param(
        lambda: SnowflakeClientConfiguration(credentials=SnowflakeCredentials()),
        id="sf_no_host",
    ),
    pytest.param(lambda: BigQueryClientConfiguration(location=""), id="bq_no_location"),
    pytest.param(
        lambda: MsSqlClientConfiguration(credentials=MsSqlCredentials()), id="mssql_no_host"
    ),
    pytest.param(
        lambda: FabricClientConfiguration(credentials=FabricCredentials()),
        id="fabric_no_host",
    ),
]


@pytest.mark.parametrize("factory", NO_LOCATION_DEST_CASES)
def test_data_location_raises_when_not_configured(factory: ConfigFactory) -> None:
    with pytest.raises(ConfigurationValueError, match="cannot determine the data location"):
        factory().data_location()


# can_read_from() matrices (symmetric)

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
        lambda: SynapseClientConfiguration(credentials=SynapseCredentials("mssql://u:p@h:1433/db")),
        lambda: SynapseClientConfiguration(credentials=SynapseCredentials("mssql://u:p@h:1433/db")),
        True,
        id="synapse_same_host_same_db",
    ),
    # Synapse dedicated SQL pools do not support cross-database queries, unlike SQL Server
    pytest.param(
        lambda: SynapseClientConfiguration(
            credentials=SynapseCredentials("mssql://u:p@h:1433/db1")
        ),
        lambda: SynapseClientConfiguration(
            credentials=SynapseCredentials("mssql://u:p@h:1433/db2")
        ),
        False,
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
        lambda: DuckDbClientConfiguration(credentials=DuckDbCredentials("p/db.duckdb")),
        lambda: DuckDbClientConfiguration(credentials=DuckDbCredentials("p/db.duckdb")),
        True,
        id="duckdb_same_path",
    ),
    pytest.param(
        # duckdb attaches another database file, so the query accesses it
        lambda: DuckDbClientConfiguration(credentials=DuckDbCredentials("p/db1.duckdb")),
        lambda: DuckDbClientConfiguration(credentials=DuckDbCredentials("p/db2.duckdb")),
        True,
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
    # the file is the lake, attach name does not matter
    pytest.param(
        lambda: DuckLakeClientConfiguration(
            credentials=_ducklake_creds("sqlite:///cat.sqlite", "lake1")
        ),
        lambda: DuckLakeClientConfiguration(
            credentials=_ducklake_creds("sqlite:///cat.sqlite", "lake2")
        ),
        True,
        id="dl_file_cat_diff_name",
    ),
    # sql catalogs: different name means different metadata schema, so a different lake
    pytest.param(
        lambda: DuckLakeClientConfiguration(
            credentials=_ducklake_creds("postgres://u@h:5432/db", "lake1")
        ),
        lambda: DuckLakeClientConfiguration(
            credentials=_ducklake_creds("postgres://u@h:5432/db", "lake2")
        ),
        True,
        id="dl_sql_cat_diff_name",
    ),
    # sql catalogs: explicit metadata schema overrides the name
    pytest.param(
        lambda: DuckLakeClientConfiguration(
            credentials=_ducklake_creds("postgres://u@h:5432/db", "lake1", metadata_schema="meta")
        ),
        lambda: DuckLakeClientConfiguration(
            credentials=_ducklake_creds("postgres://u@h:5432/db", "lake2", metadata_schema="meta")
        ),
        True,
        id="dl_sql_cat_same_metadata_schema",
    ),
    pytest.param(
        lambda: DuckLakeClientConfiguration(
            credentials=_ducklake_creds("postgres://u@h:5432/db", "lake", metadata_schema="meta1")
        ),
        lambda: DuckLakeClientConfiguration(
            credentials=_ducklake_creds("postgres://u@h:5432/db", "lake", metadata_schema="meta2")
        ),
        True,
        id="dl_sql_cat_diff_metadata_schema",
    ),
    # md catalogs have no non-secret identity
    pytest.param(
        lambda: DuckLakeClientConfiguration(credentials=_ducklake_creds("md:///md_db", "lake")),
        lambda: DuckLakeClientConfiguration(credentials=_ducklake_creds("md:///md_db", "lake")),
        True,
        id="dl_md_cat_joinable",
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
            credentials=GcpServiceAccountCredentials(project_id="p1"),
            location="EU",
        ),
        lambda: BigQueryClientConfiguration(
            credentials=GcpServiceAccountCredentials(project_id="p2"),
            location="EU",
        ),
        True,
        id="bq_same_location",
    ),
    pytest.param(
        lambda: BigQueryClientConfiguration(
            credentials=GcpServiceAccountCredentials(project_id="proj"),
            location="US",
        ),
        lambda: BigQueryClientConfiguration(
            credentials=GcpServiceAccountCredentials(project_id="proj"),
            location="EU",
        ),
        False,
        id="bq_diff_location",
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
    # catalog names are case-insensitive
    pytest.param(
        lambda: _athena_config("us-west-2", "AwsDataCatalog"),
        lambda: _athena_config("us-west-2", "awsdatacatalog"),
        True,
        id="athena_catalog_case_insensitive",
    ),
    # the same catalog, and no region on either side: dlt treats this as one data location
    pytest.param(
        lambda: AthenaClientConfiguration(
            credentials=AwsCredentials(),
            aws_data_catalog="cat",
        ),
        lambda: AthenaClientConfiguration(
            credentials=AwsCredentials(),
            aws_data_catalog="cat",
        ),
        True,
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
def test_can_read_from_matrix(f1: ConfigFactory, f2: ConfigFactory, expected: bool) -> None:
    c1, c2 = f1(), f2()
    assert_join_result(c1, c2, expected)


@pytest.mark.parametrize(
    "f1,f2",
    [
        pytest.param(
            lambda: PostgresClientConfiguration(
                credentials=PostgresCredentials("postgresql://h/db")
            ),
            lambda: _PhysicalDestinationConfig("h:5432/db"),
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
        c2._data_location = c1.data_location()
    assert_not_joinable(c1, c2)


def test_cross_type_different_data_locations() -> None:
    sf = SnowflakeClientConfiguration(
        credentials=SnowflakeCredentials("snowflake://u:p@a1.snowflake.com/db")
    )
    bq = BigQueryClientConfiguration(location="US")
    assert sf.data_location() != bq.data_location()
    assert_not_joinable(sf, bq)


# a duckdb engine attaches any other filesystem location, so all of these join
@pytest.mark.parametrize(
    "url1,url2,expected",
    [
        pytest.param("s3://b/p1", "s3://b/p2", True, id="same_bucket_different_prefix"),
        pytest.param("s3://b1/p", "s3://b2/p", True, id="different_bucket"),
        pytest.param("s3://b/p", "az://b/p", True, id="different_scheme_same_bucket"),
        pytest.param("/local/p", "/local/p", True, id="same_local_path"),
        pytest.param("/local/p1", "/local/p2", True, id="different_local_path"),
        pytest.param("s3://b/p", "/local/p", True, id="remote_vs_local"),
    ],
)
def test_filesystem_can_read_from_any_duckdb_engine(url1: str, url2: str, expected: bool) -> None:
    c1 = FilesystemDestinationClientConfiguration(bucket_url=url1)
    c2 = FilesystemDestinationClientConfiguration(bucket_url=url2)
    assert_join_result(c1, c2, expected)


@pytest.mark.parametrize(
    "bucket_url,attachable",
    [
        pytest.param("/local/p", True, id="file"),
        pytest.param("s3://b/p", True, id="s3"),
        pytest.param("az://c/p", True, id="az"),
        pytest.param("abfss://c@a.dfs.core.windows.net/p", True, id="abfss"),
        pytest.param("hf://datasets/d", True, id="hf"),
        pytest.param("gs://b/p", False, id="gs"),
        pytest.param("gcs://b/p", False, id="gcs"),
        pytest.param("memory://m", False, id="memory"),
        pytest.param("sftp://h/p", False, id="sftp"),
        pytest.param("gdrive://f", False, id="gdrive"),
        pytest.param("abfs://c/p", False, id="abfs"),
        pytest.param("adl://c/p", False, id="adl"),
        pytest.param("azure://c/p", False, id="azure"),
    ],
)
def test_filesystem_attachable_only_for_protocols_with_duckdb_secrets(
    bucket_url: str, attachable: bool
) -> None:
    """duckdb has no `CREATE SECRET` for some protocols. Only this process registers such a
    protocol with fsspec, so dlt never tells a foreign engine that it can access the data."""
    fs = FilesystemDestinationClientConfiguration(bucket_url=bucket_url)
    duck = DuckDbClientConfiguration(credentials=DuckDbCredentials("p/db.duckdb"))
    assert (fs.attach_type() is not None) is attachable
    assert duck.can_read_from(fs) is attachable
    # a same-location join needs no attach, so it stays possible either way
    assert fs.can_read_from(FilesystemDestinationClientConfiguration(bucket_url=bucket_url))


def test_filesystem_can_never_write() -> None:
    """dlt is the only engine that writes to filesystem, so SQL write is never possible."""
    c1 = FilesystemDestinationClientConfiguration(bucket_url="s3://b/p")
    c2 = FilesystemDestinationClientConfiguration(bucket_url="s3://b/p")
    # same location is readable but not writable
    assert c1.can_read_from(c2)
    assert not c1.can_write_from(c2)
    assert not c2.can_write_from(c1)
    # not even from itself
    assert not c1.can_write_from(c1)


def test_filesystem_cannot_read_from_non_filesystem() -> None:
    c = FilesystemDestinationClientConfiguration(bucket_url="s3://b/p")
    other = _PhysicalDestinationConfig("s3://b")
    assert_not_joinable(c, other)


def test_motherduck_token_not_exposed_as_data_location() -> None:
    """The token is the only account identity there is, so the location carries its digest."""
    md = MotherDuckClientConfiguration(
        credentials=MotherDuckCredentials("md:db?motherduck_token=token")
    )
    assert md.data_location() == f"md://{md.fingerprint()}"
    assert "token" not in md.data_location()


def test_motherduck_can_read_from_same_token_without_exposing_location() -> None:
    """Same token can join without exposing token via physical location."""
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


def test_motherduck_without_token_identifies_no_account() -> None:
    """The token is the account identity. Without a token there is nothing to compare and no way
    to connect. dlt raises a configuration error instead of a failed join."""
    with_token = MotherDuckClientConfiguration(
        credentials=MotherDuckCredentials("md:db?motherduck_token=token")
    )
    without_token = MotherDuckClientConfiguration(credentials=MotherDuckCredentials("md:db"))
    with pytest.raises(ConfigurationValueError, match="no MotherDuck access token"):
        without_token.data_location()
    with pytest.raises(ConfigurationValueError):
        with_token.can_read_from(without_token)
    with pytest.raises(ConfigurationValueError):
        without_token.can_write_from(with_token)


def test_motherduck_can_read_from_non_motherduck() -> None:
    """MotherDuck cannot join with other destination types."""
    md = MotherDuckClientConfiguration(
        credentials=MotherDuckCredentials("md:db?motherduck_token=token")
    )
    pg = PostgresClientConfiguration(credentials=PostgresCredentials("postgresql://h"))
    assert_not_joinable(md, pg)


def test_motherduck_can_write_from_same_token() -> None:
    """MotherDuck can write from another config only when the identity token matches."""
    c1 = MotherDuckClientConfiguration(
        credentials=MotherDuckCredentials("md:db?motherduck_token=token")
    )
    c2 = MotherDuckClientConfiguration(
        credentials=MotherDuckCredentials("md:db?motherduck_token=token")
    )
    # same token is writable both ways
    assert c1.can_write_from(c2)
    assert c2.can_write_from(c1)
    # different token cannot write
    other = MotherDuckClientConfiguration(
        credentials=MotherDuckCredentials("md:db?motherduck_token=other")
    )
    assert not c1.can_write_from(other)
    assert not other.can_write_from(c1)
    # a missing token is a configuration error, which another test covers


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
    # dbapi driver suffix does not change the backend identity
    pytest.param(
        "mysql+pymysql://u@h:3306/db1", "mysql+mysqldb://u@h:3306/db2", True, id="mysql_dbapi"
    ),
    pytest.param(
        "postgresql+psycopg2://u@h:5432/db", "postgresql://u@h:5432/db", True, id="pg_dbapi"
    ),
    # each in-memory database is a separate database
    pytest.param("sqlite:///:memory:", "sqlite:///:memory:", False, id="sqlite_memory"),
    # mssql can query across databases via 3-part names
    pytest.param(
        "mssql+pyodbc://u@h:1433/db1", "mssql+pyodbc://u@h:1433/db2", True, id="mssql_diff_db"
    ),
    # oracle (db links) and db2 (federation) cannot query across databases
    pytest.param("oracle://u@h:1521/svc", "oracle://u@h:1521/svc", True, id="oracle_same_service"),
    pytest.param(
        "oracle://u@h:1521/svc1", "oracle://u@h:1521/svc2", False, id="oracle_diff_service"
    ),
    pytest.param("db2://u@h:50000/db1", "db2://u@h:50000/db2", False, id="db2_diff_db"),
]


@pytest.mark.parametrize("conn1,conn2,expected", SQLA_CASES)
def test_sqlalchemy_can_read_from(conn1: str, conn2: str, expected: bool) -> None:
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
            id="same_uri",
        ),
        pytest.param(
            lambda: _lancedb_config("/tmp/db1.lancedb"),
            lambda: _lancedb_config("/tmp/db2.lancedb"),
            True,
            id="different_uri",
        ),
        pytest.param(
            lambda: _lancedb_config("/tmp/db.lancedb", dataset_name="dataset1"),
            lambda: _lancedb_config("/tmp/db.lancedb", dataset_name="dataset2"),
            True,
            id="different_dataset_same_uri",
        ),
        # any table at the same location is readable via the same ATTACH,
        # separator only affects table naming
        pytest.param(
            lambda: _lancedb_config("/tmp/db.lancedb", dataset_separator="___"),
            lambda: _lancedb_config("/tmp/db.lancedb", dataset_separator="__"),
            True,
            id="different_separator_same_uri",
        ),
        pytest.param(
            lambda: _lancedb_config(":external:"),
            lambda: _lancedb_config(":external:"),
            True,
            id="external_native_client",
        ),
    ],
)
def test_lancedb_can_read_from(f1: ConfigFactory, f2: ConfigFactory, expected: bool) -> None:
    assert_join_result(f1(), f2(), expected)


def test_lancedb_can_never_write() -> None:
    """dlt is the only engine that writes to LanceDB, so SQL write is never possible."""
    c1 = _lancedb_config("/tmp/db.lancedb")
    c2 = _lancedb_config("/tmp/db.lancedb")
    # same location is readable but not writable
    assert c1.can_read_from(c2)
    assert not c1.can_write_from(c2)
    assert not c2.can_write_from(c1)
    # not even from itself
    assert not c1.can_write_from(c1)


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
            True,
            id="different_catalog",
        ),
        pytest.param(
            lambda: _lance_config("file:///tmp/lance", dataset_name="dataset1"),
            lambda: _lance_config("file:///tmp/lance", dataset_name="dataset2"),
            True,
            id="different_dataset_same_catalog",
        ),
        pytest.param(
            lambda: _lance_multi_base_config("s3://catalogs/manifest", "s3://data1/lake"),
            lambda: _lance_multi_base_config("s3://catalogs/manifest", "s3://data2/lake"),
            True,
            id="same_catalog_different_data_storage",
        ),
        pytest.param(
            lambda: _lance_rest_config("http://127.0.0.1:2333"),
            lambda: _lance_rest_config("http://127.0.0.1:2333/"),
            True,
            id="same_rest_namespace",
        ),
        pytest.param(
            lambda: _lance_rest_config("http://127.0.0.1:2333"),
            lambda: _lance_rest_config("http://other:2333"),
            True,
            id="different_rest_namespace",
        ),
    ],
)
def test_lance_can_read_from(f1: ConfigFactory, f2: ConfigFactory, expected: bool) -> None:
    assert_join_result(f1(), f2(), expected)


@pytest.mark.parametrize(
    "factory,expected",
    [
        pytest.param(
            lambda: _lance_config("file:///tmp/lance"),
            "dir:file:///tmp/lance",
            id="explicit_dir_catalog",
        ),
        pytest.param(
            lambda: _lance_multi_base_config("s3://catalogs/manifest", "s3://data/lake"),
            "dir:s3://catalogs/manifest",
            id="catalog_takes_precedence_over_storage",
        ),
        pytest.param(
            lambda: _lance_multi_base_config(None, "s3://data/lake"),
            f"dir:s3://data/lake/{DEFAULT_LANCE_NAMESPACE_NAME}",
            id="falls_back_to_storage_namespace",
        ),
        pytest.param(
            lambda: _lance_rest_config("http://127.0.0.1:2333/"),
            "rest:http://127.0.0.1:2333",
            id="rest_namespace_uri",
        ),
    ],
)
def test_lance_data_location(factory: ConfigFactory, expected: str) -> None:
    assert factory().data_location() == expected


@pytest.mark.parametrize(
    "factory",
    [
        pytest.param(lambda: _lance_rest_config(None), id="rest_without_uri"),
        pytest.param(lambda: LanceClientConfiguration(), id="empty"),
    ],
)
def test_lance_data_location_raises_when_not_configured(factory: ConfigFactory) -> None:
    with pytest.raises(ConfigurationValueError, match="cannot determine the data location"):
        factory().data_location()


def test_lance_can_never_write() -> None:
    """dlt is the only engine that writes to Lance, so SQL write is never possible."""
    c1 = _lance_config("file:///tmp/lance")
    c2 = _lance_config("file:///tmp/lance")
    # same catalog and dataset is readable but not writable
    assert c1.can_read_from(c2)
    assert not c1.can_write_from(c2)
    assert not c2.can_write_from(c1)
    # not even from itself
    assert not c1.can_write_from(c1)


def test_lance_and_lancedb_can_join_with_each_other() -> None:
    """Both read `.lance` data through the same duckdb extension, so either attaches the other."""
    lance = _lance_config("file:///tmp/lance")
    lancedb = _lancedb_config("file:///tmp/lance")
    assert_joinable(lance, lancedb)


def test_weaviate_data_location_but_not_joinable() -> None:
    c1 = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="https://cluster.weaviate.cloud")
    )
    c2 = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="https://cluster.weaviate.cloud")
    )
    assert c1.data_location() == "cluster.weaviate.cloud"
    assert_not_joinable(c1, c2)


def test_qdrant_data_location_but_not_joinable() -> None:
    c1 = QdrantClientConfiguration(qd_location="https://cluster.qdrant.io")
    c2 = QdrantClientConfiguration(qd_location="https://cluster.qdrant.io")
    assert c1.data_location() == "https://cluster.qdrant.io"
    assert_not_joinable(c1, c2)
    assert not c1.can_write_from(c2)


# data_location() - `can_read_from` derives its answer from this key


def test_data_location_never_carries_secrets() -> None:
    """dlt compares the location in the clear, so a secret identity must arrive as a digest."""
    token = "sup3rsecret-token"
    md = MotherDuckClientConfiguration(
        credentials=MotherDuckCredentials(f"md:///my_db?token={token}")
    )
    assert md.data_location()
    assert token not in md.data_location()

    password = "sup3rsecret-password"
    lake = DuckLakeClientConfiguration(
        credentials=_ducklake_creds(f"postgres://u:{password}@h:5432/db", "lake")
    )
    assert lake.data_location()
    assert password not in lake.data_location()


def test_data_location_tells_data_locations_apart() -> None:
    """Two configs share a data location when one query engine accesses both."""
    # one MotherDuck token accesses the whole account
    same_token = MotherDuckCredentials("md:///db_a?token=T"), MotherDuckCredentials(
        "md:///db_b?token=T"
    )
    a, b = (MotherDuckClientConfiguration(credentials=c) for c in same_token)
    assert a.data_location() == b.data_location()
    other = MotherDuckClientConfiguration(
        credentials=MotherDuckCredentials("md:///db_a?token=OTHER")
    )
    assert a.data_location() != other.data_location()

    # one ducklake is one metadata schema inside one catalog
    catalog = "postgres://u:p@h:5432/db"
    lake = DuckLakeClientConfiguration(credentials=_ducklake_creds(catalog, "lake"))
    same = DuckLakeClientConfiguration(credentials=_ducklake_creds(catalog, "lake"))
    other_schema = DuckLakeClientConfiguration(
        credentials=_ducklake_creds(catalog, "lake", metadata_schema="other")
    )
    assert lake.data_location() == same.data_location()
    assert lake.data_location() != other_schema.data_location()

    # a destination with no query engine keeps a location to display, but accesses nothing
    qdrant = QdrantClientConfiguration(qd_location="https://q")
    assert qdrant.data_location() == "https://q"
    assert not qdrant.can_read_from(QdrantClientConfiguration(qd_location="https://q"))
    weaviate = WeaviateClientConfiguration(credentials=WeaviateCredentials(url="https://w"))
    assert weaviate.data_location() == "w"
    assert not weaviate.can_read_from(
        WeaviateClientConfiguration(credentials=WeaviateCredentials(url="https://w"))
    )


def test_can_read_from_is_asymmetric() -> None:
    """`assert_join_result` asserts both directions, so the asymmetry needs its own test. An
    attachable dataset is accessible to a non-attachable one. The reverse is not true."""
    root = active().local_dir
    attachable = FilesystemDestinationClientConfiguration(
        bucket_url=os.path.join(root, "bucket")
    )._bind_dataset_name("ds")
    # only fsspec registers gs in this process, so no query engine can attach it
    not_attachable = FilesystemDestinationClientConfiguration(
        bucket_url="gs://bucket/path"
    )._bind_dataset_name("ds")

    assert attachable.attach_type() == "duckdb"
    assert not_attachable.attach_type() is None
    assert not_attachable.can_read_from(attachable)
    assert not attachable.can_read_from(not_attachable)


def test_needs_attach_agrees_with_is_same_location() -> None:
    """One query engine accesses every schema of the database that it opened, and nothing else."""
    one = DuckDbClientConfiguration(credentials=DuckDbCredentials("p/db.duckdb"))
    same = DuckDbClientConfiguration(credentials=DuckDbCredentials("p/db.duckdb"))
    other = DuckDbClientConfiguration(credentials=DuckDbCredentials("p/db2.duckdb"))
    assert one.needs_attach(same) is False
    assert one.needs_attach(other) is True
    # only the query engine that holds an in-memory database accesses it, so that engine is its
    # identity. dlt rejects `:memory:` as a credential string for this reason
    conn = duckdb.connect()
    try:
        in_memory = DuckDbClientConfiguration(credentials=DuckDbCredentials(conn))
        shared = DuckDbClientConfiguration(credentials=DuckDbCredentials(conn))
        assert in_memory.needs_attach(shared) is False
        assert in_memory.needs_attach(one) is True
        assert one.needs_attach(in_memory) is True
        assert in_memory.needs_attach(
            DuckDbClientConfiguration(credentials=DuckDbCredentials(duckdb.connect()))
        )
    finally:
        conn.close()

    # one MotherDuck token accesses the whole account
    token = MotherDuckClientConfiguration(credentials=MotherDuckCredentials("md:a?token=T"))
    assert (
        token.needs_attach(
            MotherDuckClientConfiguration(credentials=MotherDuckCredentials("md:b?token=T"))
        )
        is False
    )
    assert token.needs_attach(
        MotherDuckClientConfiguration(credentials=MotherDuckCredentials("md:b?token=OTHER"))
    )


def test_needs_attach_across_destination_types() -> None:
    """Two destination types can identify the same path and still be different query engines.
    dlt must not read a location match across types as "already accessible". Such a match emits
    no `ATTACH` and queries a catalog that nobody attached.
    """
    shared = os.path.abspath("shared_location")
    duck = DuckDbClientConfiguration(credentials=DuckDbCredentials(shared))._bind_dataset_name("a")
    # a duckdb database and a local bucket are both bare absolute paths
    bucket = FilesystemDestinationClientConfiguration(bucket_url=shared)._bind_dataset_name("b")

    assert duck.data_location() == bucket.data_location()
    # duckdb accesses the scanner views of the bucket only through an attach, never directly
    assert duck.can_read_from(bucket)
    assert duck.needs_attach(bucket) is True


def test_data_location_is_never_blank() -> None:
    """dlt compares a location for equality, so a blank one reads as "these two are the same
    place". Every config either names its place or refuses to guess.
    """
    factories = [
        cast(ConfigFactory, case.values[0])
        for case in [*PHYSICAL_DEST_CASES, *NO_LOCATION_DEST_CASES]
    ]
    assert len(factories) > 30

    for factory in factories:
        try:
            location = factory().data_location()
        except ConfigurationValueError:
            continue
        # `None` says "no place at all", which never matches. An empty location matches
        # the next empty one
        assert location is None or location, f"{factory} reported a blank location"


def test_reading_attaches_but_writing_does_not() -> None:
    """A join attaches the foreign data and reads it. A model attaches only the datasets that it
    joins, never the one that it selects from. A model therefore writes only when this location
    already holds that data.
    """
    one = DuckDbClientConfiguration(credentials=DuckDbCredentials("p/db.duckdb"))
    other = DuckDbClientConfiguration(credentials=DuckDbCredentials("p/db2.duckdb"))

    assert one.can_read_from(other)
    assert other.can_read_from(one)
    assert one.can_write_from(other) is False
    assert other.can_write_from(one) is False

    # within one database both hold
    same = DuckDbClientConfiguration(credentials=DuckDbCredentials("p/db.duckdb"))
    assert one.can_read_from(same)
    assert one.can_write_from(same)
