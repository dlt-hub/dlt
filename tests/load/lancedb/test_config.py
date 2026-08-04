import os
from pathlib import Path
from typing import Iterator, Optional

import pytest

import dlt
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.exceptions import (
    ConfigFieldMissingException,
    ConfigurationValueError,
)
from dlt.common.schema import Schema
from dlt.common.utils import digest128

from dlt.destinations.impl.lancedb.configuration import (
    DEFAULT_FLIGHTSQL_PORT,
    LanceDBClientConfiguration,
    LanceDBCredentials,
)

from tests.utils import reset_providers


# Mark all tests as essential, do not remove.
pytestmark = pytest.mark.essential


@pytest.fixture(autouse=True)
def clear_lancedb_env(tmp_path: Path) -> Iterator[None]:
    """Drops the credentials of the test cluster so resolution can be asserted from scratch."""
    for key in list(os.environ):
        if key.startswith("DESTINATION__LANCEDB__"):
            del os.environ[key]
    # toml providers read an empty dir, so a configured cluster cannot leak into resolution
    with reset_providers(settings_dir=str(tmp_path)):
        yield


def test_lancedb_configuration() -> None:
    os.environ["DESTINATION__LANCEDB__CREDENTIALS__API_KEY"] = "secret"
    os.environ["DESTINATION__LANCEDB__EMBEDDINGS__PROVIDER"] = "openai"
    os.environ["DESTINATION__LANCEDB__EMBEDDINGS__NAME"] = "text-embedding-3-small"

    config = resolve_configuration(
        LanceDBClientConfiguration()._bind_dataset_name(dataset_name="dataset"),
        sections=("destination", "lancedb"),
    )
    # no database is configured, so every dataset becomes a database of its own
    assert config.credentials.database is None
    assert config.embeddings.provider == "openai"
    assert config.embeddings.name == "text-embedding-3-small"
    # the vector column and retries fall back to the shared defaults
    assert config.embeddings.vector_column == "vector"
    assert config.embeddings.max_retries == 3
    # reads are disabled until the Arrow Flight SQL endpoint is configured
    assert config.credentials.has_flightsql is False
    assert config.dataset_sentinel_namespace_name == "_dlt_sentinel"


def test_api_key_is_required() -> None:
    with pytest.raises(ConfigFieldMissingException):
        resolve_configuration(
            LanceDBClientConfiguration()._bind_dataset_name(dataset_name="dataset"),
            sections=("destination", "lancedb"),
        )


def test_cluster_is_required() -> None:
    with pytest.raises(ConfigurationValueError):
        resolve_configuration(
            LanceDBCredentials(api_key="secret", region=None),
            sections=("destination", "lancedb"),
        )


@pytest.mark.parametrize(
    "pinned_database,dataset_name,expected_database",
    [
        pytest.param(None, "sales", "sales", id="dataset-is-the-database"),
        # a dataset name is normalized like any other, and the database follows it
        pytest.param(None, "My-Sales", "my_sales", id="dataset-normalized"),
        pytest.param("dlt-ci-5", None, "dlt-ci-5", id="pinned-without-dataset"),
        pytest.param("dlt-ci-5", "dlt_ci_5", "dlt-ci-5", id="pinned-dataset-matches-normalized"),
    ],
)
def test_dataset_becomes_database(
    pinned_database: Optional[str], dataset_name: Optional[str], expected_database: str
) -> None:
    config = LanceDBClientConfiguration(
        credentials=LanceDBCredentials(database=pinned_database)
    )._bind_dataset_name(dataset_name=dataset_name)

    assert config.normalize_dataset_name(Schema("events")) == expected_database


def test_configured_database_holds_every_schema() -> None:
    """The configured database is the dataset, so a second schema does not get one of its own."""
    config = LanceDBClientConfiguration(
        credentials=LanceDBCredentials(database="dlt-ci-5")
    )._bind_dataset_name(dataset_name=None, default_schema_name="events")

    assert config.normalize_dataset_name(Schema("events")) == "dlt-ci-5"
    assert config.normalize_dataset_name(Schema("other")) == "dlt-ci-5"


def test_dataset_gets_a_database_per_schema() -> None:
    """Without a configured database, a non-default schema keeps the dlt suffix and becomes one."""
    config = LanceDBClientConfiguration(credentials=LanceDBCredentials())._bind_dataset_name(
        dataset_name="sales", default_schema_name="events"
    )

    assert config.normalize_dataset_name(Schema("events")) == "sales"
    assert config.normalize_dataset_name(Schema("other")) == "sales_other"


def test_dataset_name_stays_optional() -> None:
    """The configured database stands in for the dataset, so `dlt` must not autogenerate a name."""
    assert not LanceDBClientConfiguration.needs_dataset_name()

    os.environ["DESTINATION__LANCEDB__CREDENTIALS__API_KEY"] = "secret"
    pipeline = dlt.pipeline(pipeline_name="lancedb_optional_dataset", destination="lancedb")

    assert pipeline.dataset_name is None


def test_dataset_without_name_needs_a_configured_database() -> None:
    """Without a database to fall back on, the missing dataset is caught while resolving."""
    os.environ["DESTINATION__LANCEDB__CREDENTIALS__API_KEY"] = "secret"

    with pytest.raises(ConfigurationValueError):
        resolve_configuration(
            LanceDBClientConfiguration()._bind_dataset_name(dataset_name=None),
            sections=("destination", "lancedb"),
        )


def test_configured_database_resolves_without_a_dataset() -> None:
    os.environ["DESTINATION__LANCEDB__CREDENTIALS__API_KEY"] = "secret"
    os.environ["DESTINATION__LANCEDB__CREDENTIALS__DATABASE"] = "dlt-ci-5"

    config = resolve_configuration(
        LanceDBClientConfiguration()._bind_dataset_name(dataset_name=None),
        sections=("destination", "lancedb"),
    )

    assert config.normalize_dataset_name(Schema("events")) == "dlt-ci-5"


def test_configured_database_rejects_a_different_dataset() -> None:
    config = LanceDBClientConfiguration(
        credentials=LanceDBCredentials(database="dlt-ci-5")
    )._bind_dataset_name(dataset_name="sales")

    with pytest.raises(ConfigurationValueError):
        config.normalize_dataset_name(Schema("events"))


@pytest.mark.parametrize(
    "host_override,api_key,region,expected_location",
    [
        pytest.param(
            "https://cluster.example.com/",
            "secret",
            "us-east-1",
            "lancedb:https://cluster.example.com",
            id="enterprise-endpoint-identifies-it",
        ),
        # Cloud shares a region between tenants, so the api key is the only account identity
        pytest.param(
            None,
            "secret",
            "us-east-1",
            f"lancedb-cloud:us-east-1:{digest128('secret')}",
            id="cloud-account",
        ),
    ],
)
def test_lancedb_data_location(
    host_override: Optional[str],
    api_key: Optional[str],
    region: Optional[str],
    expected_location: str,
) -> None:
    # the location identifies the cluster, not a database, because every dataset is a database
    config = LanceDBClientConfiguration(
        credentials=LanceDBCredentials(host_override=host_override, api_key=api_key, region=region)
    )

    assert config.data_location() == expected_location
    assert config.fingerprint() == digest128(expected_location)


def test_lancedb_without_a_cluster_has_no_data_location() -> None:
    config = LanceDBClientConfiguration(
        credentials=LanceDBCredentials(host_override=None, api_key=None, region=None)
    )

    with pytest.raises(ConfigurationValueError):
        config.data_location()
    # telemetry must not raise, so a fingerprint of nothing is blank
    assert config.fingerprint() == ""


def test_lancedb_can_read_from() -> None:
    """Two datasets of one cluster must be readable from each other, which joins across them need."""
    sales = LanceDBClientConfiguration(
        credentials=LanceDBCredentials(host_override="https://cluster")
    )._bind_dataset_name(dataset_name="sales")
    marketing = LanceDBClientConfiguration(
        credentials=LanceDBCredentials(host_override="https://cluster")
    )._bind_dataset_name(dataset_name="marketing")
    other_cluster = LanceDBClientConfiguration(
        credentials=LanceDBCredentials(host_override="https://other")
    )._bind_dataset_name(dataset_name="sales")

    assert sales.can_read_from(marketing)
    assert marketing.can_read_from(sales)
    assert not sales.can_read_from(other_cluster)
    # dlt is the only engine that writes to LanceDB
    assert not sales.can_write_from(marketing)


@pytest.mark.parametrize(
    "host,tls,expected_uri",
    [
        pytest.param("flight.example.com", False, "grpc://flight.example.com:10025", id="plain"),
        pytest.param("flight.example.com", True, "grpc+tls://flight.example.com:10025", id="tls"),
        # the endpoint is commonly configured as a URL, whose scheme selects TLS
        pytest.param(
            "http://flight.example.com", False, "grpc://flight.example.com:10025", id="http-url"
        ),
        pytest.param(
            "https://flight.example.com/",
            False,
            "grpc+tls://flight.example.com:10025",
            id="https-url",
        ),
        pytest.param(
            "http://flight.example.com", True, "grpc+tls://flight.example.com:10025", id="http-tls"
        ),
    ],
)
def test_flightsql_uri(host: str, tls: bool, expected_uri: str) -> None:
    credentials = LanceDBCredentials(flightsql_host=host, flightsql_tls=tls)

    assert credentials.flightsql_port == DEFAULT_FLIGHTSQL_PORT
    assert credentials.has_flightsql
    assert credentials.flightsql_uri() == expected_uri


def test_flightsql_headers() -> None:
    credentials = LanceDBCredentials(
        api_key="secret",
        flightsql_host="flight.example.com",
        headers={"X-Custom": "value"},
    )

    # header names must be lowercase bytes, the endpoint routes on the `database` header
    assert credentials.flightsql_headers("sales") == [
        (b"x-custom", b"value"),
        (b"authorization", b"Bearer secret"),
        (b"database", b"sales"),
    ]


def test_weak_read_consistency_interval() -> None:
    credentials = LanceDBCredentials()
    assert credentials.weak_read_consistency_interval_seconds == 0.0

    os.environ["DESTINATION__LANCEDB__CREDENTIALS__API_KEY"] = "secret"
    os.environ["DESTINATION__LANCEDB__CREDENTIALS__WEAK_READ_CONSISTENCY_INTERVAL_SECONDS"] = "10"
    config = resolve_configuration(
        LanceDBClientConfiguration()._bind_dataset_name(dataset_name="dataset"),
        sections=("destination", "lancedb"),
    )
    assert config.credentials.weak_read_consistency_interval_seconds == 10.0


def test_commit_tag_from_factory() -> None:
    os.environ["DESTINATION__LANCEDB__CREDENTIALS__API_KEY"] = "secret"

    destination = dlt.destinations.lancedb(commit_tag="snapshot_1")
    config = destination.configuration(
        destination.spec()._bind_dataset_name(dataset_name="dataset")
    )

    assert config.commit_tag == "snapshot_1"
