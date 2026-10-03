import warnings
from typing import Any, Optional

import pytest
from packaging.version import Version

from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.schema import Schema
from dlt.common.utils import digest128
from dlt.common.warnings import Dlt100DeprecationWarning
from dlt.destinations import weaviate as weaviate_destination
from dlt.destinations.impl.weaviate.configuration import (
    TWeaviateBatchMode,
    WeaviateClientConfiguration,
    WeaviateCredentials,
    WEAVIATE_CLIENT_INTEGRATION,
    WEAVIATE_CLIENT_INTEGRATION_HEADER,
    WEAVIATE_SERVER_SIDE_BATCH_MIN_VERSION,
)
from dlt.destinations.impl.weaviate.weaviate_client import WeaviateClient

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "credentials,expected_fingerprint",
    [
        pytest.param(None, "", id="empty"),
        pytest.param(
            WeaviateCredentials(url="https://weaviate.example.com:8080/v1"),
            digest128("weaviate.example.com"),
            id="hostname_only_url",
        ),
        pytest.param(
            WeaviateCredentials(url="http://localhost:8080"),
            digest128("localhost"),
            id="hostname_only_localhost",
        ),
    ],
)
def test_weaviate_fingerprint(
    credentials: Optional[WeaviateCredentials], expected_fingerprint: str
) -> None:
    config = WeaviateClientConfiguration(credentials=credentials)

    assert config.fingerprint() == expected_fingerprint


@pytest.mark.parametrize(
    "url,expected",
    [
        pytest.param("https://my-cluster.weaviate.cloud", "cloud", id="cloud"),
        pytest.param("https://my-cluster.wcs.api.weaviate.io", "cloud", id="cloud_wcs"),
        pytest.param("http://localhost:8080", "local", id="localhost"),
        pytest.param("http://127.0.0.1:8080", "local", id="loopback"),
        pytest.param("https://weaviate.example.com", "custom", id="custom"),
    ],
)
def test_connection_type_inferred_from_url(url: str, expected: str) -> None:
    config = WeaviateClientConfiguration(credentials=WeaviateCredentials(url=url))

    assert config.resolve_connection_type() == expected


def test_explicit_connection_type_wins_over_url() -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="http://localhost:8080"),
        connection_type="custom",
    )

    assert config.resolve_connection_type() == "custom"


def test_integration_header_is_sent_by_default() -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="http://localhost:8080")
    )

    headers = config.to_connector_params()["headers"]

    assert headers[WEAVIATE_CLIENT_INTEGRATION_HEADER] == WEAVIATE_CLIENT_INTEGRATION
    assert WEAVIATE_CLIENT_INTEGRATION.startswith("dlt/")


def test_additional_headers_override_integration_header() -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(
            url="http://localhost:8080",
            additional_headers={
                "X-OpenAI-Api-Key": "key",
                WEAVIATE_CLIENT_INTEGRATION_HEADER: "custom",
            },
        )
    )

    headers = config.to_connector_params()["headers"]

    assert headers[WEAVIATE_CLIENT_INTEGRATION_HEADER] == "custom"
    assert headers["X-OpenAI-Api-Key"] == "key"


@pytest.mark.parametrize("connection_type", ["cloud", "local", "custom"])
def test_integration_header_sent_for_every_connection_type(connection_type: str) -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(
            url="https://weaviate.example.com", http_port=8080, grpc_port=50051
        ),
        connection_type=connection_type,  # type: ignore[arg-type]
    )

    headers = config.to_connector_params()["headers"]

    assert headers[WEAVIATE_CLIENT_INTEGRATION_HEADER] == WEAVIATE_CLIENT_INTEGRATION


def test_timeouts_are_passed_to_the_client() -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="http://localhost:8080"),
        conn_timeout=1.0,
        read_timeout=2.0,
    )

    timeout = config.to_connector_params()["additional_config"].timeout

    assert timeout.init == 1.0
    assert timeout.query == 2.0
    assert timeout.insert == 2.0


def test_custom_connection_requires_explicit_ports() -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="https://weaviate.example.com"),
        connection_type="custom",
    )

    with pytest.raises(ConfigurationValueError):
        config.to_connector_params()


def test_custom_connection_uses_url_scheme_for_security() -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(
            url="https://weaviate.example.com", http_port=443, grpc_port=50051
        ),
        connection_type="custom",
    )

    params = config.to_connector_params()

    assert params["http_host"] == "weaviate.example.com"
    assert params["http_secure"] is True
    assert params["grpc_secure"] is True


def test_local_connection_defaults_ports() -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="http://localhost:8080")
    )

    params = config.to_connector_params()

    assert params["host"] == "localhost"
    assert params["port"] == 8080
    assert params["grpc_port"] == 50051


@pytest.mark.parametrize("option", ["batch_retries", "startup_period"])
def test_batch_options_without_v4_equivalent_are_deprecated(option: str) -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="http://localhost:8080")
    )
    setattr(config, option, 99)

    with pytest.warns(Dlt100DeprecationWarning, match=option):
        config.on_resolved()


def test_default_batch_options_do_not_warn() -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="http://localhost:8080")
    )

    with warnings.catch_warnings():
        warnings.simplefilter("error", Dlt100DeprecationWarning)
        config.on_resolved()


def _client_with_server_version(server_version: str, **config_kwargs: Any) -> WeaviateClient:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="http://localhost:8080"), **config_kwargs
    )
    client = WeaviateClient(
        Schema("batch_mode_test"), config, weaviate_destination().capabilities()
    )
    client._server_version = Version(server_version)
    return client


@pytest.mark.parametrize(
    "server_version,expected",
    [
        pytest.param(WEAVIATE_SERVER_SIDE_BATCH_MIN_VERSION, "stream", id="exactly_min"),
        pytest.param("1.36.23", "stream", id="above_min"),
        pytest.param("1.40.0", "stream", id="well_above_min"),
        pytest.param("1.35.2", "fixed_size", id="below_min"),
        pytest.param("1.30.0", "fixed_size", id="well_below_min"),
    ],
)
def test_auto_batch_mode_follows_server_version(server_version: str, expected: str) -> None:
    client = _client_with_server_version(server_version)

    assert client.supports_server_side_batching() == (expected == "stream")
    assert client.resolve_batch_mode() == expected


@pytest.mark.parametrize("batch_mode", ["stream", "dynamic", "fixed_size", "rate_limit"])
def test_explicit_batch_mode_is_not_overridden(batch_mode: TWeaviateBatchMode) -> None:
    """An explicit mode is honoured even when the server could not support it."""
    client = _client_with_server_version("1.30.0", batch_mode=batch_mode)

    assert client.resolve_batch_mode() == batch_mode


def test_tenant_requires_multi_tenancy() -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="http://localhost:8080"), tenant="tenanta"
    )

    with pytest.raises(ConfigurationValueError):
        config.on_resolved()


def test_multi_tenancy_requires_a_tenant() -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="http://localhost:8080"), multi_tenancy=True
    )

    with pytest.raises(ConfigurationValueError):
        config.on_resolved()


def test_multi_tenancy_with_tenant_resolves() -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="http://localhost:8080"),
        multi_tenancy=True,
        tenant="tenanta",
    )

    config.on_resolved()


def test_grpc_host_defaults_to_the_rest_host() -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(
            url="https://weaviate.example.com", http_port=443, grpc_port=50051
        ),
        connection_type="custom",
    )

    params = config.to_connector_params()

    assert params["http_host"] == "weaviate.example.com"
    assert params["grpc_host"] == "weaviate.example.com"


def test_grpc_host_can_differ_from_the_rest_host() -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(
            url="https://rest.example.com",
            http_port=443,
            grpc_port=50051,
            grpc_host="grpc.example.com",
        ),
        connection_type="custom",
    )

    params = config.to_connector_params()

    assert params["http_host"] == "rest.example.com"
    assert params["grpc_host"] == "grpc.example.com"


@pytest.mark.parametrize("connection_type", ["local", "cloud"])
def test_grpc_host_is_refused_where_the_client_cannot_use_it(connection_type: str) -> None:
    """Only `connect_to_custom` takes a separate gRPC host, so elsewhere it must not be ignored."""
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="http://localhost:8080", grpc_host="grpc.example.com"),
        connection_type=connection_type,  # type: ignore[arg-type]
    )

    with pytest.raises(ConfigurationValueError, match="grpc_host"):
        config.to_connector_params()


@pytest.mark.parametrize(
    "url,http_secure,grpc_secure,expected_http,expected_grpc",
    [
        pytest.param("https://x.example.com", None, None, True, True, id="https_url"),
        pytest.param("http://x.example.com", None, None, False, False, id="http_url"),
        pytest.param("http://x.example.com", True, None, True, True, id="explicit_http_secure"),
        pytest.param("https://x.example.com", None, False, True, False, id="insecure_grpc_only"),
        pytest.param("http://x.example.com", False, True, False, True, id="secure_grpc_only"),
    ],
)
def test_tls_is_taken_from_the_url_unless_set_explicitly(
    url: str,
    http_secure: Optional[bool],
    grpc_secure: Optional[bool],
    expected_http: bool,
    expected_grpc: bool,
) -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(
            url=url,
            http_port=443,
            grpc_port=50051,
            http_secure=http_secure,
            grpc_secure=grpc_secure,
        ),
        connection_type="custom",
    )

    params = config.to_connector_params()

    assert params["http_secure"] is expected_http
    assert params["grpc_secure"] is expected_grpc


@pytest.mark.parametrize(
    "connection_type,expected",
    [
        pytest.param("local", False, id="local_checks_on_startup"),
        pytest.param("cloud", True, id="cloud_skips"),
        pytest.param("custom", True, id="custom_skips"),
    ],
)
def test_skip_init_checks_default_per_connection_type(connection_type: str, expected: bool) -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(
            url="https://weaviate.example.com", http_port=443, grpc_port=50051
        ),
        connection_type=connection_type,  # type: ignore[arg-type]
    )

    assert config.to_connector_params()["skip_init_checks"] is expected


@pytest.mark.parametrize("skip_init_checks", [True, False])
def test_skip_init_checks_can_be_set_explicitly(skip_init_checks: bool) -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="http://localhost:8080"),
        skip_init_checks=skip_init_checks,
    )

    assert config.to_connector_params()["skip_init_checks"] is skip_init_checks


def test_legacy_timeouts_still_drive_the_client() -> None:
    """`conn_timeout`/`read_timeout` predate the per-operation timeouts and must keep working."""
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="http://localhost:8080"),
        conn_timeout=4.0,
        read_timeout=44.0,
    )

    timeout = config.to_connector_params()["additional_config"].timeout_

    assert timeout.init == 4.0
    assert timeout.query == 44.0
    assert timeout.insert == 44.0


def test_per_operation_timeouts_override_the_legacy_ones() -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="http://localhost:8080"),
        conn_timeout=4.0,
        read_timeout=44.0,
        init_timeout=1.5,
        query_timeout=11.0,
        insert_timeout=22.0,
        stream_timeout=33.0,
    )

    timeout = config.to_connector_params()["additional_config"].timeout_

    assert timeout.init == 1.5
    assert timeout.query == 11.0
    assert timeout.insert == 22.0
    assert timeout.stream == 33.0


def test_proxies_and_trust_env_reach_the_client() -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="http://localhost:8080"),
        proxies={"https": "http://proxy:3128"},
        trust_env=True,
    )

    additional_config = config.to_connector_params()["additional_config"]

    assert additional_config.proxies.https == "http://proxy:3128"
    assert additional_config.trust_env is True


def test_session_pool_options_reach_the_client() -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="http://localhost:8080"),
        session_pool_connections=7,
        session_pool_maxsize=77,
        session_pool_max_retries=2,
        session_pool_timeout=9,
    )

    connection = config.to_connector_params()["additional_config"].connection

    assert connection.session_pool_connections == 7
    assert connection.session_pool_maxsize == 77
    assert connection.session_pool_max_retries == 2
    assert connection.session_pool_timeout == 9


def test_session_pool_defaults_are_left_to_the_client() -> None:
    """Unset pool options must not pin dlt's own values on top of the client's defaults."""
    default_connection = (
        WeaviateClientConfiguration(credentials=WeaviateCredentials(url="http://localhost:8080"))
        .to_connector_params()["additional_config"]
        .connection
    )

    from weaviate.config import ConnectionConfig

    assert default_connection.session_pool_connections == (
        ConnectionConfig().session_pool_connections
    )
    assert default_connection.session_pool_maxsize == ConnectionConfig().session_pool_maxsize


@pytest.mark.parametrize("option", ["auto_tenant_creation", "auto_tenant_activation"])
def test_tenant_automation_defaults_to_on(option: str) -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="http://localhost:8080"),
        multi_tenancy=True,
        tenant="tenanta",
    )

    assert getattr(config, option) is True


@pytest.mark.parametrize("option", ["auto_tenant_creation", "auto_tenant_activation"])
def test_tenant_automation_can_be_turned_off(option: str) -> None:
    config = WeaviateClientConfiguration(
        credentials=WeaviateCredentials(url="http://localhost:8080"),
        multi_tenancy=True,
        tenant="tenanta",
        **{option: False},  # type: ignore[arg-type]
    )

    assert getattr(config, option) is False
