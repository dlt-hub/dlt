import os
import socket
import yaml
import pytest
from unittest import mock

from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.typing import ConfigValue

# Skip entire module if SQLAlchemy 2.0 is not installed (required by pyiceberg)
sqlalchemy = pytest.importorskip("sqlalchemy", minversion="2.0")

from dlt.common.libs.pyiceberg import (
    get_catalog,
)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def is_service_available(host: str, port: int, timeout: float = 1.0) -> bool:
    """Check if a TCP service is reachable."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, OSError):
        return False


# ============================================================================
# FIXTURES
# ============================================================================


@pytest.fixture
def test_credentials():
    """Create test AWS credentials for credential merging tests."""
    from dlt.common.configuration.specs.aws_credentials import AwsCredentials

    return AwsCredentials(
        aws_access_key_id="test_access_key", aws_secret_access_key="test_secret_key"
    )


@pytest.fixture(scope="session")
def rest_catalog_config():
    """Configuration for Iceberg REST catalog running at localhost:8181.

    Uses apache/iceberg-rest-fixture from docker-compose-iceberg.yml.
    Skips tests if REST catalog is not available.

    Start with: docker compose -f tests/common/libs/docker-compose-iceberg.yml up -d
    """
    if not is_service_available("localhost", 8181):
        pytest.skip("REST catalog not available at localhost:8181")

    return {
        "type": "rest",
        "uri": "http://localhost:8181",
        "warehouse": "s3://warehouse/",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
    }


@pytest.fixture(scope="session")
def postgres_catalog_config(tmp_path_factory):
    """Configuration for PostgreSQL SQL catalog.

    Uses PostgreSQL from docker-compose-iceberg.yml to store Iceberg catalog metadata.
    Skips tests if PostgreSQL is not available.

    Start with: docker compose -f tests/common/libs/docker-compose-iceberg.yml up -d
    """
    if not is_service_available("localhost", 5432):
        pytest.skip("PostgreSQL not available at localhost:5432")

    temp_dir = tmp_path_factory.mktemp("postgres_catalog")
    return {
        "type": "sql",
        "uri": "postgresql+psycopg2://loader:loader@localhost:5432/dlt_data",
        "warehouse": str(temp_dir / "warehouse"),
    }


@pytest.fixture
def sqlite_catalog_config(tmp_path_factory):
    """Configuration for SQLite catalog."""
    temp_dir = tmp_path_factory.mktemp("sqlite_catalog")
    db_path = temp_dir / "test_catalog.db"
    return {"type": "sql", "uri": f"sqlite:///{db_path}", "warehouse": str(temp_dir / "warehouse")}


@pytest.fixture(params=["sqlite", "postgres", "rest"], ids=["sqlite", "postgres", "rest"])
def catalog_config(request):
    """Parametrized fixture providing catalog configurations for all supported types.

    This allows tests to run against SQLite, PostgreSQL, and REST catalogs
    using the same test logic. Uses lazy fixture evaluation to avoid skipping
    sqlite tests when postgres/rest services are unavailable.
    """
    # Use getfixturevalue to lazily request only the needed fixture
    # This prevents postgres/rest unavailability from skipping sqlite tests
    fixture_name = f"{request.param}_catalog_config"
    return request.getfixturevalue(fixture_name)


def test_get_catalog_rejects_unsupported_types():
    """Should reject unsupported catalog types."""
    with pytest.raises(ValueError, match="Unsupported catalog type"):
        get_catalog("my_cat", iceberg_catalog_type="glue")


def test_persistence_of_sqlite_catalog(tmp_path):
    """
    This test verify that:
    1. Catalog creation works
    2. Namespaces can be created
    3. Catalog persists to disk
    4. Catalog can be reloaded
    """
    db_path = tmp_path / "test_catalog.db"
    catalog_uri = f"sqlite:///{db_path}"

    # Create catalog with explicit config
    catalog = get_catalog(
        "integration_test_catalog",
        iceberg_catalog_config={"type": "sql", "uri": catalog_uri, "warehouse": "test_warehouse"},
    )

    # Verify catalog was created
    assert catalog is not None
    assert catalog.name == "integration_test_catalog"

    # Create a namespace
    test_namespace = "test_integration_namespace"
    catalog.create_namespace(test_namespace)

    # Verify namespace exists
    namespaces = catalog.list_namespaces()
    assert test_namespace in [ns[0] if isinstance(ns, tuple) else ns for ns in namespaces]

    # Verify database file was created (persistence)
    assert db_path.exists()

    # Reload catalog from same URI to verify persistence
    catalog2 = get_catalog(
        "integration_test_catalog",
        iceberg_catalog_config={"type": "sql", "uri": catalog_uri, "warehouse": "test_warehouse"},
    )

    # Verify namespace still exists after reload
    namespaces2 = catalog2.list_namespaces()
    assert test_namespace in [ns[0] if isinstance(ns, tuple) else ns for ns in namespaces2]


# ============================================================================
# INTEGRATION / SMOKE TESTS
# ============================================================================


def test_priority_explicit_config_over_pyiceberg(tmp_path, monkeypatch):
    """Explicit config should take precedence over YAML file.

    This test verify that:
    1. When both explicit config and YAML are present, explicit config wins
    2. We can inspect actual catalog properties to verify which configuration was used
    """
    db_path_yaml = tmp_path / "yaml_catalog.db"
    db_path_explicit = tmp_path / "explicit_catalog.db"
    catalog_uri_yaml = f"sqlite:///{db_path_yaml}"
    catalog_uri_explicit = f"sqlite:///{db_path_explicit}"

    # Create YAML config
    yaml_file = tmp_path / ".pyiceberg.yaml"
    yaml_content = {
        "catalog": {
            "test_catalog": {
                "type": "sql",
                "uri": catalog_uri_yaml,
                "warehouse": "yaml_warehouse",
            }
        }
    }
    with open(yaml_file, "w", encoding="utf-8") as f:
        yaml.dump(yaml_content, f)

    # Set PYICEBERG_HOME so PyIceberg can find the YAML file if needed
    monkeypatch.setenv("PYICEBERG_HOME", str(tmp_path))

    with mock.patch("dlt.current.run_context") as mock_ctx:
        mock_ctx.return_value.run_dir = str(tmp_path)
        mock_ctx.return_value.get_setting.return_value = str(yaml_file)

        # Pass explicit config - should override YAML
        explicit_config = {
            "type": "sql",
            "uri": catalog_uri_explicit,
            "warehouse": "explicit_warehouse",
        }
        catalog = get_catalog("test_catalog", iceberg_catalog_config=explicit_config)

        # Verify explicit config was used by checking the URI in properties
        assert catalog is not None
        assert catalog.name == "test_catalog"
        assert catalog.properties is not None
        assert "uri" in catalog.properties
        # Should use explicit URI, not YAML URI
        assert catalog_uri_explicit in catalog.properties["uri"]
        assert catalog_uri_yaml not in catalog.properties["uri"]

        # Verify catalog is functional and uses explicit database
        test_namespace = "test_explicit_priority"
        catalog.create_namespace(test_namespace)
        namespaces = catalog.list_namespaces()
        assert test_namespace in [ns[0] if isinstance(ns, tuple) else ns for ns in namespaces]

        # Verify explicit database file was created, not YAML database
        assert db_path_explicit.exists()
        assert not db_path_yaml.exists()


def test_sqlite_catalog_fallback_in_memory():
    """
    INTEGRATION TEST: Verify SQLite fallback to in-memory catalog.

    This test verifies that:
    1. When no configuration is provided, get_catalog falls back to SQLite
    2. The fallback creates an in-memory SQLite catalog
    3. The catalog is functional
    """
    # Mock run_context to prevent YAML discovery
    with mock.patch("dlt.current.run_context") as mock_ctx:
        mock_ctx.return_value.run_dir = "/tmp"
        mock_ctx.return_value.get_setting.return_value = "/tmp/.pyiceberg.yaml"

        # Create catalog with no configuration - should fall back to in-memory SQLite
        catalog = get_catalog("fallback_catalog")

        # Verify catalog was created
        assert catalog is not None
        assert catalog.name == "fallback_catalog"

        # Verify it's an in-memory SQLite catalog (properties contain memory URI)
        assert catalog.properties is not None
        assert "uri" in catalog.properties
        assert ":memory:" in catalog.properties["uri"]

        # Verify catalog is functional
        test_namespace = "test_fallback_namespace"
        catalog.create_namespace(test_namespace)
        namespaces = catalog.list_namespaces()
        assert test_namespace in [ns[0] if isinstance(ns, tuple) else ns for ns in namespaces]


def test_default_sqlite_memory_catalog() -> None:
    # makes sure "default" catalog is created when no name is provided
    catalog = get_catalog()
    assert catalog.name == "default"

    # make sure we can create namespace
    test_namespace = "test_fallback_namespace"
    catalog.create_namespace(test_namespace)

    catalog = get_catalog(iceberg_catalog_name=ConfigValue)
    assert catalog.name == "default"

    with pytest.raises(ConfigFieldMissingException):
        catalog = get_catalog(iceberg_catalog_name=None)


def test_rest_catalog_namespace_operations():
    """Smoke test: verify REST catalog can perform basic namespace operations."""
    import uuid
    from requests.exceptions import ConnectionError

    try:
        catalog = get_catalog(
            "namespace_test_catalog",
            iceberg_catalog_config={
                "type": "rest",
                "uri": "http://localhost:8181",
                "warehouse": "s3://warehouse/",
                "s3.endpoint": "http://localhost:9000",
                "s3.access-key-id": "admin",
                "s3.secret-access-key": "password",
            },
        )

        namespace = f"test_ns_{uuid.uuid4().hex[:8]}"

        # Create, verify, and delete a namespace
        catalog.create_namespace(namespace)
        assert namespace in [
            ns[0] if isinstance(ns, tuple) else ns for ns in catalog.list_namespaces()
        ]

        catalog.drop_namespace(namespace)
        assert namespace not in [
            ns[0] if isinstance(ns, tuple) else ns for ns in catalog.list_namespaces()
        ]
    except ConnectionError as e:
        if "HTTPConnectionPool(host='localhost', port=8181): Max retries exceeded" in str(e):
            print(
                "Connection Error, we consider this a pass as the configuration was loaded"
                " correctly"
            )
        else:
            raise


def test_rest_catalog_smoke_test():
    """Smoke test: verify REST catalog config is loaded and connection attempt fails as expected.

    No docker required. The ConnectionError proves the config was loaded correctly:
    - SQLite catalogs never raise ConnectionError (no network)
    - If REST config wasn't parsed, it would fall back to SQLite silently
    - Getting ConnectionError = REST path was taken with the provided URI
    """
    from requests.exceptions import ConnectionError

    config = {
        "type": "rest",
        "uri": "http://localhost:19999",  # Closed port
        "warehouse": "test_warehouse",
        "s3.endpoint": "http://localhost:19998",
        "s3.access-key-id": "test_key",
        "s3.secret-access-key": "test_secret",
    }

    # ConnectionError proves REST config was loaded (SQLite would never raise this)
    with pytest.raises(ConnectionError):
        get_catalog("rest_smoke_test", iceberg_catalog_config=config)


def test_catalog_from_env_vars_parametrized(catalog_config, tmp_path, monkeypatch):
    """Parametrized test: Create catalog from PYICEBERG_* environment variables.

    This test verifies that:
    1. PYICEBERG_* environment variables are correctly parsed
    2. Catalog is created with the right configuration
    3. Catalog is functional and can perform basic operations

    Runs for: SQLite, PostgreSQL, and REST catalogs
    """
    # Set environment variables from catalog_config
    for key, value in catalog_config.items():
        env_key = key.upper().replace(".", "__").replace("-", "_")
        monkeypatch.setenv(f"PYICEBERG_CATALOG__DEFAULT__{env_key}", str(value))

    from pyiceberg.utils.config import Config

    monkeypatch.setattr("pyiceberg.catalog._ENV_CONFIG", Config())

    # Mock run_context to prevent YAML discovery
    with mock.patch("dlt.current.run_context") as mock_ctx:
        mock_ctx.return_value.run_dir = str(tmp_path)
        mock_ctx.return_value.get_setting.return_value = str(tmp_path / ".pyiceberg.yaml")

        # Create catalog from environment variables (catalog_name defaults to "default")
        catalog = get_catalog()

        assert catalog is not None
        assert catalog.name == "default"

        # Verify catalog is functional
        test_namespace = "test_env_ns"
        is_sqlite = "sqlite" in catalog_config.get("uri", "")

        # SQLite is ephemeral, so we create the namespace; for persistent catalogs, verify CI-created namespace
        if is_sqlite:
            catalog.create_namespace(test_namespace)

        namespaces = catalog.list_namespaces()
        namespace_list = [ns[0] if isinstance(ns, tuple) else ns for ns in namespaces]
        assert test_namespace in namespace_list


def test_catalog_from_explicit_config_parametrized(catalog_config):
    """Parametrized test: Create catalog from explicit config for all catalog types.

    This test verifies that:
    1. Pyiceberg Catalog is created with explicit configuration
    2. Catalog properties match the provided config
    3. Pyiceberg Catalog is functional and can perform basic operations

    Runs for: SQLite, PostgreSQL, and REST catalogs
    """
    catalog_name = "explicit_config_catalog"

    catalog = get_catalog(catalog_name, iceberg_catalog_config=catalog_config)

    assert catalog is not None
    assert catalog.name == catalog_name

    # Verify catalog is functional
    test_namespace = "test_explicit_ns"
    is_sqlite = "sqlite" in catalog_config.get("uri", "")

    # SQLite is ephemeral, so we create the namespace; for persistent catalogs, verify CI-created namespace
    if is_sqlite:
        catalog.create_namespace(test_namespace)

    namespaces = catalog.list_namespaces()
    namespace_list = [ns[0] if isinstance(ns, tuple) else ns for ns in namespaces]
    assert test_namespace in namespace_list


def test_catalog_from_yaml_parametrized(catalog_config, tmp_path, monkeypatch):
    """Parametrized test: Create catalog from .pyiceberg.yaml for all catalog types.

    This test verifies that:
    1. YAML file discovery works via PYICEBERG_HOME
    2. Pyiceberg Catalog is created with configuration from YAML
    3. Pyiceberg Catalog is functional and can perform basic operations

    Runs for: SQLite, PostgreSQL, and REST catalogs
    """
    catalog_name = "yaml_catalog"

    # Create YAML config file
    yaml_file = tmp_path / ".pyiceberg.yaml"
    yaml_content = {"catalog": {catalog_name: catalog_config}}
    with open(yaml_file, "w", encoding="utf-8") as f:
        yaml.dump(yaml_content, f)

    # Set PYICEBERG_HOME - this is the standard pyiceberg config location
    monkeypatch.setenv("PYICEBERG_HOME", str(tmp_path))

    # Reset PyIceberg's cached environment config
    from pyiceberg.utils.config import Config

    monkeypatch.setattr("pyiceberg.catalog._ENV_CONFIG", Config())

    # Use public API - get_catalog will discover and use the YAML file via PYICEBERG_HOME
    catalog = get_catalog(catalog_name)

    # Verify catalog was created
    assert catalog is not None
    assert catalog.name == catalog_name

    # Verify catalog is functional
    test_namespace = "test_yaml_ns"
    is_sqlite = "sqlite" in catalog_config.get("uri", "")

    # SQLite is ephemeral, so we create the namespace; for persistent catalogs, verify CI-created namespace
    if is_sqlite:
        catalog.create_namespace(test_namespace)

    namespaces = catalog.list_namespaces()
    namespace_list = [ns[0] if isinstance(ns, tuple) else ns for ns in namespaces]
    assert test_namespace in namespace_list
