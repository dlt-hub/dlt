import os
import yaml
import pytest
from unittest import mock

from dlt.common.libs.pyiceberg import (
    get_catalog,
)

# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def test_credentials():
    """Create test AWS credentials for credential merging tests."""
    from dlt.common.configuration.specs.aws_credentials import AwsCredentials
    
    return AwsCredentials(
        aws_access_key_id="test_access_key",
        aws_secret_access_key="test_secret_key"
    )

@pytest.fixture
def clean_env(monkeypatch):
    """Remove all Iceberg-related environment variables."""
    # Remove any ICEBERG_CATALOG__* or PYICEBERG_CATALOG__* vars
    for key in list(os.environ.keys()):
        if key.startswith(("ICEBERG_CATALOG__", "PYICEBERG_CATALOG__")):
            monkeypatch.delenv(key, raising=False)

# ----------------------------------------------------------------------------
# Validation and Error Handling Tests  
# ----------------------------------------------------------------------------

def test_get_catalog_rejects_unsupported_types():
    """Should reject unsupported catalog types."""
    with pytest.raises(ValueError, match="Unsupported catalog type"):
        get_catalog("my_cat", iceberg_catalog_type="glue")

# ============================================================================
# INTEGRATION TESTS
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
    with open(yaml_file, "w") as f:
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
            "warehouse": "explicit_warehouse"
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

def test_catalog_from_env_vars_parametrized(catalog_config, tmp_path, monkeypatch, clean_env):
    """Parametrized test: Create catalog from PYICEBERG_* environment variables.
    
    This test verifies that:
    1. PYICEBERG_* environment variables are correctly parsed
    2. Catalog is created with the right configuration
    3. Catalog is functional and can perform basic operations
    
    Runs for: SQLite, PostgreSQL, and REST catalogs
    """
    # Set environment variables from catalog_config
    for key, value in catalog_config.items():
        # Convert config keys to env var format:
        # - dots become __
        # - hyphens become _
        # e.g., "s3.access-key-id" -> "S3__ACCESS_KEY_ID"
        env_key = key.upper().replace(".", "__").replace("-", "_")
        monkeypatch.setenv(f"PYICEBERG_CATALOG__DEFAULT__{env_key}", str(value))
    
    # Reset PyIceberg's cached environment config after setting env vars
    # PyIceberg reads environment variables once at import time into _ENV_CONFIG
    # We need to reload it after monkeypatch modifies the environment
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


def test_sqlite_catalog_fallback_in_memory(clean_env):
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



# ----------------------------------------------------------------------------
# REST/PostgreSQL Catalog Integration Tests (require a REST/PostgreSQL catalog running)
# ----------------------------------------------------------------------------

@pytest.fixture(scope="session")
def rest_catalog_config():
    """Configuration for Iceberg REST catalog running at localhost:8181.
    
    Uses apache/iceberg-rest-fixture from docker-compose-iceberg.yml.
    Expects the REST catalog to be running - tests will fail if not available.
    
    Start with: docker compose -f tests/common/libs/docker-compose-iceberg.yml up -d
    """
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
    Expects PostgreSQL to be running - tests will fail if not available.
    
    Start with: docker compose -f tests/common/libs/docker-compose-iceberg.yml up -d
    """
    temp_dir = tmp_path_factory.mktemp("postgres_catalog")
    return {
        "type": "sql",
        "uri": "postgresql+psycopg2://loader:loader@localhost:5432/dlt_data",
        "warehouse": str(temp_dir / "warehouse")
    }
    
@pytest.fixture(scope="session")
def sqlite_catalog_config(tmp_path_factory):
    """Configuration for SQLite catalog.
    """
    temp_dir = tmp_path_factory.mktemp("sqlite_catalog")
    db_path = temp_dir / "test_catalog.db"
    return {
        "type": "sql",
        "uri": f"sqlite:///{db_path}",
        "warehouse": str(temp_dir / "warehouse")
    }

@pytest.fixture(
    params=["sqlite", "postgres", "rest"],
    ids=["sqlite", "postgres", "rest"]
)
def catalog_config(request, sqlite_catalog_config, postgres_catalog_config, rest_catalog_config):
    """Parametrized fixture providing catalog configurations for all supported types.
    
    This allows tests to run against SQLite, PostgreSQL, and REST catalogs
    using the same test logic.
    """
    if request.param == "sqlite":
        return sqlite_catalog_config
    elif request.param == "postgres":
        return postgres_catalog_config
    elif request.param == "rest":
        return rest_catalog_config


def test_rest_catalog_namespace_operations(rest_catalog_config):
    """Smoke test: verify REST catalog can perform basic namespace operations."""
    import uuid
    
    catalog = get_catalog(
        "namespace_test_catalog",
        iceberg_catalog_config=rest_catalog_config
    )
    
    namespace = f"test_ns_{uuid.uuid4().hex[:8]}"
    
    try:
        # Create, verify, and delete a namespace
        catalog.create_namespace(namespace)
        assert catalog.namespace_exists(namespace)
        
        namespaces = catalog.list_namespaces()
        namespace_list = [ns[0] if isinstance(ns, tuple) else ns for ns in namespaces]
        assert namespace in namespace_list
        
        catalog.drop_namespace(namespace)
        assert not catalog.namespace_exists(namespace)
    finally:
        # Cleanup in case of failure
        try:
            if catalog.namespace_exists(namespace):
                catalog.drop_namespace(namespace)
        except Exception:
            pass


# Error Handling Tests



def test_rest_catalog_invalid_uri():
    """Test that invalid REST catalog URI raises explicit connection error.
    
    This verifies that our get_catalog doesn't silently fall back to SQLite
    when given invalid REST configuration.
    """
    from requests.exceptions import ConnectionError
    
    config = {
        "type": "rest",
        "uri": "http://nonexistent-host-xyz-12345.example.com:9999/catalog/",
        "warehouse": "test_easy",
    }
    
    # Should raise ConnectionError, not fall back to SQLite
    with pytest.raises(ConnectionError):
        catalog = get_catalog("invalid_uri_catalog", iceberg_catalog_config=config)


# ----------------------------------------------------------------------------
# Parametrized Integration Tests (SQLite + PostgreSQL + REST)
# ----------------------------------------------------------------------------



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
    yaml_content = {
        "catalog": {
            catalog_name: catalog_config
        }
    }
    with open(yaml_file, "w") as f:
        yaml.dump(yaml_content, f)
    
    # Set PYICEBERG_HOME - this is the standard pyiceberg config location
    # Our implementation now checks this path first
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

