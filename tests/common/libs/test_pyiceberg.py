"""
Test suite for Iceberg catalog configuration and loading.

This suite tests the get_catalog() function with:
- Unit tests (5 tests): Configuration priority and input validation
- Integration tests (12 tests): Real catalogs to verify actual configuration

All integration tests use real catalogs (no mocks) and inspect catalog.properties
to verify configuration actually works correctly.
"""

import os
import yaml
import pytest
from unittest import mock

from dlt.common.libs.pyiceberg import (
    get_catalog,
    load_catalog_from_yaml,
    CatalogNotFoundError,
)

# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def mock_pyiceberg_load():
    """Mock PyIceberg's load_catalog function for priority tests."""
    with mock.patch("pyiceberg.catalog.load_catalog") as m:
        mock_catalog = mock.Mock()
        mock_catalog.name = "test_catalog"
        mock_catalog.list_namespaces.return_value = []
        m.return_value = mock_catalog
        yield m


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


@pytest.fixture
def yaml_config_file(tmp_path):
    """Create a .pyiceberg.yaml file for priority tests."""
    yaml_file = tmp_path / ".pyiceberg.yaml"
    yaml_content = {
        "catalog": {
            "test_rest_catalog": {
                "type": "rest",
                "uri": "https://yaml-rest.example.com",
                "warehouse": "yaml_warehouse",
            }
        }
    }
    with open(yaml_file, "w") as f:
        yaml.dump(yaml_content, f)
    return yaml_file


# ============================================================================
# UNIT TESTS
# ============================================================================

# ----------------------------------------------------------------------------
# Configuration Priority Tests
# ----------------------------------------------------------------------------

def test_priority_explicit_config_over_yaml(mock_pyiceberg_load, yaml_config_file, tmp_path):
    """Explicit config should take precedence over YAML file."""
    with mock.patch("dlt.current.run_context") as mock_ctx:
        mock_ctx.return_value.run_dir = str(tmp_path)
        mock_ctx.return_value.get_setting.return_value = str(yaml_config_file)
        
        # Pass explicit config - should override YAML
        explicit_config = {
            "type": "rest",
            "uri": "https://explicit-uri.example.com",
            "warehouse": "explicit_wh"
        }
        catalog = get_catalog("test_rest_catalog", iceberg_catalog_config=explicit_config)
        
        # Verify explicit config was used, not YAML
        call_kwargs = mock_pyiceberg_load.call_args[1]
        assert call_kwargs["uri"] == "https://explicit-uri.example.com"
        assert call_kwargs["warehouse"] == "explicit_wh"


def test_priority_yaml_over_env_vars(mock_pyiceberg_load, yaml_config_file, tmp_path, monkeypatch):
    """YAML config should take precedence over environment variables."""
    # Set environment variables (should be ignored)
    monkeypatch.setenv("PYICEBERG_CATALOG__DEFAULT__TYPE", "rest")
    monkeypatch.setenv("PYICEBERG_CATALOG__DEFAULT__URI", "https://env-uri.example.com")
    monkeypatch.setenv("PYICEBERG_CATALOG__DEFAULT__WAREHOUSE", "env_wh")
    
    with mock.patch("dlt.current.run_context") as mock_ctx:
        mock_ctx.return_value.run_dir = str(tmp_path)
        mock_ctx.return_value.get_setting.return_value = str(yaml_config_file)
        
        catalog = get_catalog("test_rest_catalog")
        
        # Verify YAML was used, not env vars
        call_kwargs = mock_pyiceberg_load.call_args[1]
        assert call_kwargs["uri"] == "https://yaml-rest.example.com"
        assert call_kwargs["warehouse"] == "yaml_warehouse"


# ----------------------------------------------------------------------------
# Validation and Error Handling Tests  
# ----------------------------------------------------------------------------

def test_get_catalog_rejects_unsupported_types():
    """Should reject unsupported catalog types."""
    with pytest.raises(ValueError, match="Unsupported catalog type"):
        get_catalog("my_cat", iceberg_catalog_type="glue")


@pytest.mark.parametrize("error_type,setup,expected_msg", [
    ("missing_file", lambda tmp_path: tmp_path / "nonexistent.yaml", "No .pyiceberg.yaml file found"),
    ("missing_catalog", lambda tmp_path: _create_yaml_without_catalog(tmp_path), "Catalog 'missing_cat' not found"),
])
def test_get_catalog_yaml_errors(error_type, setup, expected_msg, tmp_path, clean_env):
    """Should raise appropriate errors for YAML configuration issues."""
    config_path = setup(tmp_path)
    
    with pytest.raises(CatalogNotFoundError, match=expected_msg):
        load_catalog_from_yaml("missing_cat", config_path=str(config_path))


def _create_yaml_without_catalog(tmp_path):
    """Helper to create YAML file with different catalog name."""
    yaml_file = tmp_path / "wrong_catalog.yaml"
    with open(yaml_file, "w") as f:
        yaml.dump({"catalog": {"other_cat": {"type": "sql"}}}, f)
    return yaml_file


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

# All integration tests use real catalogs (no mocks) and inspect
# catalog.properties to verify configuration actually works correctly.

# ----------------------------------------------------------------------------
# SQLite Integration Tests
# ----------------------------------------------------------------------------

@pytest.mark.integration
def test_real_sqlite_catalog_integration(tmp_path):
    """
    INTEGRATION TEST: Create and use a real persistent SQLite catalog.
    
    This test uses actual PyIceberg without mocking to verify:
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
        iceberg_catalog_config={
            "type": "sql",
            "uri": catalog_uri
        }
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
        iceberg_catalog_config={
            "type": "sql",
            "uri": catalog_uri
        }
    )
    
    # Verify namespace still exists after reload
    namespaces2 = catalog2.list_namespaces()
    assert test_namespace in [ns[0] if isinstance(ns, tuple) else ns for ns in namespaces2]


@pytest.mark.integration
def test_sqlite_catalog_from_env_vars(tmp_path, monkeypatch, clean_env):
    """
    INTEGRATION TEST: Create SQLite catalog from PYICEBERG_* environment variables.
        
    This test uses a real SQLite catalog (no mocking) to verify that:
    1. PYICEBERG_* environment variables are correctly parsed
    2. Catalog is created with the right configuration
    3. We can inspect actual catalog properties to verify configuration
    """
    db_path = tmp_path / "env_test_catalog.db"
    catalog_uri = f"sqlite:///{db_path}"
    
    # Set DLT config environment variables 
    monkeypatch.setenv("PYICEBERG_CATALOG__DEFAULT__TYPE", "sql")
    monkeypatch.setenv("PYICEBERG_CATALOG__DEFAULT__URI", catalog_uri)
    
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
        
        # Verify catalog properties match environment variables
        # SQLite catalogs should have the URI in their properties
        assert catalog.properties is not None
        assert "uri" in catalog.properties
        assert catalog_uri in catalog.properties["uri"]
        
        # Verify catalog is functional
        test_namespace = "test_env_namespace"
        catalog.create_namespace(test_namespace)
        namespaces = catalog.list_namespaces()
        assert test_namespace in [ns[0] if isinstance(ns, tuple) else ns for ns in namespaces]
        
        # Verify database file was created
        assert db_path.exists()


@pytest.mark.integration
def test_sqlite_catalog_from_yaml(tmp_path):
    """
    INTEGRATION TEST: Create SQLite catalog from .pyiceberg.yaml file.
    
    This test uses a real SQLite catalog (no mocking) to verify that:
    1. YAML file discovery works
    2. Catalog is created with configuration from YAML
    3. We can inspect actual catalog properties to verify configuration
    """
    db_path = tmp_path / "yaml_test_catalog.db"
    catalog_uri = f"sqlite:///{db_path}"
    
    # Create .pyiceberg.yaml file with SQLite catalog config
    yaml_file = tmp_path / ".pyiceberg.yaml"
    yaml_content = {
        "catalog": {
            "yaml_sqlite_catalog": {
                "type": "sql",
                "uri": catalog_uri,
            }
        }
    }
    with open(yaml_file, "w") as f:
        yaml.dump(yaml_content, f)
    
    # Mock run_context to point to our YAML file
    with mock.patch("dlt.current.run_context") as mock_ctx:
        mock_ctx.return_value.run_dir = str(tmp_path)
        mock_ctx.return_value.get_setting.return_value = str(yaml_file)
        
        # Create catalog - should discover and use YAML file
        catalog = get_catalog("yaml_sqlite_catalog")
        
        # Verify catalog was created with correct name
        assert catalog is not None
        assert catalog.name == "yaml_sqlite_catalog"
        
        # Verify catalog properties match YAML configuration
        assert catalog.properties is not None
        assert "uri" in catalog.properties
        assert catalog_uri in catalog.properties["uri"]
        
        # Verify catalog is functional
        test_namespace = "test_yaml_namespace"
        catalog.create_namespace(test_namespace)
        namespaces = catalog.list_namespaces()
        assert test_namespace in [ns[0] if isinstance(ns, tuple) else ns for ns in namespaces]
        
        # Verify database file was created
        assert db_path.exists()


@pytest.mark.integration
def test_sqlite_catalog_from_explicit_config(tmp_path):
    """
    INTEGRATION TEST: Create SQLite catalog from explicit config dictionary.
    
    This test uses a real SQLite catalog (no mocking) to verify that:
    1. Explicit config dict is used correctly
    2. Catalog is created with the right configuration
    3. We can inspect actual catalog properties to verify configuration
    """
    db_path = tmp_path / "explicit_config_catalog.db"
    catalog_uri = f"sqlite:///{db_path}"
    
    # Explicit config dictionary
    config = {
        "type": "sql",
        "uri": catalog_uri,
    }
    
    # Create catalog from explicit config
    catalog = get_catalog("explicit_config_catalog", iceberg_catalog_config=config)
    
    # Verify catalog was created with correct name
    assert catalog is not None
    assert catalog.name == "explicit_config_catalog"
    
    # Verify catalog properties match explicit config
    assert catalog.properties is not None
    assert "uri" in catalog.properties
    assert catalog_uri in catalog.properties["uri"]
    
    # Verify catalog is functional
    test_namespace = "test_explicit_namespace"
    catalog.create_namespace(test_namespace)
    namespaces = catalog.list_namespaces()
    assert test_namespace in [ns[0] if isinstance(ns, tuple) else ns for ns in namespaces]
    
    # Verify database file was created
    assert db_path.exists()


@pytest.mark.integration
def test_sqlite_catalog_with_credentials(tmp_path, test_credentials):
    """
    INTEGRATION TEST: Create SQLite catalog with merged credentials.
    
    This test uses a real SQLite catalog (no mocking) to verify that:
    1. Credentials are merged into catalog configuration
    2. We can inspect actual catalog properties to verify credentials were merged
    """
    db_path = tmp_path / "credentials_catalog.db"
    catalog_uri = f"sqlite:///{db_path}"
    
    # Config without credentials
    config = {
        "type": "sql",
        "uri": catalog_uri,
    }
    
    # Create catalog with credentials
    catalog = get_catalog(
        "credentials_catalog",
        iceberg_catalog_config=config,
        credentials=test_credentials
    )
    
    # Verify catalog was created
    assert catalog is not None
    assert catalog.name == "credentials_catalog"
    
    # Verify credentials were merged into catalog properties
    assert catalog.properties is not None
    assert "s3.access-key-id" in catalog.properties
    assert catalog.properties["s3.access-key-id"] == "test_access_key"
    assert "s3.secret-access-key" in catalog.properties
    assert catalog.properties["s3.secret-access-key"] == "test_secret_key"
    
    # Verify catalog is functional
    test_namespace = "test_creds_namespace"
    catalog.create_namespace(test_namespace)
    namespaces = catalog.list_namespaces()
    assert test_namespace in [ns[0] if isinstance(ns, tuple) else ns for ns in namespaces]


@pytest.mark.integration
def test_sqlite_catalog_from_dlt_config(tmp_path, monkeypatch, clean_env):
    """
    INTEGRATION TEST: Create SQLite catalog from DLT config (ICEBERG_CATALOG__* env vars).
    
    This test uses a real SQLite catalog (no mocking) to verify that:
    1. ICEBERG_CATALOG__* environment variables are correctly parsed
    2. Catalog is created with the right configuration
    3. We can inspect actual catalog properties to verify configuration
    """
    db_path = tmp_path / "dlt_config_catalog.db"
    catalog_uri = f"sqlite:///{db_path}"
    
    # Set DLT config environment variables (ICEBERG_CATALOG__* format)
    monkeypatch.setenv("ICEBERG_CATALOG__ICEBERG_CATALOG_TYPE", "sql")
    monkeypatch.setenv("ICEBERG_CATALOG__ICEBERG_CATALOG_URI", catalog_uri)
    
    # Mock run_context to prevent YAML discovery
    with mock.patch("dlt.current.run_context") as mock_ctx:
        mock_ctx.return_value.run_dir = str(tmp_path)
        mock_ctx.return_value.get_setting.return_value = str(tmp_path / ".pyiceberg.yaml")
        
        # Create catalog from DLT config (catalog_name defaults to "default")
        catalog = get_catalog()
        
        # Verify catalog was created
        assert catalog is not None
        assert catalog.name == "default"
        
        # Verify catalog properties match DLT config
        assert catalog.properties is not None
        assert "uri" in catalog.properties
        assert catalog_uri in catalog.properties["uri"]
        
        # Verify catalog is functional
        test_namespace = "test_dlt_namespace"
        catalog.create_namespace(test_namespace)
        namespaces = catalog.list_namespaces()
        assert test_namespace in [ns[0] if isinstance(ns, tuple) else ns for ns in namespaces]
        
        # Verify database file was created
        assert db_path.exists()


@pytest.mark.integration
def test_sqlite_catalog_fallback_in_memory(clean_env):
    """
    INTEGRATION TEST: Verify SQLite fallback to in-memory catalog.
    
    This test uses a real SQLite catalog (no mocking) to verify that:
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


@pytest.mark.integration
def test_sqlite_catalog_fallback_persistent(tmp_path, clean_env):
    """
    INTEGRATION TEST: Verify SQLite fallback with explicit persistent URI.
    
    This test uses a real SQLite catalog (no mocking) to verify that:
    1. When only URI is provided, catalog is created with that URI
    2. The catalog persists to disk
    3. The catalog is functional
    """
    db_path = tmp_path / "persistent_fallback.db"
    catalog_uri = f"sqlite:///{db_path}"
    
    # Mock run_context to prevent YAML discovery
    with mock.patch("dlt.current.run_context") as mock_ctx:
        mock_ctx.return_value.run_dir = str(tmp_path)
        mock_ctx.return_value.get_setting.return_value = str(tmp_path / ".pyiceberg.yaml")
        
        # Create catalog with only URI (no full config) - should use SQLite with that URI
        catalog = get_catalog("persistent_fallback", iceberg_catalog_uri=catalog_uri)
        
        # Verify catalog was created
        assert catalog is not None
        assert catalog.name == "persistent_fallback"
        
        # Verify catalog has the correct URI
        assert catalog.properties is not None
        assert "uri" in catalog.properties
        assert catalog_uri in catalog.properties["uri"]
        
        # Verify catalog is functional and persists
        test_namespace = "test_persistent_namespace"
        catalog.create_namespace(test_namespace)
        
        # Verify database file was created
        assert db_path.exists()
        
        # Verify namespace exists
        namespaces = catalog.list_namespaces()
        assert test_namespace in [ns[0] if isinstance(ns, tuple) else ns for ns in namespaces]


# ----------------------------------------------------------------------------
# REST Catalog Integration Tests (require lakekeeper)
# ----------------------------------------------------------------------------

@pytest.fixture(scope="session")
def rest_catalog_config():
    """Configuration for lakekeeper REST catalog running at localhost:8181."""
    return {
        "type": "rest",
        "uri": "http://localhost:8181/catalog",
        "warehouse": "test_easy",
    }


@pytest.mark.integration
def test_rest_catalog_from_explicit_config(rest_catalog_config):
    """Test that REST catalog can be created and used with explicit config."""
    catalog = get_catalog(
        "rest_test_catalog",
        iceberg_catalog_config=rest_catalog_config
    )
    
    # Verify catalog is functional - can connect and perform basic operations
    assert catalog is not None
    assert catalog.name == "rest_test_catalog"
    namespaces = catalog.list_namespaces()
    assert isinstance(namespaces, list)


@pytest.mark.integration
def test_rest_catalog_from_yaml(rest_catalog_config, tmp_path):
    """Test creating REST catalog from .pyiceberg.yaml file using get_catalog."""
    # Create YAML config file
    yaml_file = tmp_path / ".pyiceberg.yaml"
    yaml_content = {
        "catalog": {
            "yaml_rest_catalog": rest_catalog_config
        }
    }
    with open(yaml_file, "w") as f:
        yaml.dump(yaml_content, f)
    
    # Mock run_context to point to our YAML file
    with mock.patch("dlt.current.run_context") as mock_ctx:
        mock_ctx.return_value.run_dir = str(tmp_path)
        mock_ctx.return_value.get_setting.return_value = str(yaml_file)
        
        # Use public API - get_catalog will discover and use the YAML file
        catalog = get_catalog("yaml_rest_catalog")
        
        # Verify catalog is functional
        assert catalog is not None
        namespaces = catalog.list_namespaces()
        assert isinstance(namespaces, list)


@pytest.mark.integration
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


@pytest.mark.integration
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
