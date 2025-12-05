
import os
import yaml
import pytest
from unittest import mock
from pathlib import Path
from typing import Any, Dict, Optional

from dlt.common.exceptions import MissingDependencyException
from dlt.common.storages.configuration import FileSystemCredentials
# Import the module to test
from dlt.common.libs.pyiceberg import (
    get_catalog,
    load_catalog_from_yaml,
    load_catalog_from_env,
    CatalogNotFoundError,
)

# --- Fixtures ---

@pytest.fixture
def mock_load_catalog():
    """Mock the upstream pyiceberg load_catalog function."""
    # We patch pyiceberg.catalog.load_catalog where it is used/imported in our module or upstream
    # Since the module does `from pyiceberg.catalog import load_catalog` inside functions, 
    # we should patch it where it is looked up.
    with mock.patch("pyiceberg.catalog.load_catalog") as m_upstream:
        yield m_upstream

@pytest.fixture
def mock_sql_catalog():
    """Mock the get_sql_catalog helper to verify fallback logic."""
    with mock.patch("dlt.common.libs.pyiceberg.get_sql_catalog") as m:
        yield m

@pytest.fixture
def mock_credentials():
    """Mock filesystem credentials."""
    creds = mock.Mock()
    # When converted to fileio config, return a dummy dict
    creds.to_pyiceberg_fileio_config.return_value = {"s3.access-key-id": "mock_key"}
    # Make it look like an instance of WithPyicebergConfig for isinstance checks
    from dlt.common.configuration.specs.mixins import WithPyicebergConfig
    creds.__class__ = WithPyicebergConfig 
    return creds

# --- Tests ---

def test_load_catalog_from_config_dict(mock_load_catalog):
    """Test loading directly from a provided configuration dictionary."""
    config = {
        "type": "rest",
        "uri": "https://catalog.example.com",
        "warehouse": "my_warehouse",
    }
    
    get_catalog("my_catalog", iceberg_catalog_config=config)
    
    mock_load_catalog.assert_called_once_with("my_catalog", **config)

def test_load_catalog_from_yaml_file(mock_load_catalog, tmp_path, mock_credentials):
    """
    Test loading from a REAL .pyiceberg.yaml file using tmp_path.
    This replaces the brittle `mock_open` tests.
    """
    # 1. Create a real YAML file in the temporary directory
    config_path = tmp_path / ".pyiceberg.yaml"
    yaml_content = {
        "catalog": {
            "test_cat": {
                "type": "rest", 
                "uri": "https://real-file.example.com",
                "warehouse": "my_wh"
            }
        }
    }
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(yaml_content, f)

    # 2. Invoke the internal loader directly to verify it reads the file
    # We pass the path explicitly to avoid patching search paths
    load_catalog_from_yaml("test_cat", config_path=str(config_path), credentials=mock_credentials)
    
    # 3. Verify credentials were merged and catalog loaded
    mock_load_catalog.assert_called_once_with(
        "test_cat",
        type="rest",
        uri="https://real-file.example.com",
        warehouse="my_wh",
        **{"s3.access-key-id": "mock_key"}  # From mock_credentials
    )

def test_load_catalog_from_yaml_missing_file(tmp_path):
    """Verify correct error when YAML file doesn't exist."""
    non_existent = tmp_path / "missing.yaml"
    with pytest.raises(CatalogNotFoundError) as exc:
        load_catalog_from_yaml("my_cat", config_path=str(non_existent))
    assert "No .pyiceberg.yaml file found" in str(exc.value)

def test_load_catalog_from_yaml_missing_catalog(tmp_path):
    """Verify correct error when file exists but catalog is missing."""
    config_path = tmp_path / ".pyiceberg.yaml"
    with open(config_path, "w") as f:
        yaml.dump({"catalog": {"other_cat": {}}}, f)
        
    with pytest.raises(CatalogNotFoundError) as exc:
        load_catalog_from_yaml("my_cat", config_path=str(config_path))
    assert "Catalog 'my_cat' not found" in str(exc.value)

@pytest.mark.parametrize("catalog_type,extra_env,expected_kwargs", [
    ("rest", {
        "DLT_ICEBERG_CATALOG_URI": "https://rest.example.com", 
        "DLT_ICEBERG_CATALOG_WAREHOUSE": "rest_wh",
        "DLT_ICEBERG_CATALOG_PROP_TOKEN": "abc"
    }, {
        "type": "rest", 
        "uri": "https://rest.example.com", 
        "warehouse": "rest_wh", 
        "token": "abc"
    }),
    ("sql", {
        "DLT_ICEBERG_CATALOG_URI": "sqlite:///persistent.db"
    }, {
        "type": "sql", 
        "uri": "sqlite:///persistent.db"
    })
])
def test_load_catalog_from_env_vars(mock_load_catalog, catalog_type, extra_env, expected_kwargs):
    """Test loading catalog configuration from environment variables."""
    env_vars = {
        "DLT_ICEBERG_CATALOG_TYPE": catalog_type,
        **extra_env
    }
    
    with mock.patch.dict(os.environ, env_vars):
        # We use a specific name to trigger the env loader (or bypass yaml/config)
        # However, the env loader checks specific DLT_ICEBERG_... keys regardless of catalog name passed
        load_catalog_from_env("env_cat")
        
        mock_load_catalog.assert_called_once_with("env_cat", **expected_kwargs)

def test_priority_config_over_yaml(mock_load_catalog, tmp_path):
    """Verify that explicit config dictionary takes precedence over YAML file."""
    # Create a yaml file that would be valid
    config_path = tmp_path / ".pyiceberg.yaml"
    with open(config_path, "w") as f:
        yaml.dump({"catalog": {"priority_cat": {"type": "file"}}}, f)
        
    # Patch the search path to find our temp file
    with mock.patch("dlt.common.libs.pyiceberg.load_catalog_from_yaml", wraps=load_catalog_from_yaml) as mocked_yaml_loader:
        # Call with BOTH config and the ability to find the yaml
        explicit_config = {"type": "memory"}
        
        # We need to trick the function into finding our yaml without explicit path
        # Easier: just assert that if we pass config, the yaml loader is NOT called 
        # or ignored. But since logic is "if config: return ...", yaml shouldn't be touched.
        
        get_catalog("priority_cat", iceberg_catalog_config=explicit_config)
        
        mock_load_catalog.assert_called_with("priority_cat", **explicit_config)
        mocked_yaml_loader.assert_not_called()

def test_priority_yaml_over_env(mock_load_catalog):
    """Verify that YAML configuration takes precedence over environment variables."""
    # Mock successful YAML load
    with mock.patch("dlt.common.libs.pyiceberg.load_catalog_from_yaml") as mock_yaml_load, \
         mock.patch("dlt.common.libs.pyiceberg.load_catalog_from_env") as mock_env_load:
             
        mock_yaml_load.return_value = mock.Mock()
        
        get_catalog("my_catalog")
        
        mock_yaml_load.assert_called_once()
        mock_env_load.assert_not_called()

def test_fallback_to_sqlite(mock_load_catalog, mock_sql_catalog):
    """Verify fallback to in-memory SQLite when no other config is found."""
    # Ensure clean environment
    with mock.patch.dict(os.environ, {}, clear=True):
        # Force YAML load to fail
        with mock.patch("dlt.common.libs.pyiceberg.load_catalog_from_yaml", side_effect=CatalogNotFoundError("No yaml")):
            
            get_catalog("fallback_catalog")
            
            mock_sql_catalog.assert_called_once_with(
                "fallback_catalog", "sqlite:///:memory:", None
            )

def test_persistent_sqlite_catalog_via_fallback(mock_load_catalog, mock_sql_catalog):
    """Verify that we can specify a persistent SQLite URI via arguments (fallback path)."""
    uri = "sqlite:////absolute/path/to/catalog.db"
    
    # Ensure clean environment/no yaml
    with mock.patch.dict(os.environ, {}, clear=True), \
         mock.patch("dlt.common.libs.pyiceberg.load_catalog_from_yaml", side_effect=CatalogNotFoundError):
        
        # Pass the URI directly as argument
        get_catalog("persistent_sqlite", iceberg_catalog_uri=uri)
        
        mock_sql_catalog.assert_called_once_with(
            "persistent_sqlite",
            uri,
            None
        )

def test_credentials_merging(mock_load_catalog, mock_credentials):
    """Verify that FileSystemCredentials are merged into the catalog config."""
    config = {"type": "rest", "uri": "https://creds.example.com"}
    
    get_catalog("secure_cat", iceberg_catalog_config=config, credentials=mock_credentials)
    
    mock_load_catalog.assert_called_once()
    call_kwargs = mock_load_catalog.call_args[1]
    assert call_kwargs["type"] == "rest"
    assert call_kwargs["s3.access-key-id"] == "mock_key"

def test_postgres_catalog_config(mock_load_catalog):
    """Test Postgres catalog configuration (regression test for user scenario)."""
    uri = "postgresql+psycopg2://user:password@localhost/dbname"
    config = {
        "type": "sql",
        "uri": uri
    }
    
    get_catalog("postgres_catalog", iceberg_catalog_config=config)
    
    mock_load_catalog.assert_called_once_with(
        "postgres_catalog",
        type="sql",
        uri=uri
    )

def test_dlt_config_injection_rest_bug(mock_load_catalog, mock_sql_catalog):
    """
    Demonstrate current limitation: DLT-injected config via @with_config 
    (env vars like ICEBERG_CATALOG__...) falls back to SQL catalog even for REST.
    """
    # dlt maps env vars like ICEBERG_CATALOG__ICEBERG_CATALOG_URI to arguments
    dlt_env_vars = {
        "ICEBERG_CATALOG__ICEBERG_CATALOG_NAME": "dlt_config_cat",
        "ICEBERG_CATALOG__ICEBERG_CATALOG_TYPE": "rest",
        "ICEBERG_CATALOG__ICEBERG_CATALOG_URI": "https://dlt-injected.com",
    }
    
    # Ensure clean environment (no DLT_ICEBERG_... vars) and no yaml
    with mock.patch.dict(os.environ, dlt_env_vars, clear=True), \
         mock.patch("dlt.common.libs.pyiceberg.load_catalog_from_yaml", side_effect=CatalogNotFoundError):
        
        get_catalog()
        
        # Current behavior: It falls back to SQL catalog because `load_catalog_from_env`
        # looks for DLT_ICEBERG_... vars, not the injected args.
        # And the fallback (Priority 4) hardcodes get_sql_catalog.
        
        mock_sql_catalog.assert_called_once_with(
            "dlt_config_cat",
            "https://dlt-injected.com",
            None
        )
        mock_load_catalog.assert_not_called()
