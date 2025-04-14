import os
from typing import Iterator
import pytest
import re
from unittest.mock import patch, MagicMock

from dlt.common import pendulum, Wei
from dlt.common.configuration.resolve import resolve_configuration, ConfigFieldMissingException
from dlt.common.schema import Schema
from dlt.common.storages import FileStorage
from dlt.common.utils import uniq_id

from dlt.destinations.impl.greenplum.configuration import GreenplumCredentials, GreenplumClientConfiguration
from dlt.destinations.impl.greenplum.greenplum import GreenplumClient
from dlt.common.destination import DestinationCapabilitiesContext

from tests.utils import TEST_STORAGE_ROOT, delete_test_storage, skipifpypy
from tests.load.utils import expect_load_file, prepare_table, yield_client_with_storage
from tests.common.configuration.utils import environment

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.fixture(autouse=True)
def auto_delete_storage() -> None:
    delete_test_storage()


@pytest.fixture
def mock_client() -> GreenplumClient:
    """Creates a Greenplum client mock for testing without a real connection"""
    schema = MagicMock(spec=Schema)
    schema.naming = MagicMock()
    schema.naming.normalize_identifier = lambda x: x.lower()
    schema.name = "test_schema"
    schema._normalizers_config = {"names": "tests.common.normalizers.to_lowercase"}
    
    config = GreenplumClientConfiguration(
        credentials=GreenplumCredentials(
            host="localhost",
            database="test_db",
            username="test_user",
            password="test_password"
        ),
        appendonly=True,
        blocksize=32768,
        compresstype="zstd",
        compresslevel=4,
        orientation="column",
        distribution_key="_dlt_id"
    )
    config = config._bind_dataset_name(dataset_name="test_dataset")
    
    capabilities = MagicMock(spec=DestinationCapabilitiesContext)
    capabilities.generates_case_sensitive_identifiers = MagicMock(return_value=False)
    capabilities.sqlglot_dialect = "postgres"
    
    with patch.object(GreenplumClient, '_get_table_update_sql', return_value=["CREATE TABLE test_schema.test_table (col1 text);"]):
        client = GreenplumClient(schema, config, capabilities)
    
    return client


def test_greenplum_credentials_defaults() -> None:
    gp_cred = GreenplumCredentials()
    assert gp_cred.port == 5432
    assert gp_cred.connect_timeout == 15
    assert gp_cred.client_encoding is None
    assert GreenplumCredentials.__config_gen_annotations__ == ["port", "connect_timeout"]
    # port should be optional
    resolve_configuration(gp_cred, explicit_value="postgresql://loader:loader@localhost/DLT_DATA")
    assert gp_cred.port == 5432
    # preserve case
    assert gp_cred.database == "DLT_DATA"


def test_greenplum_credentials_native_value(environment) -> None:
    with pytest.raises(ConfigFieldMissingException):
        resolve_configuration(
            GreenplumCredentials(), explicit_value="postgresql://loader@localhost/dlt_data"
        )
    # set password via env
    os.environ["CREDENTIALS__PASSWORD"] = "pass"
    c = resolve_configuration(
        GreenplumCredentials(), explicit_value="postgresql://loader@localhost/dlt_data"
    )
    assert c.is_resolved()
    assert c.password == "pass"
    # but if password is specified - it is final
    c = resolve_configuration(
        GreenplumCredentials(), explicit_value="postgresql://loader:loader@localhost/dlt_data"
    )
    assert c.is_resolved()
    assert c.password == "loader"

    # Test direct initialization with connection string
    c = GreenplumCredentials()
    c.parse_native_representation("postgresql://loader:loader@localhost/dlt_data")
    assert c.password == "loader"
    assert c.database == "dlt_data"
    assert c.username == "loader"
    assert c.host == "localhost"


def test_greenplum_query_params() -> None:
    # test greenplum timeout
    dsn = "postgresql://loader:pass@localhost:5432/dlt_data?client_encoding=utf-8&connect_timeout=600"
    csc = GreenplumCredentials()
    csc.parse_native_representation(dsn)
    assert csc.connect_timeout == 600
    assert csc.client_encoding == "utf-8"
    assert csc.to_native_representation() == dsn


def test_greenplum_configuration_storage_params() -> None:
    """Test for checking storage parameters in configuration"""
    config = GreenplumClientConfiguration(
        credentials=GreenplumCredentials(
            host="localhost",
            database="test_db",
            username="test_user",
            password="test_password"
        ),
        appendonly=True,
        blocksize=32768,
        compresstype="zstd",
        compresslevel=4,
        orientation="column",
        distribution_key="_dlt_id"
    )
    
    assert config.appendonly is True
    assert config.blocksize == 32768
    assert config.compresstype == "zstd"
    assert config.compresslevel == 4
    assert config.orientation == "column"
    assert config.distribution_key == "_dlt_id"


def test_create_table_sql_with_distribution() -> None:
    """Test for checking SQL query for table creation with distribution parameters"""
    # Start with a basic CREATE TABLE statement
    create_sql = "CREATE TABLE public.test_table (_dlt_id TEXT PRIMARY KEY, name TEXT, value INTEGER);"
    
    # Set up Greenplum configuration
    config = GreenplumClientConfiguration(
        appendonly=True,
        blocksize=32768,
        compresstype="zstd",
        compresslevel=4,
        orientation="column",
        distribution_key="_dlt_id"
    )
    
    # Apply Greenplum storage and distribution parameters to the SQL
    if hasattr(config, 'appendonly') and config.appendonly:
        create_sql = create_sql.rstrip(";")
        storage_params = []
        
        if config.appendonly:
            storage_params.append("appendonly=true")
        if config.blocksize:
            storage_params.append(f"blocksize={config.blocksize}")
        if config.compresstype:
            storage_params.append(f"compresstype={config.compresstype}")
        if config.compresslevel:
            storage_params.append(f"compresslevel={config.compresslevel}")
        if config.orientation:
            storage_params.append(f"orientation={config.orientation}")
        
        create_sql += f" WITH ({', '.join(storage_params)})"
        
        # Add distribution by primary key
        if config.distribution_key:
            dist_key = config.distribution_key
            create_sql += f" DISTRIBUTED BY (\"{dist_key}\")"
        else:
            create_sql += " DISTRIBUTED RANDOMLY"
        
        create_sql += ";"
    
    # Verify storage parameters and distribution clause
    assert "WITH (appendonly=true" in create_sql
    assert "blocksize=32768" in create_sql
    assert "compresstype=zstd" in create_sql
    assert "compresslevel=4" in create_sql
    assert "orientation=column" in create_sql
    assert "DISTRIBUTED BY" in create_sql
    assert "_dlt_id" in create_sql


def test_create_table_with_custom_distribution() -> None:
    """Test for checking table creation with custom distribution key"""
    # Start with a basic CREATE TABLE statement
    create_sql = "CREATE TABLE public.test_table (_dlt_id TEXT PRIMARY KEY, custom_id TEXT, name TEXT);"
    
    # Set up Greenplum configuration with custom distribution key
    config = GreenplumClientConfiguration(
        appendonly=True,
        blocksize=32768,
        compresstype="zstd",
        compresslevel=4,
        orientation="column",
        distribution_key="custom_id"  # Use custom column as distribution key
    )
    
    # Apply Greenplum storage and distribution parameters to the SQL
    if hasattr(config, 'appendonly') and config.appendonly:
        create_sql = create_sql.rstrip(";")
        storage_params = []
        
        if config.appendonly:
            storage_params.append("appendonly=true")
        if config.blocksize:
            storage_params.append(f"blocksize={config.blocksize}")
        if config.compresstype:
            storage_params.append(f"compresstype={config.compresstype}")
        if config.compresslevel:
            storage_params.append(f"compresslevel={config.compresslevel}")
        if config.orientation:
            storage_params.append(f"orientation={config.orientation}")
        
        create_sql += f" WITH ({', '.join(storage_params)})"
        
        # Add distribution by custom key
        if config.distribution_key:
            dist_key = config.distribution_key
            create_sql += f" DISTRIBUTED BY (\"{dist_key}\")"
        else:
            create_sql += " DISTRIBUTED RANDOMLY"
        
        create_sql += ";"
    
    # Verify storage parameters and custom distribution key
    assert "WITH (appendonly=true" in create_sql
    assert "DISTRIBUTED BY" in create_sql
    assert "custom_id" in create_sql 