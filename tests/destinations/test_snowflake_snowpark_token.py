import os
import tempfile
from pathlib import Path
from dlt.destinations.impl.snowflake.configuration import SnowflakeCredentials

def test_snowpark_token(monkeypatch):
    """Test Snowpark Container Services token authentication"""
    with tempfile.NamedTemporaryFile("w+", delete=False) as tf:
        tf.write("test-token")
        tf.flush()
        token_path = tf.name

        # Set up test environment
        monkeypatch.setenv("SNOWFLAKE_HOST", "test-account")
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test-account")
        
        # Mock token path existence and file reading
        monkeypatch.setattr("os.path.exists", lambda p: p == token_path)
        monkeypatch.setattr("builtins.open", lambda p, mode="r": open(token_path, mode))

        # Test credentials resolution
        creds = SnowflakeCredentials()
        creds.on_resolved.__globals__["token_path"] = token_path
        creds.on_resolved()

        assert creds.token == "test-token"
        assert creds.host == "test-account" 
        assert creds.authenticator == "oauth"

        # Test connector parameters
        params = creds.to_connector_params()
        assert params["token"] == "test-token"
        assert params["account"] == "test-account"
        assert params["authenticator"] == "oauth"
        assert "user" not in params
        assert "password" not in params

        # Cleanup
        Path(token_path).unlink()

def test_explicit_credentials_preferred(monkeypatch):
    """Test that explicit credentials are preferred over Snowpark token"""
    with tempfile.NamedTemporaryFile("w+", delete=False) as tf:
        tf.write("test-token")
        tf.flush()
        token_path = tf.name

        # Test with explicit credentials
        creds = SnowflakeCredentials(
            token="explicit-token",
            host="explicit-account"
        )
        creds.on_resolved.__globals__["token_path"] = token_path
        creds.on_resolved()

        assert creds.token == "explicit-token"
        assert creds.host == "explicit-account"

        # Cleanup
        Path(token_path).unlink()