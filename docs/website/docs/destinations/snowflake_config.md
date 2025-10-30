# Snowflake Configuration

## Authentication Methods

Snowflake destination supports several authentication methods:

### Username and Password
```python
pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination="snowflake",
    credentials={
        "username": "my_user",
        "password": "my_password",
        "account": "my_account"
    }
)
```

### Private Key
```python
pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination="snowflake",
    credentials={
        "username": "my_user",
        "private_key_path": "path/to/rsa_key.p8",
        "account": "my_account"
    }
)
```

### Snowpark Container Services

When running inside Snowpark Container Services, dlt will automatically detect and use the mounted OAuth token and environment variables:

```python
pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination="snowflake",
    dataset_name="my_dataset"
)

# No credentials needed - dlt will automatically use:
# - Token from /snowflake/session/token
# - Account from SNOWFLAKE_HOST or SNOWFLAKE_ACCOUNT env vars
```

You can still provide explicit credentials to override the auto-detection:

```python
pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination="snowflake",
    credentials={
        "token": "my_token",  # Override container token
        "account": "my_account"  # Override SNOWFLAKE_HOST
    }
)
```

## Additional Configuration
# ...existing documentation...