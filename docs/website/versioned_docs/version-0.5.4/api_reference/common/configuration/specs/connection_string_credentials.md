---
sidebar_label: connection_string_credentials
title: common.configuration.specs.connection_string_credentials
---

## ConnectionStringCredentials Objects

```python
@configspec
class ConnectionStringCredentials(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/specs/connection_string_credentials.py#L11)

### \_\_init\_\_

```python
def __init__(connection_string: Union[str, Dict[str, Any]] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/specs/connection_string_credentials.py#L22)

Initializes the credentials from SQLAlchemy like connection string or from dict holding connection string elements

### get\_query

```python
def get_query() -> Dict[str, Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/specs/connection_string_credentials.py#L47)

Gets query preserving parameter types. Mostly used internally to export connection params

### to\_url

```python
def to_url() -> URL
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/specs/connection_string_credentials.py#L51)

Creates SQLAlchemy compatible URL object, computes current query via `get_query` and serializes its values to str

