---
sidebar_label: base_configuration
title: common.configuration.specs.base_configuration
---

## configspec

```python
def configspec(
    cls: Optional[Type[Any]] = None
) -> Union[Type[TAnyClass], Callable[[Type[TAnyClass]], Type[TAnyClass]]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/base_configuration.py#L116)

Converts (via derivation) any decorated class to a Python dataclass that may be used as a spec to resolve configurations

In comparison the Python dataclass, a spec implements full dictionary interface for its attributes, allows instance creation from ie. strings
or other types (parsing, deserialization) and control over configuration resolution process. See `BaseConfiguration` and CredentialsConfiguration` for
more information.

## BaseConfiguration Objects

```python
@configspec
class BaseConfiguration(MutableMapping[str, Any])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/base_configuration.py#L200)

### \_\_is\_resolved\_\_

True when all config fields were resolved and have a specified value type

### \_\_section\_\_

Obligatory section used by config providers when searching for keys, always present in the search path

### \_\_exception\_\_

Holds the exception that prevented the full resolution

### \_\_config\_gen\_annotations\_\_

Additional annotations for config generator, currently holds a list of fields of interest that have defaults

### \_\_dataclass\_fields\_\_

Typing for dataclass fields

### parse\_native\_representation

```python
def parse_native_representation(native_value: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/base_configuration.py#L213)

Initialize the configuration fields by parsing the `native_value` which should be a native representation of the configuration
or credentials, for example database connection string or JSON serialized GCP service credentials file.

**Arguments**:

- `native_value` _Any_ - A native representation of the configuration
  

**Raises**:

- `NotImplementedError` - This configuration does not have a native representation
- `ValueError` - The value provided cannot be parsed as native representation

### to\_native\_representation

```python
def to_native_representation() -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/base_configuration.py#L226)

Represents the configuration instance in its native form ie. database connection string or JSON serialized GCP service credentials file.

**Raises**:

- `NotImplementedError` - This configuration does not have a native representation
  

**Returns**:

- `Any` - A native representation of the configuration

### get\_resolvable\_fields

```python
@classmethod
def get_resolvable_fields(cls) -> Dict[str, type]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/base_configuration.py#L247)

Returns a mapping of fields to their type hints. Dunders should not be resolved and are not returned

### is\_partial

```python
def is_partial() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/base_configuration.py#L254)

Returns True when any required resolvable field has its value missing.

### copy

```python
def copy() -> _T
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/base_configuration.py#L269)

Returns a deep copy of the configuration instance

### \_\_iter\_\_

```python
def __iter__() -> Iterator[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/base_configuration.py#L296)

Iterator or valid key names

## CredentialsConfiguration Objects

```python
@configspec
class CredentialsConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/base_configuration.py#L342)

Base class for all credentials. Credentials are configurations that may be stored only by providers supporting secrets.

### \_\_init\_\_

```python
def __init__(init_value: Any = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/base_configuration.py#L347)

Initializes credentials from `init_value`

Init value may be a native representation of the credentials or a dict. In case of native representation (for example a connection string or JSON with service account credentials)
a `parse_native_representation` method will be used to parse it. In case of a dict, the credentials object will be updated with key: values of the dict.
Unexpected values in the dict will be ignored.

Credentials will be marked as resolved if all required fields are set.

### to\_native\_credentials

```python
def to_native_credentials() -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/base_configuration.py#L365)

Returns native credentials object.

By default calls `to_native_representation` method.

### \_\_str\_\_

```python
def __str__() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/base_configuration.py#L372)

Get string representation of credentials to be displayed, with all secret parts removed

## CredentialsWithDefault Objects

```python
class CredentialsWithDefault()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/base_configuration.py#L377)

A mixin for credentials that can be instantiated from default ie. from well known env variable with credentials

## ContainerInjectableContext Objects

```python
@configspec
class ContainerInjectableContext(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/base_configuration.py#L393)

Base class for all configurations that may be injected from a Container. Injectable configuration is called a context

### can\_create\_default

If True, `Container` is allowed to create default context instance, if none exists

### global\_affinity

If True, `Container` will create context that will be visible in any thread. If False, per thread context is created

### add\_extras

```python
def add_extras() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/base_configuration.py#L401)

Called right after context was added to the container. Benefits mostly the config provider injection context which adds extra providers using the initial ones.

