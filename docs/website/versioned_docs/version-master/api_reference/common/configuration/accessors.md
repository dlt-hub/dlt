---
sidebar_label: accessors
title: common.configuration.accessors
---

## \_Accessor Objects

```python
class _Accessor(abc.ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/accessors.py#L15)

### register\_provider

```python
@staticmethod
def register_provider(provider: ConfigProvider) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/accessors.py#L91)

Registers `provider` to participate in the configuration resolution. `provider`
is added after all existing providers and will be used if all others do not resolve.

## \_ConfigAccessor Objects

```python
class _ConfigAccessor(_Accessor)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/accessors.py#L98)

Provides direct access to configured values that are not secrets.

### config\_providers

```python
@property
def config_providers() -> Sequence[ConfigProvider]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/accessors.py#L102)

Return a list of config providers, in lookup order

### writable\_provider

```python
@property
def writable_provider() -> ConfigProvider
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/accessors.py#L111)

find first writable provider that does not support secrets - should be config.toml

### value

A placeholder that tells dlt to replace it with actual config value during the call to a source or resource decorated function.

## \_SecretsAccessor Objects

```python
class _SecretsAccessor(_Accessor)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/accessors.py#L123)

Provides direct access to secrets.

### config\_providers

```python
@property
def config_providers() -> Sequence[ConfigProvider]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/accessors.py#L127)

Return a list of config providers that can hold secrets, in lookup order

### writable\_provider

```python
@property
def writable_provider() -> ConfigProvider
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/accessors.py#L136)

find first writable provider that supports secrets - should be secrets.toml

### value

A placeholder that tells dlt to replace it with actual secret during the call to a source or resource decorated function.

## config

Dictionary-like access to all config values to dlt

## secrets

Dictionary-like access to all secrets known known to dlt

