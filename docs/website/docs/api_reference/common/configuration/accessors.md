---
sidebar_label: accessors
title: common.configuration.accessors
---

## \_ConfigAccessor Objects

```python
class _ConfigAccessor(_Accessor)
```

Provides direct access to configured values that are not secrets.

#### config\_providers

```python
@property
def config_providers() -> Sequence[ConfigProvider]
```

Return a list of config providers, in lookup order

#### writable\_provider

```python
@property
def writable_provider() -> ConfigProvider
```

find first writable provider that does not support secrets - should be config.toml

#### value

A placeholder that tells dlt to replace it with actual config value during the call to a source or resource decorated function.

## \_SecretsAccessor Objects

```python
class _SecretsAccessor(_Accessor)
```

Provides direct access to secrets.

#### config\_providers

```python
@property
def config_providers() -> Sequence[ConfigProvider]
```

Return a list of config providers that can hold secrets, in lookup order

#### writable\_provider

```python
@property
def writable_provider() -> ConfigProvider
```

find first writable provider that supports secrets - should be secrets.toml

#### value

A placeholder that tells dlt to replace it with actual secret during the call to a source or resource decorated function.

#### config

Dictionary-like access to all config values to dlt

#### secrets

Dictionary-like access to all secrets known known to dlt

