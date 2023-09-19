---
sidebar_label: accessors
title: common.configuration.accessors
---

## \_ConfigAccessor Objects

```python
class _ConfigAccessor(_Accessor)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/accessors.py#L88)

Provides direct access to configured values that are not secrets.

#### config\_providers

```python
@property
def config_providers() -> Sequence[ConfigProvider]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/accessors.py#L92)

Return a list of config providers, in lookup order

#### writable\_provider

```python
@property
def writable_provider() -> ConfigProvider
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/accessors.py#L101)

find first writable provider that does not support secrets - should be config.toml

#### value

A placeholder that tells dlt to replace it with actual config value during the call to a source or resource decorated function.

## \_SecretsAccessor Objects

```python
class _SecretsAccessor(_Accessor)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/accessors.py#L109)

Provides direct access to secrets.

#### config\_providers

```python
@property
def config_providers() -> Sequence[ConfigProvider]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/accessors.py#L113)

Return a list of config providers that can hold secrets, in lookup order

#### writable\_provider

```python
@property
def writable_provider() -> ConfigProvider
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/accessors.py#L122)

find first writable provider that supports secrets - should be secrets.toml

#### value

A placeholder that tells dlt to replace it with actual secret during the call to a source or resource decorated function.

#### config

Dictionary-like access to all config values to dlt

#### secrets

Dictionary-like access to all secrets known known to dlt

