---
sidebar_label: config_providers_context
title: common.configuration.specs.config_providers_context
---

## ConfigProvidersContext Objects

```python
@configspec
class ConfigProvidersContext(ContainerInjectableContext)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/configuration/specs/config_providers_context.py#L36)

Injectable list of providers used by the configuration `resolve` module

### add\_extras

```python
def add_extras() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/configuration/specs/config_providers_context.py#L55)

Adds extra providers. Extra providers may use initial providers when setting up

