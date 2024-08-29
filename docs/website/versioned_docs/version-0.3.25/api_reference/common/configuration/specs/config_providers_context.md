---
sidebar_label: config_providers_context
title: common.configuration.specs.config_providers_context
---

## ConfigProvidersContext Objects

```python
@configspec
class ConfigProvidersContext(ContainerInjectableContext)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/config_providers_context.py#L35)

Injectable list of providers used by the configuration `resolve` module

### add\_extras

```python
def add_extras() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/specs/config_providers_context.py#L50)

Adds extra providers. Extra providers may use initial providers when setting up

