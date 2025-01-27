---
sidebar_label: config_providers_context
title: common.configuration.specs.config_providers_context
---

## ConfigProvidersContext Objects

```python
@configspec
class ConfigProvidersContext(ContainerInjectableContext)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/specs/config_providers_context.py#L36)

Injectable list of providers used by the configuration `resolve` module

### add\_extras

```python
def add_extras() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/specs/config_providers_context.py#L55)

Adds extra providers. Extra providers may use initial providers when setting up

