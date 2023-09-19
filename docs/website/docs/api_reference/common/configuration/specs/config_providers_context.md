---
sidebar_label: config_providers_context
title: common.configuration.specs.config_providers_context
---

## ConfigProvidersContext Objects

```python
@configspec
class ConfigProvidersContext(ContainerInjectableContext)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/config_providers_context.py#L22)

Injectable list of providers used by the configuration `resolve` module

#### add\_extras

```python
def add_extras() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/config_providers_context.py#L34)

Adds extra providers. Extra providers may use initial providers when setting up

