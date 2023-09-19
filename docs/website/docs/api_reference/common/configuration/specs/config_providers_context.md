---
sidebar_label: config_providers_context
title: common.configuration.specs.config_providers_context
---

## ConfigProvidersContext Objects

```python
@configspec
class ConfigProvidersContext(ContainerInjectableContext)
```

Injectable list of providers used by the configuration `resolve` module

#### add\_extras

```python
def add_extras() -> None
```

Adds extra providers. Extra providers may use initial providers when setting up

