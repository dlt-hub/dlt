---
sidebar_label: plugins
title: common.configuration.plugins
---

## manager

```python
def manager() -> pluggy.PluginManager
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/plugins.py#L37)

Returns current plugin context

## load\_setuptools\_entrypoints

```python
def load_setuptools_entrypoints(m: pluggy.PluginManager) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/plugins.py#L44)

Scans setuptools distributions that are path or have name starting with `dlt-`
loads entry points in group `dlt` and instantiates them to initialize contained plugins

