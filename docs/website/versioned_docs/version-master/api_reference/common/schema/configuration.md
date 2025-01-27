---
sidebar_label: configuration
title: common.schema.configuration
---

## SchemaConfiguration Objects

```python
@configspec
class SchemaConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/configuration.py#L10)

### naming

Union[str, NamingConvention]

### use\_break\_path\_on\_normalize

Post 1.4.0 to allow table and column names that contain table separators

