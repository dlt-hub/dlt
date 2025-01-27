---
sidebar_label: naming
title: destinations.impl.weaviate.naming
---

## NamingConvention Objects

```python
class NamingConvention(SnakeCaseNamingConvention)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/weaviate/naming.py#L9)

Normalizes identifiers according to Weaviate documentation: https://weaviate.io/developers/weaviate/config-refs/schema#class

### normalize\_identifier

```python
def normalize_identifier(identifier: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/weaviate/naming.py#L22)

Normalizes Weaviate property name by removing not allowed characters, replacing them by _ and contracting multiple _ into single one
and lowercasing the first character.

### normalize\_table\_identifier

```python
def normalize_table_identifier(identifier: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/weaviate/naming.py#L36)

Creates Weaviate class name. Runs property normalization and then creates capitalized case name by splitting on _

https://weaviate.io/developers/weaviate/configuration/schema-configuration#create-a-class

