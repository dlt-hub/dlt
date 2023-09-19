---
sidebar_label: naming
title: destinations.weaviate.naming
---

## NamingConvention Objects

```python
class NamingConvention(SnakeCaseNamingConvention)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/weaviate/naming.py#L7)

Normalizes identifiers according to Weaviate documentation: https://weaviate.io/developers/weaviate/config-refs/schema#class

#### normalize\_identifier

```python
def normalize_identifier(identifier: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/weaviate/naming.py#L20)

Normalizes Weaviate property name by removing not allowed characters, replacing them by _ and contracting multiple _ into single one
and lowercasing the first character.

#### normalize\_table\_identifier

```python
def normalize_table_identifier(identifier: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/weaviate/naming.py#L34)

Creates Weaviate class name. Runs property normalization and then creates capitalized case name by splitting on _

https://weaviate.io/developers/weaviate/configuration/schema-configuration#create-a-class

