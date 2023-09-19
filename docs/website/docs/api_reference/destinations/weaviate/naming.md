---
sidebar_label: naming
title: destinations.weaviate.naming
---

## NamingConvention Objects

```python
class NamingConvention(SnakeCaseNamingConvention)
```

Normalizes identifiers according to Weaviate documentation: https://weaviate.io/developers/weaviate/config-refs/schema#class

#### normalize\_identifier

```python
def normalize_identifier(identifier: str) -> str
```

Normalizes Weaviate property name by removing not allowed characters, replacing them by _ and contracting multiple _ into single one
and lowercasing the first character.

#### normalize\_table\_identifier

```python
def normalize_table_identifier(identifier: str) -> str
```

Creates Weaviate class name. Runs property normalization and then creates capitalized case name by splitting on _

https://weaviate.io/developers/weaviate/configuration/schema-configuration#create-a-class

