---
sidebar_label: naming
title: common.normalizers.naming.naming
---

## NamingConvention Objects

```python
class NamingConvention(ABC)
```

#### normalize\_identifier

```python
@abstractmethod
def normalize_identifier(identifier: str) -> str
```

Normalizes and shortens the identifier according to naming convention in this function code

#### normalize\_table\_identifier

```python
def normalize_table_identifier(identifier: str) -> str
```

Normalizes and shortens identifier that will function as a dataset, table or schema name, defaults to `normalize_identifier`

#### make\_path

```python
@abstractmethod
def make_path(*identifiers: str) -> str
```

Builds path out of identifiers. Identifiers are neither normalized nor shortened

#### break\_path

```python
@abstractmethod
def break_path(path: str) -> Sequence[str]
```

Breaks path into sequence of identifiers

#### normalize\_path

```python
def normalize_path(path: str) -> str
```

Breaks path into identifiers, normalizes components, reconstitutes and shortens the path

#### normalize\_tables\_path

```python
def normalize_tables_path(path: str) -> str
```

Breaks path of table identifiers, normalizes components, reconstitutes and shortens the path

#### shorten\_fragments

```python
def shorten_fragments(*normalized_idents: str) -> str
```

Reconstitutes and shortens the path of normalized identifiers

#### shorten\_identifier

```python
@staticmethod
@lru_cache(maxsize=None)
def shorten_identifier(normalized_ident: str,
                       identifier: str,
                       max_length: int,
                       collision_prob: float = _DEFAULT_COLLISION_PROB) -> str
```

Shortens the `name` to `max_length` and adds a tag to it to make it unique. Tag may be placed in the middle or at the end

## SupportsNamingConvention Objects

```python
class SupportsNamingConvention(Protocol)
```

Expected of modules defining naming convention

#### NamingConvention

A class with a name NamingConvention deriving from normalizers.naming.NamingConvention

