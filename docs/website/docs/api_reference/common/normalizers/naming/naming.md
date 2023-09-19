---
sidebar_label: naming
title: common.normalizers.naming.naming
---

## NamingConvention Objects

```python
class NamingConvention(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/normalizers/naming/naming.py#L9)

#### normalize\_identifier

```python
@abstractmethod
def normalize_identifier(identifier: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/normalizers/naming/naming.py#L18)

Normalizes and shortens the identifier according to naming convention in this function code

#### normalize\_table\_identifier

```python
def normalize_table_identifier(identifier: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/normalizers/naming/naming.py#L27)

Normalizes and shortens identifier that will function as a dataset, table or schema name, defaults to `normalize_identifier`

#### make\_path

```python
@abstractmethod
def make_path(*identifiers: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/normalizers/naming/naming.py#L32)

Builds path out of identifiers. Identifiers are neither normalized nor shortened

#### break\_path

```python
@abstractmethod
def break_path(path: str) -> Sequence[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/normalizers/naming/naming.py#L37)

Breaks path into sequence of identifiers

#### normalize\_path

```python
def normalize_path(path: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/normalizers/naming/naming.py#L41)

Breaks path into identifiers, normalizes components, reconstitutes and shortens the path

#### normalize\_tables\_path

```python
def normalize_tables_path(path: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/normalizers/naming/naming.py#L47)

Breaks path of table identifiers, normalizes components, reconstitutes and shortens the path

#### shorten\_fragments

```python
def shorten_fragments(*normalized_idents: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/normalizers/naming/naming.py#L53)

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

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/normalizers/naming/naming.py#L62)

Shortens the `name` to `max_length` and adds a tag to it to make it unique. Tag may be placed in the middle or at the end

## SupportsNamingConvention Objects

```python
class SupportsNamingConvention(Protocol)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/normalizers/naming/naming.py#L90)

Expected of modules defining naming convention

#### NamingConvention

A class with a name NamingConvention deriving from normalizers.naming.NamingConvention

