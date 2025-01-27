---
sidebar_label: naming
title: common.normalizers.naming.naming
---

## NamingConvention Objects

```python
class NamingConvention(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/naming/naming.py#L9)

Initializes naming convention to generate identifier with `max_length` if specified. Base naming convention
is case sensitive by default

### PATH\_SEPARATOR

Subsequent nested fields will be separated with the string below, applies both to field and table names

### is\_case\_sensitive

```python
@property
@abstractmethod
def is_case_sensitive() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/naming/naming.py#L24)

Tells if given naming convention is producing case insensitive or case sensitive identifiers.

### normalize\_identifier

```python
@abstractmethod
def normalize_identifier(identifier: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/naming/naming.py#L29)

Normalizes and shortens the identifier according to naming convention in this function code

### normalize\_table\_identifier

```python
def normalize_table_identifier(identifier: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/naming/naming.py#L38)

Normalizes and shortens identifier that will function as a dataset, table or schema name, defaults to `normalize_identifier`

### make\_path

```python
def make_path(*identifiers: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/naming/naming.py#L42)

Builds path out of identifiers. Identifiers are neither normalized nor shortened

### break\_path

```python
def break_path(path: str) -> Sequence[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/naming/naming.py#L46)

Breaks path into sequence of identifiers

### normalize\_path

```python
def normalize_path(path: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/naming/naming.py#L50)

Breaks path into identifiers, normalizes components, reconstitutes and shortens the path

### normalize\_tables\_path

```python
def normalize_tables_path(path: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/naming/naming.py#L56)

Breaks path of table identifiers, normalizes components, reconstitutes and shortens the path

### shorten\_fragments

```python
def shorten_fragments(*normalized_idents: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/naming/naming.py#L64)

Reconstitutes and shortens the path of normalized identifiers

### name

```python
@classmethod
def name(cls) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/naming/naming.py#L72)

Naming convention name is the name of the module in which NamingConvention is defined

### shorten\_identifier

```python
@staticmethod
@lru_cache(maxsize=None)
def shorten_identifier(normalized_ident: str,
                       identifier: str,
                       max_length: int,
                       collision_prob: float = _DEFAULT_COLLISION_PROB) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/naming/naming.py#L88)

Shortens the `name` to `max_length` and adds a tag to it to make it unique. Tag may be placed in the middle or at the end

