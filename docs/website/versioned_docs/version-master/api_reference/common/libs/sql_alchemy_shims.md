---
sidebar_label: sql_alchemy_shims
title: common.libs.sql_alchemy_shims
---

Ports fragments of URL class from Sql Alchemy to use them when dependency is not available.

## ImmutableDict Objects

```python
class ImmutableDict(Dict[_KT, _VT])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/sql_alchemy_shims.py#L36)

Not a real immutable dict

## URL Objects

```python
class URL(NamedTuple)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/sql_alchemy_shims.py#L63)

Represent the components of a URL used to connect to a database.

Based on SqlAlchemy URL class with copyright as below:

__engine/url.py__

__Copyright (C) 2005-2023 the SQLAlchemy authors and contributors__

#
__This module is part of SQLAlchemy and is released under__

__the MIT License: https://www.opensource.org/licenses/mit-license.php__


### drivername

database backend and driver name, such as `postgresql+psycopg2`

### username

username string

### password

password, which is normally a string but may also be any object that has a `__str__()` method.

### host

hostname or IP number.  May also be a data source name for some drivers.

### port

integer port number

### database

database name

### query

an immutable mapping representing the query string.  contains strings
for keys and either strings or tuples of strings for values

### create

```python
@classmethod
def create(cls,
           drivername: str,
           username: Optional[str] = None,
           password: Optional[str] = None,
           host: Optional[str] = None,
           port: Optional[int] = None,
           database: Optional[str] = None,
           query: Mapping[str, Union[Sequence[str], str]] = None) -> "URL"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/sql_alchemy_shims.py#L93)

Create a new `URL` object.

### set

```python
def set(drivername: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        query: Optional[Mapping[str, Union[Sequence[str],
                                           str]]] = None) -> "URL"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/sql_alchemy_shims.py#L189)

return a new `URL` object with modifications.

### update\_query\_pairs

```python
def update_query_pairs(key_value_pairs: Iterable[Tuple[str, Union[str,
                                                                  List[str]]]],
                       append: bool = False) -> "URL"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/sql_alchemy_shims.py#L237)

Return a new `URL` object with the `query` parameter dictionary updated by the given sequence of key/value pairs

### render\_as\_string

```python
def render_as_string(hide_password: bool = True) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/sql_alchemy_shims.py#L282)

Render this `URL` object as a string.

### get\_backend\_name

```python
def get_backend_name() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/sql_alchemy_shims.py#L344)

Return the backend name.

This is the name that corresponds to the database backend in
use, and is the portion of the `drivername`
that is to the left of the plus sign.

### get\_driver\_name

```python
def get_driver_name() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/sql_alchemy_shims.py#L357)

Return the backend name.

This is the name that corresponds to the DBAPI driver in
use, and is the portion of the `drivername`
that is to the right of the plus sign.

## make\_url

```python
def make_url(name_or_url: Union[str, URL]) -> URL
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/libs/sql_alchemy_shims.py#L371)

Given a string, produce a new URL instance.

The format of the URL generally follows `RFC-1738`, with some exceptions, including
that underscores, and not dashes or periods, are accepted within the
"scheme" portion.

If a `URL` object is passed, it is returned as is.

