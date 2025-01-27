---
sidebar_label: jsonpath
title: common.jsonpath
---

## TJsonPath

Jsonpath compiled or str

## TAnyJsonPath

A single or multiple jsonpaths

## delete\_matches

```python
def delete_matches(paths: TAnyJsonPath, data: DictStrAny) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/jsonpath.py#L25)

Remove all keys from `data` matching any of given json path(s).
Filtering is done in place.

## find\_values

```python
def find_values(path: TJsonPath, data: DictStrAny) -> List[Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/jsonpath.py#L33)

Return a list of values found under the given json path

## resolve\_paths

```python
def resolve_paths(paths: TAnyJsonPath, data: DictStrAny) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/jsonpath.py#L39)

Return a list of paths resolved against `data`. The return value is a list of strings.

**Example**:

```py
resolve_paths('$.a.items[*].b', {'a': {'items': [{'b': 2}, {'b': 3}]}})
# ['a.items.[0].b', 'a.items.[1].b']
```

## is\_simple\_field\_path

```python
def is_simple_field_path(path: JSONPath) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/jsonpath.py#L51)

Checks if the given path represents a simple single field name.

**Example**:

```py
is_simple_field_path(compile_path('id'))
```
  True
```py
is_simple_field_path(compile_path('$.id'))
```
  False

## extract\_simple\_field\_name

```python
def extract_simple_field_name(path: Union[str, JSONPath]) -> Optional[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/jsonpath.py#L63)

Extracts a simple field name from a JSONPath if it represents a single field access.
Returns None if the path is complex (contains wildcards, array indices, or multiple fields).

**Arguments**:

- `path` - A JSONPath object or string
  

**Returns**:

- `Optional[str]` - The field name if path represents a simple field access, None otherwise
  

**Example**:

```py
    extract_simple_field_name('name')
```
  'name'
```py
    extract_simple_field_name('"name"')
```
  'name'
```py
    extract_simple_field_name('"na$me"') # Escaped characters are preserved
```
  'na$me'
```py
    extract_simple_field_name('"na.me"') # Escaped characters are preserved
```
  'na.me'
```py
    extract_simple_field_name('$.name')  # Returns None
    extract_simple_field_name('$.items[*].name')  # Returns None
    extract_simple_field_name('*')  # Returns None
```

