---
sidebar_label: jsonpath
title: common.jsonpath
---

#### TJsonPath

Jsonpath compiled or str

#### TAnyJsonPath

A single or multiple jsonpaths

#### delete\_matches

```python
def delete_matches(paths: TAnyJsonPath, data: DictStrAny) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/jsonpath.py#L25)

Remove all keys from `data` matching any of given json path(s).
Filtering is done in place.

#### find\_values

```python
def find_values(path: TJsonPath, data: DictStrAny) -> List[Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/jsonpath.py#L33)

Return a list of values found under the given json path

#### resolve\_paths

```python
def resolve_paths(paths: TAnyJsonPath, data: DictStrAny) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/jsonpath.py#L39)

Return a list of paths resolved against `data`. The return value is a list of strings.

**Example**:

  >>> resolve_paths('$.a.items[*].b', {'a': {'items': [{'b': 2}, {'b': 3}]}})
  >>> # ['a.items.[0].b', 'a.items.[1].b']

