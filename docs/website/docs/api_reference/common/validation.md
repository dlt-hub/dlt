---
sidebar_label: validation
title: common.validation
---

#### validate\_dict

```python
def validate_dict(spec: Type[_TypedDict],
                  doc: StrAny,
                  path: str,
                  filter_f: TFilterFunc = None,
                  validator_f: TCustomValidator = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/validation.py#L12)

Validate the `doc` dictionary based on the given typed dictionary specification `spec`.

**Arguments**:

- `spec` _Type[_TypedDict]_ - The typed dictionary that `doc` should conform to.
- `doc` _StrAny_ - The dictionary to validate.
- `path` _str_ - The string representing the location of the dictionary
  in a hierarchical data structure.
- `filter_f` _TFilterFunc, optional_ - A function to filter keys in `doc`. It should
  return `True` for keys to be kept. Defaults to a function that keeps all keys.
- `validator_f` _TCustomValidator, optional_ - A function to perform additional validation
  for types not covered by this function. It should return `True` if the validation passes.
  Defaults to a function that rejects all such types.
  

**Raises**:

- `DictValidationException` - If there are missing required fields, unexpected fields,
  type mismatches or unvalidated types in `doc` compared to `spec`.
  

**Returns**:

  None

