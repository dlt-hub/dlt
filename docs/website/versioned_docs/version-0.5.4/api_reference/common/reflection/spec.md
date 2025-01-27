---
sidebar_label: spec
title: common.reflection.spec
---

## spec\_from\_signature

```python
def spec_from_signature(
    f: AnyFun,
    sig: Signature,
    include_defaults: bool = True,
    base: Type[BaseConfiguration] = BaseConfiguration
) -> Tuple[Type[BaseConfiguration], Dict[str, Any]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/reflection/spec.py#L26)

Creates a SPEC on base `base1 for a function `f` with signature `sig`.

All the arguments in `sig` that are valid SPEC hints and have defaults will be part of the SPEC.
Special default markers for required SPEC fields `dlt.secrets.value` and `dlt.config.value` are sentinel
string values with a type set to Any during typechecking. The sentinels are defined in dlt.common.typing module.

The name of a SPEC type is inferred from qualname of `f` and type will refer to `f` module and is unique
for a module. NOTE: the SPECS are cached in the module by using name as an id.

Return value is a tuple of SPEC and SPEC fields created from a `sig`.

