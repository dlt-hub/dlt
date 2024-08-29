---
sidebar_label: inject
title: common.configuration.inject
---

## with\_config

```python
def with_config(
        func: Optional[AnyFun] = None,
        spec: Type[BaseConfiguration] = None,
        sections: Tuple[str, ...] = (),
        sections_merge_style: ConfigSectionContext.
    TMergeFunc = ConfigSectionContext.prefer_incoming,
        auto_pipeline_section: bool = False,
        include_defaults: bool = True,
        accept_partial: bool = False,
        initial_config: Optional[BaseConfiguration] = None,
        base: Type[BaseConfiguration] = BaseConfiguration,
        lock_context_on_injection: bool = True) -> Callable[[TFun], TFun]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/inject.py#L60)

Injects values into decorated function arguments following the specification in `spec` or by deriving one from function's signature.

The synthesized spec contains the arguments marked with `dlt.secrets.value` and `dlt.config.value` which are required to be injected at runtime.
Optionally (and by default) arguments with default values are included in spec as well.

**Arguments**:

- `func` _Optional[AnyFun], optional_ - A function with arguments to be injected. Defaults to None.
- `spec` _Type[BaseConfiguration], optional_ - A specification of injectable arguments. Defaults to None.
- `sections` _Tuple[str, ...], optional_ - A set of config sections in which to look for arguments values. Defaults to ().
- `prefer_existing_sections` - (bool, optional): When joining existing section context, the existing context will be preferred to the one in `sections`. Default: False
- `auto_pipeline_section` _bool, optional_ - If True, a top level pipeline section will be added if `pipeline_name` argument is present . Defaults to False.
- `include_defaults` _bool, optional_ - If True then arguments with default values will be included in synthesized spec. If False only the required arguments marked with `dlt.secrets.value` and `dlt.config.value` are included
- `base` _Type[BaseConfiguration], optional_ - A base class for synthesized spec. Defaults to BaseConfiguration.
- `lock_context_on_injection` _bool, optional_ - If True, the thread context will be locked during injection to prevent race conditions. Defaults to True.

**Returns**:

  Callable[[TFun], TFun]: A decorated function

## last\_config

```python
def last_config(**kwargs: Any) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/inject.py#L252)

Get configuration instance used to inject function arguments

## create\_resolved\_partial

```python
def create_resolved_partial(f: AnyFun,
                            config: Optional[BaseConfiguration] = None
                            ) -> AnyFun
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/configuration/inject.py#L261)

Create a pre-resolved partial of the with_config decorated function

