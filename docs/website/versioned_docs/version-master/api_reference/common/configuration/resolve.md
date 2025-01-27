---
sidebar_label: resolve
title: common.configuration.resolve
---

## initialize\_credentials

```python
def initialize_credentials(hint: Any,
                           initial_value: Any) -> CredentialsConfiguration
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/resolve.py#L70)

Instantiate credentials of type `hint` with `initial_value`. The initial value must be a native representation (typically string)
or a dictionary corresponding to credential's fields. In case of union of credentials, the first configuration in the union fully resolved by
initial value will be instantiated.

## inject\_section

```python
def inject_section(
        section_context: ConfigSectionContext,
        merge_existing: bool = True,
        lock_context: bool = False) -> ContextManager[ConfigSectionContext]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/resolve.py#L96)

Context manager that sets section specified in `section_context` to be used during configuration resolution. Optionally merges the context already in the container with the one provided

**Arguments**:

- `section_context` _ConfigSectionContext_ - Instance providing a pipeline name and section context
- `merge_existing` _bool, optional_ - Merges existing section context with `section_context` in the arguments by executing `merge_style` function on `section_context`. Defaults to True.
- `lock_context` _bool, optional_ - Instruct to threadlock the current thread to prevent race conditions in context injection.
  
  Default Merge Style:
  Gets `pipeline_name` and `sections` from existing context if they are not provided in `section_context` argument.
  

**Yields**:

- `Iterator[ConfigSectionContext]` - Context manager with current section context

