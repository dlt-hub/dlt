---
sidebar_label: config_section_context
title: common.configuration.specs.config_section_context
---

## ConfigSectionContext Objects

```python
@configspec
class ConfigSectionContext(ContainerInjectableContext)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/config_section_context.py#L7)

#### merge

```python
def merge(existing: "ConfigSectionContext") -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/config_section_context.py#L17)

Merges existing context into incoming using a merge style function

#### source\_name

```python
def source_name() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/config_section_context.py#L22)

Gets name of a source from `sections`

#### source\_section

```python
def source_section() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/config_section_context.py#L28)

Gets section of a source from `sections`

#### prefer\_existing

```python
@staticmethod
def prefer_existing(incoming: "ConfigSectionContext",
                    existing: "ConfigSectionContext") -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/config_section_context.py#L41)

Prefer existing section context when merging this context before injecting

#### resource\_merge\_style

```python
@staticmethod
def resource_merge_style(incoming: "ConfigSectionContext",
                         existing: "ConfigSectionContext") -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/specs/config_section_context.py#L48)

If top level section is same and there are 3 sections it replaces second element (source module) from existing and keeps the 3rd element (name)

