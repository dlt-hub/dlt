---
sidebar_label: config_section_context
title: common.configuration.specs.config_section_context
---

## ConfigSectionContext Objects

```python
@configspec
class ConfigSectionContext(ContainerInjectableContext)
```

#### merge

```python
def merge(existing: "ConfigSectionContext") -> None
```

Merges existing context into incoming using a merge style function

#### source\_name

```python
def source_name() -> str
```

Gets name of a source from `sections`

#### source\_section

```python
def source_section() -> str
```

Gets section of a source from `sections`

#### prefer\_existing

```python
@staticmethod
def prefer_existing(incoming: "ConfigSectionContext",
                    existing: "ConfigSectionContext") -> None
```

Prefer existing section context when merging this context before injecting

#### resource\_merge\_style

```python
@staticmethod
def resource_merge_style(incoming: "ConfigSectionContext",
                         existing: "ConfigSectionContext") -> None
```

If top level section is same and there are 3 sections it replaces second element (source module) from existing and keeps the 3rd element (name)

