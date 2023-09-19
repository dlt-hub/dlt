---
sidebar_label: requirements
title: cli.requirements
---

## SourceRequirements Objects

```python
class SourceRequirements()
```

Helper class to parse and manipulate entries in source's requirements.txt

#### dlt\_requirement

Final dlt requirement that may be updated with destination extras

#### dlt\_requirement\_base

Original dlt requirement without extras

#### from\_string

```python
@classmethod
def from_string(cls, requirements: str) -> "SourceRequirements"
```

Initialize from requirements.txt string, one dependency per line

#### update\_dlt\_extras

```python
def update_dlt_extras(destination_name: str) -> None
```

Update the dlt requirement to include destination

#### is\_installed\_dlt\_compatible

```python
def is_installed_dlt_compatible() -> bool
```

Check whether currently installed version is compatible with dlt requirement

For example, requirements.txt of the source may specify dlt>=0.3.5,<0.4.0
and we check whether the installed dlt version (e.g. 0.3.6) falls within this range.

