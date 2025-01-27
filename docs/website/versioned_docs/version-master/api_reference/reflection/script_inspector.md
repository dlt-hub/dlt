---
sidebar_label: script_inspector
title: reflection.script_inspector
---

## DummyModule Objects

```python
class DummyModule(ModuleType)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/reflection/script_inspector.py#L23)

A dummy module from which you can import anything

## import\_script\_module

```python
def import_script_module(module_path: str,
                         script_relative_path: str,
                         ignore_missing_imports: bool = False) -> ModuleType
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/reflection/script_inspector.py#L90)

Loads a module in `script_relative_path` by splitting it into a script module (file part) and package (folders).  `module_path` is added to sys.path
Optionally, missing imports will be ignored by importing a dummy module instead.

