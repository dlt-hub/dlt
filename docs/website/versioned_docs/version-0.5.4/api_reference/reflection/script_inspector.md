---
sidebar_label: script_inspector
title: reflection.script_inspector
---

## DummyModule Objects

```python
class DummyModule(ModuleType)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/reflection/script_inspector.py#L23)

A dummy module from which you can import anything

## load\_script\_module

```python
def load_script_module(module_path: str,
                       script_relative_path: str,
                       ignore_missing_imports: bool = False) -> ModuleType
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/reflection/script_inspector.py#L90)

Loads a module in `script_relative_path` by splitting it into a script module (file part) and package (folders).  `module_path` is added to sys.path
Optionally, missing imports will be ignored by importing a dummy module instead.

