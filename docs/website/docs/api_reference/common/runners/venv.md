---
sidebar_label: venv
title: common.runners.venv
---

## Venv Objects

```python
class Venv()
```

Creates and wraps the Python Virtual Environment to allow for code execution

#### \_\_init\_\_

```python
def __init__(context: types.SimpleNamespace, current: bool = False) -> None
```

Please use `Venv.create`, `Venv.restore` or `Venv.restore_current` methods to create Venv instance

#### create

```python
@classmethod
def create(cls, venv_dir: str, dependencies: List[str] = None) -> "Venv"
```

Creates a new Virtual Environment at the location specified in `venv_dir` and installs `dependencies` via pip. Deletes partially created environment on failure.

#### restore

```python
@classmethod
def restore(cls, venv_dir: str, current: bool = False) -> "Venv"
```

Restores Virtual Environment at `venv_dir`

#### restore\_current

```python
@classmethod
def restore_current(cls) -> "Venv"
```

Wraps the current Python environment.

#### delete\_environment

```python
def delete_environment() -> None
```

Deletes the Virtual Environment.

#### run\_command

```python
def run_command(entry_point: str, *script_args: Any) -> str
```

Runs any `command` with specified `script_args`. Current `os.environ` and cwd is passed to executed process

#### run\_script

```python
def run_script(script_path: str, *script_args: Any) -> str
```

Runs a python `script` source with specified `script_args`. Current `os.environ` and cwd is passed to executed process

#### run\_module

```python
def run_module(module: str, *module_args: Any) -> str
```

Runs a python `module` with specified `module_args`. Current `os.environ` and cwd is passed to executed process

#### is\_virtual\_env

```python
@staticmethod
def is_virtual_env() -> bool
```

Checks if we are running in virtual environment

#### is\_venv\_activated

```python
@staticmethod
def is_venv_activated() -> bool
```

Checks if virtual environment is activated in the shell

