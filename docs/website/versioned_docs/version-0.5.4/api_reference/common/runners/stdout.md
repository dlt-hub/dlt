---
sidebar_label: stdout
title: common.runners.stdout
---

## exec\_to\_stdout

```python
@contextmanager
def exec_to_stdout(f: AnyFun) -> Iterator[Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/runners/stdout.py#L17)

Executes parameter-less function f and encodes the pickled return value to stdout. In case of exceptions, encodes the pickled exceptions to stderr

## iter\_std

```python
def iter_std(venv: Venv, command: str,
             *script_args: Any) -> Iterator[Tuple[OutputStdStreamNo, str]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/runners/stdout.py#L31)

Starts a process `command` with `script_args` in environment `venv` and returns iterator
of (filno, line) tuples where `fileno` is 1 for stdout and 2 for stderr. `line` is
a content of a line with stripped new line character.

Use -u in scripts_args for unbuffered python execution

## iter\_stdout\_with\_result

```python
def iter_stdout_with_result(venv: Venv, command: str,
                            *script_args: Any) -> Generator[str, None, Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/runners/stdout.py#L104)

Yields stdout lines coming from remote process and returns the last result decoded with decode_obj. In case of exit code != 0 if exception is decoded
it will be raised, otherwise CalledProcessError is raised

