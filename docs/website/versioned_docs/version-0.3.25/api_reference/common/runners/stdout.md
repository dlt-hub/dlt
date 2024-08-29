---
sidebar_label: stdout
title: common.runners.stdout
---

## exec\_to\_stdout

```python
@contextmanager
def exec_to_stdout(f: AnyFun) -> Iterator[Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/runners/stdout.py#L13)

Executes parameter-less function f and encodes the pickled return value to stdout. In case of exceptions, encodes the pickled exceptions to stderr

## iter\_stdout\_with\_result

```python
def iter_stdout_with_result(venv: Venv, command: str,
                            *script_args: Any) -> Generator[str, None, Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/runners/stdout.py#L62)

Yields stdout lines coming from remote process and returns the last result decoded with decode_obj. In case of exit code != 0 if exception is decoded
it will be raised, otherwise CalledProcessError is raised

