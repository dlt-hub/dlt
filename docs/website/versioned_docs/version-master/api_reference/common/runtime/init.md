---
sidebar_label: init
title: common.runtime.init
---

## restore\_run\_context

```python
def restore_run_context(run_context: SupportsRunContext,
                        runtime_config: RuntimeConfiguration) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/init.py#L33)

Restores `run_context` by placing it into container and if `runtime_config` is present, initializes runtime
Intended to be called by workers in a process pool.

## init\_telemetry

```python
def init_telemetry(runtime_config: RuntimeConfiguration) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/init.py#L46)

Starts telemetry only once

## apply\_runtime\_config

```python
def apply_runtime_config(runtime_config: RuntimeConfiguration) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/init.py#L57)

Updates run context with newest runtime_config

