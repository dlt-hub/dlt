---
sidebar_label: echo
title: cli.echo
---

#### always\_choose

```python
@contextlib.contextmanager
def always_choose(always_choose_default: bool,
                  always_choose_value: Any) -> Iterator[None]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/cli/echo.py#L11)

Temporarily answer all confirmations and prompts with the values specified in arguments

