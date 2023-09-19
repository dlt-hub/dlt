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

Temporarily answer all confirmations and prompts with the values specified in arguments

