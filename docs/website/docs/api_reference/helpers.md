---
sidebar_label: helpers
title: helpers
---

#### retry\_load

```python
def retry_load(retry_on_pipeline_steps: Sequence[TPipelineStep] = (
    "load", )) -> Callable[[BaseException], bool]
```

A retry strategy for Tenacity that, with default setting, will repeat `load` step for all exceptions that are not terminal

Use this condition with tenacity `retry_if_exception`. Terminal exceptions are exceptions that will not go away when operations is repeated.
Examples: missing configuration values, Authentication Errors, terminally failed jobs exceptions etc.

&gt;&gt;&gt; data = source(...)
&gt;&gt;&gt; for attempt in Retrying(stop=stop_after_attempt(3), retry=retry_if_exception(retry_load(())), reraise=True):
&gt;&gt;&gt;     with attempt:
&gt;&gt;&gt;         p.run(data)

**Arguments**:

- `retry_on_pipeline_steps` _Tuple[TPipelineStep, ...], optional_ - which pipeline steps are allowed to be repeated. Default: &quot;load&quot;

