---
sidebar_label: trace
title: pipeline.trace
---

## SerializableResolvedValueTrace Objects

```python
class SerializableResolvedValueTrace(NamedTuple)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/trace.py#L41)

Information on resolved secret and config values

### asdict

```python
def asdict() -> StrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/trace.py#L52)

A dictionary representation that is safe to load.

## \_PipelineStepTrace Objects

```python
class _PipelineStepTrace(NamedTuple)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/trace.py#L63)

### step\_info

A step outcome info ie. LoadInfo

### step\_exception

For failing steps contains exception string

### exception\_traces

For failing steps contains traces of exception chain causing it

## PipelineStepTrace Objects

```python
class PipelineStepTrace(SupportsHumanize, _PipelineStepTrace)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/trace.py#L76)

Trace of particular pipeline step, contains timing information, the step outcome info or exception in case of failing step with custom asdict()

### asdict

```python
def asdict() -> DictStrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/trace.py#L97)

A dictionary representation of PipelineStepTrace that can be loaded with `dlt`

## \_PipelineTrace Objects

```python
class _PipelineTrace(NamedTuple)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/trace.py#L125)

Pipeline runtime trace containing data on "extract", "normalize" and "load" steps and resolved config and secret values.

### steps

A list of steps in the trace

### resolved\_config\_values

A list of resolved config values

## PipelineTrace Objects

```python
class PipelineTrace(SupportsHumanize, _PipelineTrace)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/trace.py#L140)

### asdict

```python
def asdict() -> DictStrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/trace.py#L167)

A dictionary representation of PipelineTrace that can be loaded with `dlt`

## merge\_traces

```python
def merge_traces(last_trace: PipelineTrace,
                 new_trace: PipelineTrace) -> PipelineTrace
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/trace.py#L313)

Merges `new_trace` into `last_trace` by combining steps and timestamps. `new_trace` replace the `last_trace` if it has more than 1 step.`

## get\_exception\_traces

```python
def get_exception_traces(exc: BaseException,
                         container: Container = None) -> List[ExceptionTrace]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/trace.py#L346)

Gets exception trace chain and extend it with data available in Container context

