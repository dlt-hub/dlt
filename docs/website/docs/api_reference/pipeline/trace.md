---
sidebar_label: trace
title: pipeline.trace
---

## SerializableResolvedValueTrace Objects

```python
class SerializableResolvedValueTrace(NamedTuple)
```

Information on resolved secret and config values

#### asdict

```python
def asdict() -> StrAny
```

A dictionary representation that is safe to load.

## \_PipelineStepTrace Objects

```python
@dataclasses.dataclass(init=True)
class _PipelineStepTrace()
```

#### step\_info

A step outcome info ie. LoadInfo

#### step\_exception

For failing steps contains exception string

## PipelineStepTrace Objects

```python
class PipelineStepTrace(_PipelineStepTrace)
```

Trace of particular pipeline step, contains timing information, the step outcome info or exception in case of failing step with custom asdict()

#### asdict

```python
def asdict() -> DictStrAny
```

A dictionary representation of PipelineStepTrace that can be loaded with `dlt`

## PipelineTrace Objects

```python
@dataclasses.dataclass(init=True)
class PipelineTrace()
```

Pipeline runtime trace containing data on "extract", "normalize" and "load" steps and resolved config and secret values.

#### steps

A list of steps in the trace

#### resolved\_config\_values

A list of resolved config values

#### merge\_traces

```python
def merge_traces(last_trace: PipelineTrace,
                 new_trace: PipelineTrace) -> PipelineTrace
```

Merges `new_trace` into `last_trace` by combining steps and timestamps. `new_trace` replace the `last_trace` if it has more than 1 step.`

#### describe\_extract\_data

```python
def describe_extract_data(data: Any) -> List[ExtractDataInfo]
```

Extract source and resource names from data passed to extract

