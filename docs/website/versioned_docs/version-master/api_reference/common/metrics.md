---
sidebar_label: metrics
title: common.metrics
---

## StepMetrics Objects

```python
class StepMetrics(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/metrics.py#L25)

Metrics for particular package processed in particular pipeline step

### started\_at

Start of package processing

### finished\_at

End of package processing

## ExtractMetrics Objects

```python
class ExtractMetrics(StepMetrics)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/metrics.py#L39)

### job\_metrics

Metrics collected per job id during writing of job file

### table\_metrics

Job metrics aggregated by table

### resource\_metrics

Job metrics aggregated by resource

### dag

A resource dag where elements of the list are graph edges

### hints

Hints passed to the resources

## NormalizeMetrics Objects

```python
class NormalizeMetrics(StepMetrics)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/metrics.py#L53)

### job\_metrics

Metrics collected per job id during writing of job file

### table\_metrics

Job metrics aggregated by table

