---
title: Tracing
description: Rich information on executed dlt pipelines
keywords: [tracing, opentelemetry, sentry, opt in, observability, metrics, spans]
---

# Tracing

## OpenTelemetry

`dlt` provides native OpenTelemetry integration through the `OpenTelemetryCollector`, enabling you to export traces and metrics from your pipelines to any OpenTelemetry-compatible backend (Jaeger, Grafana Tempo, Prometheus, Datadog, etc.). **OpenTelemetry tracing is disabled by default.**

### What data is collected

The `OpenTelemetryCollector` captures comprehensive telemetry data during pipeline execution:

**Traces (Spans):**
- A root span (`dlt.run`) for the entire pipeline execution
- Child spans for each pipeline step: `dlt.extract`, `dlt.normalize`, `dlt.load`
- Span attributes including pipeline name, destination, dataset, duration, and row counts
- Exception recording and status codes for error tracking

**Metrics:**
- `dlt.pipeline.rows.total` - Total rows processed (by step)
- `dlt.pipeline.step.duration` - Duration of each pipeline step
- `dlt.pipeline.tables.count` - Number of tables in the pipeline
- `dlt.pipeline.runs.total` - Total pipeline runs (with success/failure status)
- `dlt.system.memory_usage_mb` - Process memory usage
- `dlt.system.memory_percent` - System memory percentage
- `dlt.system.cpu_percent` - Process CPU usage

### Enable OpenTelemetry tracing

First, install the OpenTelemetry dependencies:

```sh
uv add dlt[otel]
```

For exporting to an OTLP collector (recommended), also install:

```sh
uv add opentelemetry-exporter-otlp
```

#### Basic usage

The simplest way to use OpenTelemetry is to pass the `OpenTelemetryCollector` to the `progress` parameter of your pipeline:

```py
import dlt
from dlt.common.runtime.collector import OpenTelemetryCollector

# Create the collector
otel_collector = OpenTelemetryCollector()

# Create pipeline with OpenTelemetry collector
pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination="duckdb",
    progress=otel_collector,
)

# Run the pipeline - traces and metrics are automatically exported
pipeline.run(data, table_name="my_table")
```

#### Full setup with OTLP exporter

For production use, configure OpenTelemetry with an exporter to send data to your observability backend:

```py
import dlt
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

from dlt.common.runtime.collector import OpenTelemetryCollector

# Create a resource identifying your service
resource = Resource.create({
    "service.name": "my-data-pipeline",
})

# Configure the OTLP endpoint (e.g., Jaeger, Grafana Agent, or OpenTelemetry Collector)
otlp_endpoint = "http://localhost:4317"

# Set up tracing
trace_provider = TracerProvider(resource=resource)
trace_provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True))
)
trace.set_tracer_provider(trace_provider)

# Set up metrics
metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True),
    export_interval_millis=5000,
)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)

# Create the OpenTelemetry collector
otel_collector = OpenTelemetryCollector(
    capture_per_table_metrics=True,  # Include per-table row counts as span attributes
    enable_logging=True,             # Also log progress to console
)

# Create and run pipeline
pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination="duckdb",
    progress=otel_collector,
)

pipeline.run(data, table_name="my_table")
```

### Span attributes

The collector adds detailed attributes to each span:

**Root span (`dlt.run`):**
- `pipeline.name` - Pipeline name
- `pipeline.destination` - Destination type (e.g., "duckdb", "bigquery")
- `pipeline.dataset` - Dataset name
- `pipeline.total_rows` - Total rows processed
- `pipeline.tables` - Number of tables
- `pipeline.duration` - Total duration in seconds
- `pipeline.success` - Whether the run succeeded

**Extract span (`dlt.extract`):**
- `extract.rows` - Rows extracted
- `extract.resources` - Resource names
- `extract.tables` - Table names
- `extract.schema_name` - Schema name

**Normalize span (`dlt.normalize`):**
- `normalize.rows` - Rows normalized
- `normalize.tables` - Table names
- `normalize.table_count` - Number of tables
- `normalize.rows.<table_name>` - Per-table row counts (if enabled)

**Load span (`dlt.load`):**
- `load.rows` - Rows loaded
- `load.tables` - Number of tables
- `load.destination` - Destination name
- `load.dataset` - Dataset name
- `load.jobs.completed` - Completed load jobs
- `load.jobs.failed` - Failed load jobs

## Sentry
`dlt` users can configure [Sentry](https://sentry.io) DSN to start receiving rich information on
executed pipelines, including encountered errors and exceptions. **Sentry tracing is disabled by
default.**

### When and what we send

An exception trace is sent when:

- Any Python logger (including `dlt`) logs an error.
- Any Python logger (including `dlt`) logs a warning (enabled only if the `dlt` logging level is
  `WARNING` or below).
- On unhandled exceptions.

A transaction trace is sent when the `pipeline.run` is called. We send information when
[extract, normalize, and load](../reference/explainers/how-dlt-works.md) steps are completed.

The data available in Sentry makes finding and documenting bugs easy, allowing you to easily find
bottlenecks and profile data extraction, normalization, and loading.

`dlt` adds a set of additional tags (e.g., pipeline name, destination name) to the Sentry data.

Please refer to the Sentry [documentation](https://docs.sentry.io/platforms/python/data-collected/).

### Enable pipeline tracing

To enable Sentry, you should configure the
[DSN](https://docs.sentry.io/product/sentry-basics/dsn-explainer/) in the `config.toml`:

```toml
[runtime]

sentry_dsn="https:///<...>"
```

Alternatively, you can use environment variables:

```sh
RUNTIME__SENTRY_DSN="https:///<...>"
```

The Sentry client is configured after the first pipeline is created with `dlt.pipeline()`. Feel free
to use `sentry_sdk` init again to cover your specific needs.

> 💡 `dlt` does not have Sentry client as a dependency. Remember to install it with `pip install sentry-sdk`.

## Disable all tracing

`dlt` allows you to completely disable pipeline tracing, including the anonymous telemetry and
Sentry. **Note: this does not include the OpenTelemetryCollector**.  Using `config.toml`:

```toml
enable_runtime_trace=false
```

