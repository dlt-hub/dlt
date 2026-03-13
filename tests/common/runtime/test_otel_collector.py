"""
Tests for OpenTelemetryCollector.

Uses in-memory OpenTelemetry exporters to capture and verify spans/metrics
against actual pipeline execution results.
"""

from typing import Any, Dict, List

import pytest

# Skip all tests if opentelemetry is not installed
pytest.importorskip("opentelemetry", reason="OpenTelemetry not installed")
pytest.importorskip("opentelemetry.sdk.trace", reason="OpenTelemetry SDK not installed")

import dlt
from dlt.common.runtime.collector import OpenTelemetryCollector

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from tests.pipeline.utils import load_table_counts


# =============================================================================
# Test Data
# =============================================================================

TEST_DATA: List[Dict[str, Any]] = [
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob", "age": 25},
    {"id": 3, "name": "Charlie", "age": 35},
    {"id": 4, "name": "Diana", "age": 28},
    {"id": 5, "name": "Eve", "age": 32},
]


# Counter for unique pipeline names
_pipeline_counter = 0


def get_unique_pipeline_name(base_name: str) -> str:
    """Generate unique pipeline name to ensure test isolation."""
    global _pipeline_counter
    _pipeline_counter += 1
    return f"{base_name}_{_pipeline_counter}"


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def otel_env():
    """
    Setup in-memory OpenTelemetry exporters for testing.

    Yields a dict containing tracer, meter, and exporters to inspect results.
    """
    # Setup tracing with in-memory exporter
    span_exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(span_exporter))
    tracer = tracer_provider.get_tracer(
        "opentelemetry.instrumentation.dlt.test",
        "0.1.0",
    )

    # Setup metrics with in-memory reader
    metric_reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[metric_reader])
    meter = meter_provider.get_meter(
        "opentelemetry.instrumentation.dlt.test",
        "0.1.0",
    )

    yield {
        "tracer": tracer,
        "meter": meter,
        "span_exporter": span_exporter,
        "metric_reader": metric_reader,
        "tracer_provider": tracer_provider,
        "meter_provider": meter_provider,
    }

    # Cleanup
    span_exporter.clear()
    span_exporter.shutdown()


# =============================================================================
# Initialization Tests
# =============================================================================


def test_otel_collector_initialization_with_custom_tracer_meter(otel_env) -> None:
    """Verify collector initializes correctly with provided tracer and meter."""
    collector = OpenTelemetryCollector(
        tracer=otel_env["tracer"],
        meter=otel_env["meter"],
    )

    assert collector.tracer is otel_env["tracer"]
    assert collector.meter is otel_env["meter"]
    assert collector.enable_logging is True
    assert collector.capture_per_table_metrics is True


def test_otel_collector_metrics_instruments_created(otel_env) -> None:
    """Verify all expected metrics instruments are created during initialization."""
    collector = OpenTelemetryCollector(
        tracer=otel_env["tracer"],
        meter=otel_env["meter"],
    )

    assert hasattr(collector, "rows_counter")
    assert hasattr(collector, "duration_histogram")
    assert hasattr(collector, "tables_counter")
    assert hasattr(collector, "pipeline_runs_counter")


# =============================================================================
# Root Span Tests
# =============================================================================


def test_otel_collector_creates_root_span(otel_env) -> None:
    """Verify a root 'dlt.run' span is created when pipeline runs."""
    collector = OpenTelemetryCollector(
        tracer=otel_env["tracer"],
        meter=otel_env["meter"],
        enable_logging=False,
    )

    pipeline = dlt.pipeline(
        pipeline_name=get_unique_pipeline_name("test_root_span"),
        destination="duckdb",
        dataset_name="test_dataset",
        progress=collector,
    )

    pipeline.run(TEST_DATA, table_name="users")

    # Get finished spans
    spans = otel_env["span_exporter"].get_finished_spans()
    span_names = [span.name for span in spans]

    # Verify root span exists
    assert "dlt.run" in span_names, f"Expected 'dlt.run' span, got: {span_names}"


def test_otel_collector_root_span_has_pipeline_attributes(otel_env) -> None:
    """Verify root span contains expected pipeline attributes."""
    collector = OpenTelemetryCollector(
        tracer=otel_env["tracer"],
        meter=otel_env["meter"],
        enable_logging=False,
    )

    pipeline_name = get_unique_pipeline_name("test_attributes")
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="duckdb",
        dataset_name="attr_dataset",
        progress=collector,
    )

    pipeline.run(TEST_DATA, table_name="users")

    # Find root span
    spans = otel_env["span_exporter"].get_finished_spans()
    root_span = next((s for s in spans if s.name == "dlt.run"), None)

    assert root_span is not None, "Root span 'dlt.run' not found"
    attrs = dict(root_span.attributes)

    assert attrs.get("pipeline.name") == pipeline_name
    assert attrs.get("pipeline.destination") == "duckdb"
    assert "pipeline.success" in attrs


def test_otel_collector_creates_child_spans_for_steps(otel_env) -> None:
    """Verify child spans are created for extract, normalize, and load steps."""
    collector = OpenTelemetryCollector(
        tracer=otel_env["tracer"],
        meter=otel_env["meter"],
        enable_logging=False,
    )

    pipeline = dlt.pipeline(
        pipeline_name=get_unique_pipeline_name("test_child_spans"),
        destination="duckdb",
        dataset_name="child_dataset",
        progress=collector,
    )

    pipeline.run(TEST_DATA, table_name="users")

    spans = otel_env["span_exporter"].get_finished_spans()
    span_names = [span.name for span in spans]

    # All step spans should exist
    assert "dlt.extract" in span_names, f"Expected 'dlt.extract', got: {span_names}"
    assert "dlt.normalize" in span_names, f"Expected 'dlt.normalize', got: {span_names}"
    assert "dlt.load" in span_names, f"Expected 'dlt.load', got: {span_names}"


# =============================================================================
# Row Metrics Tests
# =============================================================================


def test_otel_collector_normalize_row_count_matches_data(otel_env) -> None:
    """Verify normalize step captures correct row count matching input data."""
    collector = OpenTelemetryCollector(
        tracer=otel_env["tracer"],
        meter=otel_env["meter"],
        enable_logging=False,
    )

    pipeline = dlt.pipeline(
        pipeline_name=get_unique_pipeline_name("test_normalize_rows"),
        destination="duckdb",
        dataset_name="normalize_dataset",
        progress=collector,
    )

    pipeline.run(TEST_DATA, table_name="users")

    # Find normalize span
    spans = otel_env["span_exporter"].get_finished_spans()
    normalize_span = next((s for s in spans if s.name == "dlt.normalize"), None)

    assert normalize_span is not None, "Normalize span not found"
    attrs = dict(normalize_span.attributes)

    # Normalize should report rows processed
    assert "normalize.rows" in attrs, f"Expected 'normalize.rows' attribute, got: {list(attrs.keys())}"
    
    # The row count includes all tables (user data + internal _dlt_loads table)
    # Our 5 rows of user data + 1 row in _dlt_loads = 6 total
    # We verify the user data rows are included (normalize.rows >= user data count)
    assert attrs["normalize.rows"] >= len(TEST_DATA), (
        f"Expected at least {len(TEST_DATA)} rows, got {attrs['normalize.rows']}"
    )
    
    # Also verify per-table metrics are captured
    assert "normalize.rows.users" in attrs, f"Expected per-table metric, got: {list(attrs.keys())}"
    assert attrs["normalize.rows.users"] == len(TEST_DATA), (
        f"Expected {len(TEST_DATA)} rows in users table, got {attrs['normalize.rows.users']}"
    )


def test_otel_collector_row_metrics_match_loaded_data(otel_env) -> None:
    """
    Verify row metrics captured by OTEL match actual rows loaded to destination.

    This is the key validation: metrics should reflect reality.
    """
    collector = OpenTelemetryCollector(
        tracer=otel_env["tracer"],
        meter=otel_env["meter"],
        enable_logging=False,
    )

    pipeline = dlt.pipeline(
        pipeline_name=get_unique_pipeline_name("test_row_metrics"),
        destination="duckdb",
        dataset_name="metrics_dataset",
        progress=collector,
    )

    pipeline.run(TEST_DATA, table_name="users", write_disposition="replace")

    # Get actual row count from destination using dlt's helper
    table_counts = load_table_counts(pipeline, "users")
    actual_user_rows = table_counts["users"]

    # Verify actual data loaded matches our test data
    assert actual_user_rows == len(TEST_DATA), (
        f"Expected {len(TEST_DATA)} rows in destination, got {actual_user_rows}"
    )

    # Find normalize span and verify per-table row count matches the destination
    spans = otel_env["span_exporter"].get_finished_spans()
    normalize_span = next((s for s in spans if s.name == "dlt.normalize"), None)

    assert normalize_span is not None
    
    # Check per-table metric matches actual loaded data
    otel_user_rows = normalize_span.attributes.get("normalize.rows.users")
    assert otel_user_rows == actual_user_rows, (
        f"OTEL reported {otel_user_rows} rows for users table but destination has {actual_user_rows}"
    )

