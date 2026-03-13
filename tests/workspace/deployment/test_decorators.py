"""Tests for @job and @interactive decorators."""

import asyncio
import inspect

import pytest

import dlt
from dlt._workspace.deployment.decorators import JobFactory, interactive, job
from dlt._workspace.deployment.typing import DEFAULT_HTTP_PORT, TTrigger


# module-level sources and resources for deliver tests


@dlt.source
def _deliver_source():
    @dlt.resource
    def r():
        yield 1

    return r


@dlt.resource
def _deliver_standalone_resource():
    yield 1


def test_job_decorator() -> None:
    """@job without and with parens, callable, metadata, async."""

    # no parens
    @job
    def no_parens():
        """A simple job."""
        return "done"

    assert isinstance(no_parens, JobFactory)
    assert no_parens.name == "no_parens"
    assert no_parens.job_type == "batch"
    assert no_parens() == "done"

    # with parens and args
    @job(trigger="0 8 * * *", timeout="4h", concurrency=3)
    def with_parens():
        return "scheduled"

    assert with_parens.timeout == "4h"
    assert with_parens.concurrency == 3
    assert with_parens.trigger == [TTrigger("0 8 * * *")]
    assert with_parens() == "scheduled"

    # custom name and section
    @job(name="custom", section="my_section")
    def ignored_name():
        pass

    assert ignored_name.job_ref == "jobs.my_section.custom"

    # starred and tags
    @job(starred=True, tags=["ingestion"])
    def tagged():
        pass

    job_def = tagged.to_job_definition()
    assert job_def["starred"] is True
    assert job_def["tags"] == ["ingestion"]

    # async
    @job
    async def async_fn():
        return "async_done"

    assert inspect.iscoroutinefunction(async_fn._deco_f)
    assert asyncio.run(async_fn()) == "async_done"

    # signature preserved
    @job
    def with_params(x: int, y: str = "hello"):
        return f"{x}-{y}"

    sig = inspect.signature(with_params)
    assert "x" in sig.parameters
    assert "y" in sig.parameters


def test_interactive_decorator() -> None:
    """@interactive without and with parens, defaults, custom settings, async."""

    # no parens — defaults
    @interactive
    def default_app():
        pass

    assert isinstance(default_app, JobFactory)
    assert default_app.job_type == "interactive"
    assert default_app.expose == {"interface": "gui", "port": DEFAULT_HTTP_PORT}
    assert default_app.trigger == [TTrigger(f"http:{DEFAULT_HTTP_PORT}")]

    # custom port and interface
    @interactive(port=9090, interface="rest_api")
    def custom_api():
        pass

    assert custom_api.expose == {"interface": "rest_api", "port": 9090}
    assert custom_api.trigger == [TTrigger("http:9090")]

    # mcp interface
    @interactive(interface="mcp")
    def mcp_server():
        """MCP tool server."""
        pass

    job_def = mcp_server.to_job_definition()
    assert job_def["entry_point"]["expose"]["interface"] == "mcp"
    assert job_def["description"] == "MCP tool server."

    # run_params
    @interactive(port=8080, run_params={"ws_port": 8081})
    def with_ws():
        pass

    assert with_ws.expose["run_params"] == {"ws_port": 8081}
    job_def = with_ws.to_job_definition()
    assert job_def["entry_point"]["expose"]["run_params"] == {"ws_port": 8081}

    # async
    @interactive(port=3000)
    async def async_app():
        return "async_interactive"

    assert asyncio.run(async_app()) == "async_interactive"
    assert async_app.job_type == "interactive"


def test_trigger_properties_and_chaining() -> None:
    """Trigger properties (.success/.fail/.completed) and chaining."""

    @job
    def upstream():
        pass

    section = upstream.section

    # trigger properties
    assert upstream.success == TTrigger(f"job.success:jobs.{section}.upstream")
    assert upstream.fail == TTrigger(f"job.fail:jobs.{section}.upstream")
    assert upstream.completed == (upstream.success, upstream.fail)

    # chaining via trigger list
    @job(trigger=[upstream.success, "tag:backfill"])
    def downstream():
        pass

    assert downstream.trigger[0] == upstream.success
    assert downstream.trigger[1] == TTrigger("tag:backfill")

    # trigger properties on interactive jobs
    @interactive
    def my_app():
        pass

    assert my_app.success == TTrigger(f"job.success:jobs.{my_app.section}.my_app")


def test_job_definition_batch() -> None:
    """to_job_definition produces correct TJobDefinition for batch jobs."""

    @job(trigger="0 8 * * *", timeout="4h", starred=True, tags=["etl"])
    def etl():
        """Daily ETL."""
        pass

    job_def = etl.to_job_definition()
    assert job_def["job_ref"] == f"jobs.{etl.section}.etl"
    assert job_def["entry_point"]["job_type"] == "batch"
    assert job_def["entry_point"]["function"] == "etl"
    assert "launcher" not in job_def["entry_point"]
    assert "expose" not in job_def["entry_point"]
    assert job_def["triggers"] == [TTrigger("0 8 * * *")]
    assert job_def["execution"]["timeout"] == {"timeout": 14400.0}
    assert job_def["execution"]["concurrency"] == 1
    assert job_def["starred"] is True
    assert job_def["tags"] == ["etl"]
    assert job_def["description"] == "Daily ETL."


def test_job_definition_interactive() -> None:
    """to_job_definition produces correct TJobDefinition for interactive jobs."""

    @interactive(port=9090, interface="rest_api")
    def api():
        pass

    job_def = api.to_job_definition()
    assert job_def["entry_point"]["job_type"] == "interactive"
    assert job_def["entry_point"]["expose"] == {"interface": "rest_api", "port": 9090}
    assert "launcher" not in job_def["entry_point"]
    assert job_def["triggers"] == [TTrigger("http:9090")]


def test_job_definition_omits_unset_fields() -> None:
    """Optional fields are omitted from job definition when not set."""

    @job
    def bare():
        pass

    job_def = bare.to_job_definition()
    assert "description" not in job_def
    assert "timeout" not in job_def["execution"]
    assert "tags" not in job_def
    assert "deliver" not in job_def
    assert job_def["triggers"] == []


def test_config_key_discovery() -> None:
    """Config keys from dlt.config.value defaults are discovered in job definition."""

    @job
    def with_config(api_key=dlt.config.value, limit=dlt.config.value):
        pass

    job_def = with_config.to_job_definition()
    assert "api_key" in job_def["config_keys"]
    assert "limit" in job_def["config_keys"]


def test_isinstance_check() -> None:
    """Both @job and @interactive produce JobFactory instances."""

    @job
    def batch_fn():
        pass

    @interactive
    def interactive_fn():
        pass

    assert isinstance(batch_fn, JobFactory)
    assert isinstance(interactive_fn, JobFactory)
    assert not isinstance(lambda: None, JobFactory)


def test_deliver_targets() -> None:
    """deliver accepts source factory, called source, standalone resource.
    Rejects inner resources and invalid types.
    """

    # source factory (uncalled @dlt.source)
    @job(deliver=_deliver_source)
    def from_factory():
        pass

    job_def = from_factory.to_job_definition()
    ref = job_def["deliver"]["source_ref"]
    assert ref == f"sources.{_deliver_source.ref.section}.{_deliver_source.ref.name}"

    # called source instance (DltSource with _factory)
    source_instance = _deliver_source()

    @job(deliver=source_instance)
    def from_instance():
        pass

    job_def = from_instance.to_job_definition()
    assert job_def["deliver"]["source_ref"] == ref

    # standalone resource (module-level DltResource with _factory)
    @job(deliver=_deliver_standalone_resource)
    def from_resource():
        pass

    job_def = from_resource.to_job_definition()
    factory_ref = _deliver_standalone_resource._factory.ref  # type: ignore[attr-defined]
    assert job_def["deliver"]["source_ref"] == f"sources.{factory_ref.section}.{factory_ref.name}"

    # inner resource (no _factory) raises on to_job_definition
    inner_resource = list(source_instance.resources.values())[0]

    @job(deliver=inner_resource)
    def from_inner():
        pass

    with pytest.raises(ValueError, match="only top-level standalone"):
        from_inner.to_job_definition()

    # invalid type raises on to_job_definition
    @job(deliver="not_a_source")  # type: ignore[call-overload]
    def from_invalid():
        pass

    with pytest.raises(ValueError, match="deliver must be"):
        from_invalid.to_job_definition()


@pytest.mark.parametrize(
    "input_val,expected_seconds",
    [
        ("4h", 14400.0),
        ("30m", 1800.0),
        (3600, 3600.0),
        (3600.0, 3600.0),
    ],
    ids=["4h", "30m", "int-3600", "float-3600"],
)
def test_timeout_smoke(input_val: object, expected_seconds: float) -> None:
    """Timeout values normalize correctly through to job definition."""

    @job(timeout=input_val)  # type: ignore[call-overload]
    def my_job():
        pass

    job_def = my_job.to_job_definition()
    assert job_def["execution"]["timeout"]["timeout"] == expected_seconds


def test_timeout_dict_passthrough() -> None:
    """TTimeoutSpec dict with grace_period passes through unchanged."""

    @job(timeout={"timeout": 100.0, "grace_period": 10.0})
    def my_job():
        pass

    job_def = my_job.to_job_definition()
    assert job_def["execution"]["timeout"] == {"timeout": 100.0, "grace_period": 10.0}
