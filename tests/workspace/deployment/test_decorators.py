"""Tests for @job, @interactive, and @pipeline_run decorators."""

import asyncio
import inspect
from typing import Any, Dict, List, Literal, cast

import pytest

import dlt
from dlt._workspace.deployment.decorators import JobFactory, interactive, job, pipeline_run
from dlt._workspace.deployment.exceptions import InvalidJobName, InvalidJobSection
from dlt._workspace.deployment.typing import TTrigger


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
    @job(trigger="0 8 * * *", execute={"timeout": {"timeout": 14400.0}})
    def with_parens():
        return "scheduled"

    assert with_parens.execute == {"timeout": {"timeout": 14400.0}}
    assert with_parens.trigger == [TTrigger("schedule:0 8 * * *")]
    assert with_parens() == "scheduled"

    # custom name and section
    @job(name="custom", section="my_section")
    def ignored_name():
        pass

    assert ignored_name.job_ref == "jobs.my_section.custom"

    # expose spec
    @job(expose={"starred": True, "tags": ["ingestion"]})
    def tagged():
        pass

    job_def = tagged.to_job_definition()
    assert job_def["expose"]["starred"] is True
    assert job_def["expose"]["tags"] == ["ingestion"]

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


@pytest.mark.parametrize(
    "decorator_factory",
    [
        lambda name: job(name=name),
        lambda name: interactive(name=name),
        lambda name: pipeline_run("p", name=name),
    ],
    ids=["job", "interactive", "pipeline_run"],
)
@pytest.mark.parametrize(
    "bad_name",
    ["with space", "with-dash", "with.dot", "1leading_digit", "", "name!"],
    ids=["space", "dash", "dot", "leading-digit", "empty", "punctuation"],
)
def test_decorator_rejects_non_identifier_name(decorator_factory: Any, bad_name: str) -> None:
    """Decorator-supplied name must be a valid Python identifier."""
    with pytest.raises(InvalidJobName) as exc_info:
        decorator_factory(bad_name)

    msg = str(exc_info.value)
    assert bad_name in msg or repr(bad_name) in msg
    # error nudges the user toward expose.display_name
    assert "display_name" in msg
    assert exc_info.value.name == bad_name


@pytest.mark.parametrize(
    "good_name",
    ["snake_case", "_underscore_prefix", "camelCase", "with_digits_123", "x"],
    ids=["snake", "underscore", "camel", "digits", "single-letter"],
)
def test_decorator_accepts_valid_identifier_name(good_name: str) -> None:
    """Valid Python identifiers are accepted as decorator names."""

    @job(name=good_name, section="sec")
    def fn():
        pass

    assert fn.name == good_name
    assert fn.job_ref == f"jobs.sec.{good_name}"


@pytest.mark.parametrize(
    "decorator_factory",
    [
        lambda section: job(section=section),
        lambda section: interactive(section=section),
        lambda section: pipeline_run("p", section=section),
    ],
    ids=["job", "interactive", "pipeline_run"],
)
@pytest.mark.parametrize(
    "bad_section",
    ["with space", "with-dash", "with.dot", "1leading_digit", "", "section!"],
    ids=["space", "dash", "dot", "leading-digit", "empty", "punctuation"],
)
def test_decorator_rejects_non_identifier_section(decorator_factory: Any, bad_section: str) -> None:
    """Decorator-supplied section must be a valid Python identifier."""
    with pytest.raises(InvalidJobSection) as exc_info:
        decorator_factory(bad_section)

    msg = str(exc_info.value)
    assert bad_section in msg or repr(bad_section) in msg
    assert "section" in msg.lower()
    assert exc_info.value.section == bad_section


@pytest.mark.parametrize(
    "good_section",
    ["snake_case", "_underscore_prefix", "camelCase", "with_digits_123", "x"],
    ids=["snake", "underscore", "camel", "digits", "single-letter"],
)
def test_decorator_accepts_valid_identifier_section(good_section: str) -> None:
    """Valid Python identifiers are accepted as decorator sections."""

    @job(name="my_job", section=good_section)
    def fn():
        pass

    assert fn.section == good_section
    assert fn.job_ref == f"jobs.{good_section}.my_job"


def test_decorator_display_name_in_expose() -> None:
    """`expose.display_name` is preserved on the job definition."""

    @job(name="daily_etl", expose={"display_name": "Daily ETL (production)"})
    def fn():
        pass

    job_def = fn.to_job_definition()
    assert job_def["expose"]["display_name"] == "Daily ETL (production)"
    # technical name is unchanged
    assert fn.name == "daily_etl"

    # display_name passes through interactive and pipeline_run too
    @interactive(expose={"display_name": "My API"})
    def api():
        pass

    assert api.to_job_definition()["expose"]["display_name"] == "My API"

    @pipeline_run("analytics", expose={"display_name": "Analytics loader"})
    def loader():
        pass

    assert loader.to_job_definition()["expose"]["display_name"] == "Analytics loader"


def test_interactive_decorator() -> None:
    """@interactive without and with parens, defaults, custom settings, async."""

    # no parens — defaults
    @interactive
    def default_app():
        pass

    assert isinstance(default_app, JobFactory)
    assert default_app.job_type == "interactive"
    assert default_app.expose == {"interface": "gui"}
    assert default_app.trigger == [TTrigger("http:")]

    # custom interface
    @interactive(interface="rest_api")
    def custom_api():
        pass

    assert custom_api.expose == {"interface": "rest_api"}
    assert custom_api.trigger == [TTrigger("http:")]

    # mcp interface
    @interactive(interface="mcp")
    def mcp_server():
        """MCP tool server."""
        pass

    job_def = mcp_server.to_job_definition()
    assert job_def["expose"]["interface"] == "mcp"
    assert job_def["description"] == "MCP tool server."

    # async
    @interactive
    async def async_app():
        return "async_interactive"

    assert asyncio.run(async_app()) == "async_interactive"
    assert async_app.job_type == "interactive"

    # concurrency defaults to 1 for interactive
    assert default_app.execute["concurrency"] == 1


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

    @job(
        trigger="0 8 * * *",
        execute={"timeout": {"timeout": 14400.0}},
        expose={"starred": True, "tags": ["etl"]},
    )
    def etl():
        """Daily ETL."""
        pass

    job_def = etl.to_job_definition()
    assert job_def["job_ref"] == f"jobs.{etl.section}.etl"
    assert job_def["entry_point"]["job_type"] == "batch"
    assert job_def["entry_point"]["function"] == "etl"
    assert "launcher" not in job_def["entry_point"]
    assert job_def["triggers"] == [TTrigger("schedule:0 8 * * *")]
    assert job_def["execute"]["timeout"] == {"timeout": 14400.0}
    assert "concurrency" not in job_def["execute"]
    assert job_def["expose"]["starred"] is True
    assert job_def["expose"]["tags"] == ["etl"]
    assert job_def["description"] == "Daily ETL."


def test_job_definition_interactive() -> None:
    """to_job_definition produces correct TJobDefinition for interactive jobs."""

    @interactive(interface="rest_api")
    def api():
        pass

    job_def = api.to_job_definition()
    assert job_def["entry_point"]["job_type"] == "interactive"
    assert job_def["expose"] == {"interface": "rest_api"}
    assert "expose" not in job_def["entry_point"]
    assert job_def["triggers"] == [TTrigger("http:")]


def test_job_definition_omits_unset_fields() -> None:
    """Optional fields are omitted from job definition when not set."""

    @job
    def bare():
        pass

    job_def = bare.to_job_definition()
    assert "description" not in job_def
    assert "timeout" not in job_def["execute"]
    assert "tags" not in job_def
    assert "deliver" not in job_def
    assert "expose" not in job_def
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
    "timeout_spec,expected_timeout",
    [
        ({"timeout": 14400.0}, {"timeout": 14400.0}),
        ({"timeout": 1800.0}, {"timeout": 1800.0}),
        ({"timeout": 3600.0, "grace_period": 10.0}, {"timeout": 3600.0, "grace_period": 10.0}),
    ],
    ids=["14400s", "1800s", "with-grace"],
)
def test_execute_timeout(timeout_spec: Dict[str, Any], expected_timeout: Dict[str, Any]) -> None:
    """Execute spec timeout passes through to job definition."""

    @job(execute={"timeout": timeout_spec})  # type: ignore[call-overload]
    def my_job():
        pass

    job_def = my_job.to_job_definition()
    assert job_def["execute"]["timeout"] == expected_timeout


def test_interactive_idle_timeout() -> None:
    """idle_timeout on interactive builds execute spec with grace_period."""

    @interactive(idle_timeout="24h")
    def my_app():
        pass

    job_def = my_app.to_job_definition()
    assert job_def["execute"]["timeout"]["timeout"] == 86400.0
    assert job_def["execute"]["timeout"]["grace_period"] == 5.0
    assert job_def["execute"]["concurrency"] == 1


def test_freshness_single_string() -> None:
    """Single freshness constraint string is normalized."""

    @job(freshness="job.is_matching_interval_fresh:jobs.upstream")
    def my_job():
        pass

    job_def = my_job.to_job_definition()
    assert job_def["freshness"] == ["job.is_matching_interval_fresh:jobs.upstream"]


def test_freshness_single_property() -> None:
    """Freshness constraint from JobFactory property."""

    @job
    def upstream():
        pass

    @job(freshness=upstream.is_matching_interval_fresh)
    def downstream():
        pass

    job_def = downstream.to_job_definition()
    assert len(job_def["freshness"]) == 1
    assert "job.is_matching_interval_fresh:" in job_def["freshness"][0]


def test_freshness_multiple() -> None:
    """Multiple freshness constraints from mixed sources."""

    @job
    def up_a():
        pass

    @job
    def up_b():
        pass

    @job(freshness=[up_a.is_matching_interval_fresh, up_b.is_fresh])
    def downstream():
        pass

    job_def = downstream.to_job_definition()
    assert len(job_def["freshness"]) == 2
    assert any("is_matching_interval_fresh" in f for f in job_def["freshness"])
    assert any("is_fresh" in f for f in job_def["freshness"])


def test_pipeline_run_with_string_name() -> None:
    """pipeline_run with string pipeline name."""

    @pipeline_run("my_pipeline")
    def load_data():
        pass

    assert isinstance(load_data, JobFactory)
    job_def = load_data.to_job_definition()
    assert job_def["deliver"]["pipeline_name"] == "my_pipeline"
    assert job_def["expose"]["category"] == "pipeline"
    assert job_def["entry_point"]["job_type"] == "batch"


def test_pipeline_run_with_trigger() -> None:
    """pipeline_run with schedule trigger."""

    @pipeline_run("analytics", trigger="0 8 * * *")
    def daily_analytics():
        """Load analytics daily."""
        pass

    job_def = daily_analytics.to_job_definition()
    assert job_def["deliver"]["pipeline_name"] == "analytics"
    assert job_def["triggers"] == [TTrigger("schedule:0 8 * * *")]
    assert job_def["expose"]["category"] == "pipeline"
    assert job_def["description"] == "Load analytics daily."


def test_pipeline_run_with_expose_override() -> None:
    """pipeline_run with custom expose — category defaults to pipeline."""

    @pipeline_run("etl", expose={"starred": True, "tags": ["daily"]})
    def starred_etl():
        pass

    job_def = starred_etl.to_job_definition()
    assert job_def["expose"]["category"] == "pipeline"
    assert job_def["expose"]["starred"] is True
    assert job_def["expose"]["tags"] == ["daily"]


def test_job_refresh_default_auto_omitted_from_manifest() -> None:
    """Default `refresh="auto"` is not written to the manifest dict."""

    @job
    def default_job():
        pass

    assert default_job.refresh == "auto"
    job_def = default_job.to_job_definition()
    assert "refresh" not in job_def


@pytest.mark.parametrize("policy", ["always", "block"])
def test_job_refresh_non_default_written_to_manifest(policy: str) -> None:
    """Non-default refresh values are written to the manifest dict."""
    refresh_policy = cast(Literal["always", "auto", "block"], policy)

    @job(refresh=refresh_policy)
    def explicit_job():
        pass

    assert explicit_job.refresh == policy
    job_def = explicit_job.to_job_definition()
    assert job_def["refresh"] == policy


def test_job_refresh_explicit_auto_omitted() -> None:
    """Explicitly passing `refresh="auto"` still results in no manifest field."""

    @job(refresh="auto")
    def explicit_auto():
        pass

    job_def = explicit_auto.to_job_definition()
    assert "refresh" not in job_def


def test_pipeline_run_refresh() -> None:
    """`pipeline_run` accepts and propagates the refresh policy."""

    @pipeline_run("analytics", refresh="always")
    def loader():
        pass

    assert loader.refresh == "always"
    job_def = loader.to_job_definition()
    assert job_def["refresh"] == "always"


@pytest.mark.parametrize(
    "decorator_id,tags_input,expected",
    [
        ("job", "ingestion", ["ingestion"]),
        ("job", ["ingestion"], ["ingestion"]),
        ("job", ["ingestion", "daily"], ["ingestion", "daily"]),
        ("job", [], []),
        ("pipeline_run", "daily", ["daily"]),
        ("pipeline_run", ["daily", "etl"], ["daily", "etl"]),
        ("interactive", "ops", ["ops"]),
        ("interactive", ["ops", "ui"], ["ops", "ui"]),
    ],
    ids=[
        "job-single-string",
        "job-single-element-list",
        "job-multi-list",
        "job-empty-list",
        "pipeline_run-single-string",
        "pipeline_run-multi-list",
        "interactive-single-string",
        "interactive-multi-list",
    ],
)
def test_decorator_expose_tags_normalize(
    decorator_id: str, tags_input: Any, expected: List[str]
) -> None:
    """All decorators accept `tags` as a single string or list; manifest stores list."""
    if decorator_id == "job":

        @job(expose={"tags": tags_input})
        def fn():
            pass

    elif decorator_id == "pipeline_run":

        @pipeline_run("analytics", expose={"tags": tags_input})
        def fn():
            pass

    else:

        @interactive(expose={"tags": tags_input})
        def fn():
            pass

    assert fn.to_job_definition()["expose"]["tags"] == expected


def test_expose_tags_string_drives_tag_trigger() -> None:
    """A string `tags` value still produces a single `tag:` trigger via expand_triggers."""
    from dlt._workspace.deployment.manifest import expand_triggers

    @job(expose={"tags": "nightly"})
    def tagged():
        pass

    triggers = expand_triggers(tagged.to_job_definition())
    assert "tag:nightly" in triggers
