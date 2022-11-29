import os
import asyncio
import datetime  # noqa: 251
from typing import List
import pytest

import dlt

from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext
from dlt.common.pipeline import ExtractInfo
from dlt.common.typing import TSecretValue
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.pipeline.trace import SerializableResolvedValueTrace, load_trace

from tests.utils import preserve_environ
from tests.common.configuration.utils import toml_providers
from tests.pipeline.utils import drop_dataset_from_env, patch_working_dir, drop_pipeline

def test_create_trace(toml_providers: ConfigProvidersContext) -> None:

    @dlt.source
    def inject_tomls(api_type = dlt.config.value, credentials: CredentialsConfiguration = dlt.secrets.value, secret_value: TSecretValue = "123"):

        @dlt.resource
        def data():
            yield [1, 2, 3]

        return data()

    p = dlt.pipeline(destination="dummy")

    # read from secrets and configs directly
    databricks_creds = "databricks+connector://token:<databricks_token>@<databricks_host>:443/<database_or_schema_name>?conn_timeout=15&search_path=a,b,c"
    s = dlt.secrets["databricks.credentials"]
    assert s == databricks_creds

    extract_info = p.extract(inject_tomls())
    trace = p._trace
    assert trace is not None
    assert len(trace.steps) == 1
    step = trace.steps[0]
    assert step.step == "extract"
    assert isinstance(step.started_at, datetime.datetime)
    assert isinstance(step.finished_at, datetime.datetime)
    assert step.step_info is extract_info
    # check config trace
    resolved = _find_resolved_value(trace.resolved_config_values, "api_type", [])
    assert resolved.config_type_name == "TestCreateTraceInjectTomlsConfiguration"
    assert resolved.value == "REST"
    assert resolved.is_secret_hint is False
    assert resolved.default_value is None
    assert resolved.provider_name == "config.toml"
    # dictionaries are not returned anymore
    resolved = _find_resolved_value(trace.resolved_config_values, "credentials", [])
    assert resolved is None or isinstance(resolved.value, str)
    resolved = _find_resolved_value(trace.resolved_config_values, "secret_value", [])
    assert resolved.is_secret_hint is True
    assert resolved.value == "2137"
    assert resolved.default_value == "123"
    resolved = _find_resolved_value(trace.resolved_config_values, "credentials", ["databricks"])
    assert resolved.is_secret_hint is True
    assert resolved.value == databricks_creds

    # extract with exception
    @dlt.source
    def async_exception(max_range=1):

        async def get_val(v):
            await asyncio.sleep(0.1)
            if v % 3 == 0:
                raise ValueError(v)
            return v

        @dlt.resource
        def data():
            yield from [get_val(v) for v in range(1,max_range)]

        return data()

    with pytest.raises(PipelineStepFailed):
        p.extract(async_exception())

    trace = p._trace
    assert len(trace.steps) == 2
    step = trace.steps[1]
    assert step.step == "extract"
    assert isinstance(step.step_exception, str)
    assert isinstance(step.step_info, ExtractInfo)

    # normalize
    norm_info = p.normalize()
    trace = p._trace
    assert len(trace.steps) == 3
    step = trace.steps[2]
    assert step.step == "normalize"
    assert step.step_info is norm_info

    # load
    os.environ["COMPLETED_PROB"] = "1.0"  # make it complete immediately
    load_info = p.load()
    trace = p._trace
    assert len(trace.steps) == 4
    step = trace.steps[3]
    assert step.step == "load"
    assert step.step_info is load_info
    resolved = _find_resolved_value(trace.resolved_config_values, "completed_prob", [])
    assert resolved.is_secret_hint is False
    assert resolved.value == "1.0"
    assert resolved.config_type_name == "DummyClientConfiguration"

    # run resets the trace
    load_info = inject_tomls().run()
    trace = p._trace
    # pipeline will try to normalize and load any pending packages before extracting new one
    assert len(trace.steps) == 5  # so we have 5 steps
    step = trace.steps[-1]  # the last one should be load
    assert step.step == "load"
    assert step.step_info is load_info
    assert trace.steps[0].step_info is not extract_info


def test_save_load_trace() -> None:
    os.environ["COMPLETED_PROB"] = "1.0"
    info = dlt.pipeline().run([1,2,3], table_name="data", destination="dummy")
    trace = load_trace(info.pipeline.working_dir)
    assert trace is not None
    assert len(trace.steps) == 3 == len(info.pipeline._trace.steps)
    step = trace.steps[-1]  # the last one should be load
    assert step.step == "load"
    resolved = _find_resolved_value(trace.resolved_config_values, "completed_prob", [])
    assert resolved.is_secret_hint is False
    assert resolved.value == "1.0"
    assert resolved.config_type_name == "DummyClientConfiguration"

    # exception also saves trace
    @dlt.resource
    def data():
        raise NotImplementedError()
        yield

    with pytest.raises(PipelineStepFailed) as py_ex:
        dlt.run(data(), destination="dummy")
    # there's the same pipeline in exception as in previous run
    assert py_ex.value.pipeline is info.pipeline
    trace = load_trace(py_ex.value.pipeline.working_dir)
    assert trace is not None
    assert len(trace.steps) == 3  # normalize, load, extract
    step = trace.steps[-1]
    assert step.step == "extract"
    assert step.step_exception is not None


def test_load_none_trace() -> None:
    p = dlt.pipeline()
    assert load_trace(p.working_dir) is None


def _find_resolved_value(resolved: List[SerializableResolvedValueTrace], key: str, namespaces: List[str]) -> SerializableResolvedValueTrace:
    return next((v for v in resolved if v.key == key and v.namespaces == namespaces), None)

