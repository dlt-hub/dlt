from copy import deepcopy
import io
import os
import asyncio
import datetime  # noqa: 251
from typing import Any, List
from unittest.mock import patch
import pytest
import requests_mock
import yaml

import dlt

from dlt.common import json
from dlt.common.configuration.specs import CredentialsConfiguration, RuntimeConfiguration
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContainer
from dlt.common.pipeline import ExtractInfo, NormalizeInfo, LoadInfo
from dlt.common.schema import Schema
from dlt.common.runtime.telemetry import stop_telemetry
from dlt.common.typing import DictStrAny, DictStrStr, TSecretValue
from dlt.common.utils import digest128

from dlt.destinations import dummy, filesystem

from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.pipeline.pipeline import Pipeline
from dlt.pipeline.trace import (
    PipelineTrace,
    SerializableResolvedValueTrace,
    load_trace,
)
from dlt.pipeline.track import slack_notify_load_success
from dlt.extract import DltResource, DltSource
from dlt.extract.extract import describe_extract_data
from dlt.extract.pipe import Pipe

from tests.pipeline.utils import PIPELINE_TEST_CASES_PATH
from tests.utils import TEST_STORAGE_ROOT, start_test_telemetry, temporary_telemetry


def test_create_trace(toml_providers: ConfigProvidersContainer, environment: Any) -> None:
    dlt.secrets["load.delete_completed_jobs"] = True

    @dlt.source
    def inject_tomls(
        api_type=dlt.config.value,
        credentials: CredentialsConfiguration = dlt.secrets.value,
        secret_value: TSecretValue = TSecretValue("123"),  # noqa: B008
    ):
        @dlt.resource(write_disposition="replace", primary_key="id")
        def data():
            yield [{"id": 1}, {"id": 2}, {"id": 3}]

        return data()

    p = dlt.pipeline(destination="dummy")

    # read from secrets and configs directly
    databricks_creds = "databricks+connector://token:<databricks_token>@<databricks_host>:443/<database_or_schema_name>?conn_timeout=15&search_path=a,b,c"
    s = dlt.secrets["databricks.credentials"]
    assert s == databricks_creds

    extract_info = p.extract(inject_tomls())
    trace = p.last_trace
    assert trace is not None
    # assert p._trace is None
    assert len(trace.steps) == 1
    step = trace.steps[0]
    assert step.step == "extract"
    assert isinstance(step.started_at, datetime.datetime)
    assert isinstance(step.finished_at, datetime.datetime)
    assert isinstance(step.step_info, ExtractInfo)
    assert step.step_info.extract_data_info == [{"name": "inject_tomls", "data_type": "source"}]
    # check infos
    extract_info = p.last_trace.last_extract_info
    assert isinstance(extract_info, ExtractInfo)
    # should have single job and single load id
    assert len(extract_info.loads_ids) == 1
    load_id = extract_info.loads_ids[0]
    assert len(extract_info.metrics) == 1

    # extract of data in the first one
    metrics = extract_info.metrics[load_id][0]
    # inject tomls and dlt state
    assert len(metrics["job_metrics"]) == 1
    assert "data" in metrics["table_metrics"]
    assert set(metrics["resource_metrics"].keys()) == {"data"}
    assert metrics["schema_name"] == "inject_tomls"
    # check dag and hints
    assert metrics["dag"] == [("data", "data")]
    assert metrics["hints"]["data"] == {"write_disposition": "replace", "primary_key": "id"}

    metrics = extract_info.metrics[load_id][1]
    # inject tomls and dlt state
    assert len(metrics["job_metrics"]) == 1
    assert "_dlt_pipeline_state" in metrics["table_metrics"]
    assert set(metrics["resource_metrics"].keys()) == {"_dlt_pipeline_state"}
    assert metrics["schema_name"] == "inject_tomls"
    # check dag and hints
    assert metrics["dag"] == [("_dlt_pipeline_state", "_dlt_pipeline_state")]
    # state has explicit columns set
    assert metrics["hints"]["_dlt_pipeline_state"]["original_columns"] == "dict"

    # check packages
    assert len(extract_info.load_packages) == 1
    # two jobs
    print(extract_info.load_packages[0])
    assert len(extract_info.load_packages[0].jobs["new_jobs"]) == 2
    assert extract_info.load_packages[0].state == "extracted"

    # check config trace
    resolved = _find_resolved_value(trace.resolved_config_values, "api_type", [])
    assert resolved.config_type_name == "TestCreateTraceInjectTomlsConfiguration"
    assert resolved.value == "REST"
    assert resolved.is_secret_hint is False
    assert resolved.default_value is None
    assert resolved.provider_name == "config.toml"
    # dictionaries are not returned anymore, secrets are masked
    resolved = _find_resolved_value(trace.resolved_config_values, "credentials", [])
    assert resolved is None or isinstance(resolved.value, str)
    resolved = _find_resolved_value(trace.resolved_config_values, "secret_value", [])
    assert resolved.is_secret_hint is True
    assert resolved.value is None, "Credential is not masked"
    assert resolved.default_value is None, "Credential is not masked"
    resolved = _find_resolved_value(trace.resolved_config_values, "credentials", ["databricks"])
    assert resolved.is_secret_hint is True
    assert resolved.value is None, "Credential is not masked"
    assert_trace_serializable(trace)

    # activate pipeline because other was running in assert trace
    p.activate()

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
            yield from [get_val(v) for v in range(1, max_range)]

        return data()

    with pytest.raises(PipelineStepFailed):
        p.extract(async_exception())

    trace = p.last_trace
    assert p._trace is None
    assert len(trace.steps) == 2
    step = trace.steps[1]
    assert step.step == "extract"
    assert isinstance(step.step_exception, str)
    assert isinstance(step.step_info, ExtractInfo)
    assert len(step.exception_traces) > 0
    assert step.step_info.extract_data_info == [{"name": "async_exception", "data_type": "source"}]
    assert_trace_serializable(trace)

    extract_info = step.step_info
    # only new (unprocessed) package is present, all other metrics are empty, state won't be extracted
    assert len(extract_info.loads_ids) == 1
    load_id = extract_info.loads_ids[0]
    package = extract_info.load_packages[0]
    assert package.state == "new"
    # no jobs - exceptions happened before save
    assert len(package.jobs["new_jobs"]) == 0
    # metrics should be collected
    assert len(extract_info.metrics[load_id]) == 1

    # normalize
    norm_info = p.normalize()
    trace = p.last_trace
    assert p._trace is None
    assert len(trace.steps) == 3
    step = trace.steps[2]
    assert step.step == "normalize"
    assert step.step_info is norm_info
    assert_trace_serializable(trace)
    assert isinstance(p.last_trace.last_normalize_info, NormalizeInfo)
    assert p.last_trace.last_normalize_info.row_counts == {"_dlt_pipeline_state": 1, "data": 3}

    assert len(norm_info.loads_ids) == 1
    load_id = norm_info.loads_ids[0]
    assert len(norm_info.metrics) == 1

    # just one load package with single metrics
    assert len(norm_info.metrics[load_id]) == 1
    norm_metrics = norm_info.metrics[load_id][0]
    # inject tomls and dlt state
    assert len(norm_metrics["job_metrics"]) == 2
    assert "data" in norm_metrics["table_metrics"]

    # check packages
    assert len(norm_info.load_packages) == 1
    # two jobs
    assert len(norm_info.load_packages[0].jobs["new_jobs"]) == 2
    assert norm_info.load_packages[0].state == "normalized"

    # load
    os.environ["COMPLETED_PROB"] = "1.0"  # make it complete immediately
    load_info = p.load()
    trace = p.last_trace
    assert p._trace is None
    assert len(trace.steps) == 4
    step = trace.steps[3]
    assert step.step == "load"
    assert step.step_info is load_info
    load_info = step.step_info  # type: ignore[assignment]

    # check packages
    assert len(load_info.load_packages) == 1
    # two jobs
    assert load_info.load_packages[0].state == "loaded"
    assert len(load_info.load_packages[0].jobs["completed_jobs"]) == 2

    resolved = _find_resolved_value(trace.resolved_config_values, "completed_prob", [])
    assert resolved.is_secret_hint is False
    assert resolved.value == "1.0"
    assert resolved.config_type_name == "DummyClientConfiguration"
    assert_trace_serializable(trace)
    assert isinstance(p.last_trace.last_load_info, LoadInfo)
    p.activate()

    # run resets the trace
    load_info = inject_tomls().run()
    trace = p.last_trace
    assert p._trace is None
    assert len(trace.steps) == 4  # extract, normalize, load, run
    step = trace.steps[-1]  # the last one should be run
    assert step.step == "run"
    assert step.step_info is load_info
    assert trace.steps[0].step_info is not extract_info

    step = trace.steps[-2]  # the previous one should be load
    assert step.step == "load"
    assert step.step_info is load_info  # same load info
    assert trace.steps[0].step_info is not extract_info
    assert_trace_serializable(trace)
    assert isinstance(p.last_trace.last_load_info, LoadInfo)
    assert isinstance(p.last_trace.last_normalize_info, NormalizeInfo)
    assert isinstance(p.last_trace.last_extract_info, ExtractInfo)


def test_trace_schema() -> None:
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "True"
    os.environ["RESTORE_FROM_DESTINATION"] = "False"

    # mock runtime env
    os.environ["CIRCLECI"] = "1"
    os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "lambda"

    @dlt.source(section="many_hints")
    def many_hints(
        api_type=dlt.config.value,
        credentials: str = dlt.secrets.value,
        secret_value: TSecretValue = TSecretValue("123"),  # noqa: B008
    ):
        # TODO: create table / column schema from typed dicts, not explicitly
        @dlt.resource(
            write_disposition="replace",
            primary_key="id",
            table_format="delta",
            file_format="jsonl",
            schema_contract="evolve",
            columns=[
                {
                    "name": "multi",
                    "data_type": "decimal",
                    "nullable": True,
                    "cluster": True,
                    "description": "unknown",
                    "merge_key": True,
                    "precision": 9,
                    "scale": 3,
                    "sort": True,
                    "variant": True,
                    "partition": True,
                }
            ],
        )
        def data():
            yield [{"id": 1, "multi": "1.2"}, {"id": 2}, {"id": 3}]

        return data()

    @dlt.source
    def github():
        @dlt.resource
        def get_shuffled_events():
            for _ in range(1):
                with open(
                    "tests/normalize/cases/github.events.load_page_1_duck.json",
                    "r",
                    encoding="utf-8",
                ) as f:
                    issues = json.load(f)
                    yield issues

        return get_shuffled_events()

    @dlt.source
    def async_exception(max_range=1):
        async def get_val(v):
            await asyncio.sleep(0.1)
            if v % 3 == 0:
                raise ValueError(v)
            return v

        @dlt.resource
        def data():
            yield from [get_val(v) for v in range(1, max_range)]

        return data()

    # create pipeline with staging to get remote_url in load step job_metrics
    dummy_dest = dummy(completed_prob=1.0)
    pipeline = dlt.pipeline(
        pipeline_name="test_trace_schema",
        destination=dummy_dest,
        staging=filesystem(os.path.abspath(os.path.join(TEST_STORAGE_ROOT, "_remote_filesystem"))),
        dataset_name="various",
    )

    # mock config
    os.environ["API_TYPE"] = "REST"
    os.environ["SOURCES__MANY_HINTS__CREDENTIALS"] = "CREDS"

    pipeline.run([many_hints(), github()])

    trace = pipeline.last_trace
    pipeline._schema_storage.storage.save("trace.json", json.dumps(trace, pretty=True))

    schema = dlt.Schema("trace")
    trace_pipeline = dlt.pipeline(
        pipeline_name="test_trace_schema_traces", destination=dummy(completed_prob=1.0)
    )
    trace_pipeline.run([trace], table_name="trace", schema=schema)

    # add exception trace
    with pytest.raises(PipelineStepFailed):
        pipeline.extract(async_exception(max_range=4))

    trace_exception = pipeline.last_trace
    pipeline._schema_storage.storage.save(
        "trace_exception.json", json.dumps(trace_exception, pretty=True)
    )

    trace_pipeline.run([trace_exception], table_name="trace")
    inferred_trace_contract = trace_pipeline.schemas["trace"]
    inferred_contract_str = inferred_trace_contract.to_pretty_yaml(remove_processing_hints=True)

    # NOTE: this saves actual inferred contract (schema) to schema storage, move it to test cases if you update
    # trace shapes
    # TODO: create a proper schema for dlt trace and tables/columns
    pipeline._schema_storage.storage.save("trace.schema.yaml", inferred_contract_str)
    # print(pipeline._schema_storage.storage.storage_path)

    # load the schema and use it as contract
    with open(f"{PIPELINE_TEST_CASES_PATH}/contracts/trace.schema.yaml", encoding="utf-8") as f:
        imported_schema = yaml.safe_load(f)
    trace_contract = Schema.from_dict(imported_schema, remove_processing_hints=True)
    # compare pretty forms of the schemas, they must be identical
    # NOTE: if this fails you can comment this out and use contract run below to find first offending difference
    # assert trace_contract.to_pretty_yaml() == inferred_contract_str

    # use trace contract to load data again
    contract_trace_pipeline = dlt.pipeline(
        pipeline_name="test_trace_schema_traces_contract", destination=dummy(completed_prob=1.0)
    )
    contract_trace_pipeline.run(
        [trace_exception, trace],
        table_name="trace",
        schema=trace_contract,
        schema_contract="freeze",
    )

    # assert inferred_trace_contract.version_hash == trace_contract.version_hash

    # print(trace_pipeline.schemas["trace"].to_pretty_yaml())
    # print(pipeline._schema_storage.storage.storage_path)


# def test_trace_schema_contract() -> None:


def test_save_load_trace() -> None:
    os.environ["COMPLETED_PROB"] = "1.0"
    info = dlt.pipeline().run([1, 2, 3], table_name="data", destination="dummy")
    pipeline = dlt.pipeline()
    # will get trace from working dir
    trace = pipeline.last_trace
    assert trace is not None
    assert pipeline._trace is None
    assert len(trace.steps) == 4 == len(info.pipeline.last_trace.steps)  # type: ignore[attr-defined]
    step = trace.steps[-2]  # the previous to last one should be load
    assert step.step == "load"
    resolved = _find_resolved_value(trace.resolved_config_values, "completed_prob", [])
    assert resolved.is_secret_hint is False
    assert resolved.value == "1.0"
    assert resolved.config_type_name == "DummyClientConfiguration"
    assert_trace_serializable(trace)
    # check row counts
    assert pipeline.last_trace.last_normalize_info.row_counts == {
        "_dlt_pipeline_state": 1,
        "data": 3,
    }
    # reactivate the pipeline
    pipeline.activate()

    # load trace and check if all elements are present
    loaded_trace = load_trace(pipeline.working_dir)
    print(loaded_trace.asstr(2))
    assert len(trace.steps) == 4
    loaded_trace_dict = deepcopy(loaded_trace.asdict())
    trace_dict = deepcopy(trace.asdict())
    assert loaded_trace_dict == trace_dict
    # do it again to check if we are not popping
    assert loaded_trace_dict == loaded_trace.asdict()
    assert trace_dict == trace.asdict()

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
    assert pipeline._trace is None
    assert len(trace.steps) == 2  # extract with exception, also has run with exception
    step = trace.steps[-2]
    assert step.step == "extract"
    assert step.step_exception is not None
    run_step = trace.steps[-1]
    assert run_step.step == "run"
    assert run_step.step_exception is not None
    assert step.step_exception == run_step.step_exception
    assert_trace_serializable(trace)
    assert pipeline.last_trace.last_normalize_info is None


def test_save_load_empty_trace() -> None:
    os.environ["COMPLETED_PROB"] = "1.0"
    os.environ["RESTORE_FROM_DESTINATION"] = "false"
    pipeline = dlt.pipeline()
    pipeline.run([], table_name="data", destination="dummy")
    trace = pipeline.last_trace
    assert_trace_serializable(trace)
    assert len(trace.steps) == 4

    pipeline.activate()

    # load trace and check if all elements are present
    loaded_trace = load_trace(pipeline.working_dir)
    print(loaded_trace.asstr(2))
    assert len(trace.steps) == 4
    loaded_trace_dict = deepcopy(loaded_trace.asdict())
    trace_dict = deepcopy(trace.asdict())
    assert loaded_trace_dict == trace_dict
    # do it again to check if we are not popping
    assert loaded_trace_dict == loaded_trace.asdict()
    assert trace_dict == trace.asdict()


def test_disable_trace(environment: DictStrStr) -> None:
    environment["ENABLE_RUNTIME_TRACE"] = "false"
    environment["COMPLETED_PROB"] = "1.0"
    dlt.pipeline().run([1, 2, 3], table_name="data", destination="dummy")
    assert dlt.pipeline().last_trace is None


def test_trace_on_restore_state(environment: DictStrStr) -> None:
    environment["COMPLETED_PROB"] = "1.0"

    def _sync_destination_patch(
        self: Pipeline, destination: str = None, staging: str = None, dataset_name: str = None
    ):
        # just wipe the pipeline simulating deleted dataset
        self._wipe_working_folder()
        self._configure(
            self._schema_storage_config.export_schema_path,
            self._schema_storage_config.import_schema_path,
            False,
        )

    with patch.object(Pipeline, "sync_destination", _sync_destination_patch):
        dlt.pipeline().run([1, 2, 3], table_name="data", destination="dummy")
        assert len(dlt.pipeline().last_trace.steps) == 4
        assert dlt.pipeline().last_trace.last_normalize_info.row_counts == {
            "_dlt_pipeline_state": 1,
            "data": 3,
        }


def test_load_none_trace() -> None:
    p = dlt.pipeline()
    assert load_trace(p.working_dir) is None


def test_trace_telemetry(temporary_telemetry: RuntimeConfiguration) -> None:
    with patch("dlt.common.runtime.sentry.before_send", _mock_sentry_before_send), patch(
        "dlt.common.runtime.anon_tracker.before_send", _mock_anon_tracker_before_send
    ):
        ANON_TRACKER_SENT_ITEMS.clear()
        SENTRY_SENT_ITEMS.clear()
        # make dummy fail all files
        os.environ["FAIL_PROB"] = "1.0"
        # but do not raise exceptions
        os.environ["RAISE_ON_FAILED_JOBS"] = "false"
        load_info = dlt.pipeline().run(
            [1, 2, 3], table_name="data", destination="dummy", dataset_name="data_data"
        )
        # we should have 4 tracker items
        assert len(ANON_TRACKER_SENT_ITEMS) == 4
        expected_steps = ["extract", "normalize", "load", "run"]
        for event, step in zip(ANON_TRACKER_SENT_ITEMS, expected_steps):
            assert event["event"] == f"pipeline_{step}"
            assert event["properties"]["success"] is True
            assert event["properties"]["destination_name"] == "dummy"
            assert event["properties"]["destination_type"] == "dlt.destinations.dummy"
            assert event["properties"]["pipeline_name_hash"] == digest128(
                load_info.pipeline.pipeline_name
            )
            assert event["properties"]["dataset_name_hash"] == digest128(
                load_info.pipeline.dataset_name
            )
            assert event["properties"]["default_schema_name_hash"] == digest128(
                load_info.pipeline.default_schema_name
            )
            assert isinstance(event["properties"]["elapsed"], float)
            assert isinstance(event["properties"]["transaction_id"], str)
            # check extract info
            if step == "extract":
                assert event["properties"]["extract_data"] == [{"name": "", "data_type": "int"}]
            if step == "load":
                # dummy has empty fingerprint
                assert event["properties"]["destination_fingerprint"] == ""
        #
        # we have two failed files (state and data) that should be logged by sentry
        # print(SENTRY_SENT_ITEMS)
        # for item in SENTRY_SENT_ITEMS:
        #     # print(item)
        #     print(item["logentry"]["message"])
        # assert len(SENTRY_SENT_ITEMS) == 4

        # trace with exception
        @dlt.resource
        def data():
            raise NotImplementedError()
            yield

        ANON_TRACKER_SENT_ITEMS.clear()
        SENTRY_SENT_ITEMS.clear()
        with pytest.raises(PipelineStepFailed):
            dlt.pipeline().run(data, destination="dummy")
        assert len(ANON_TRACKER_SENT_ITEMS) == 2
        event = ANON_TRACKER_SENT_ITEMS[0]
        assert event["event"] == "pipeline_extract"
        assert event["properties"]["success"] is False
        assert event["properties"]["destination_name"] == "dummy"
        assert event["properties"]["destination_type"] == "dlt.destinations.dummy"
        assert isinstance(event["properties"]["elapsed"], float)
        # check extract info
        if step == "extract":
            assert event["properties"]["extract_data"] == [
                {"name": "data", "data_type": "resource"}
            ]
        # we didn't log any errors
        assert len(SENTRY_SENT_ITEMS) == 0

        # trace without destination and dataset
        p = dlt.pipeline(pipeline_name="fresh").drop()
        ANON_TRACKER_SENT_ITEMS.clear()
        SENTRY_SENT_ITEMS.clear()
        p.extract([1, 2, 3], table_name="data")
        event = ANON_TRACKER_SENT_ITEMS[0]
        assert event["event"] == "pipeline_extract"
        assert event["properties"]["success"] is True
        assert event["properties"]["destination_name"] is None
        assert event["properties"]["destination_type"] is None
        assert event["properties"]["pipeline_name_hash"] == digest128("fresh")
        assert event["properties"]["dataset_name_hash"] == digest128(p.dataset_name)
        assert event["properties"]["default_schema_name_hash"] == digest128(p.default_schema_name)


def test_extract_data_describe() -> None:
    schema = Schema("test")
    assert describe_extract_data(DltSource(schema, "sect")) == [
        {"name": "test", "data_type": "source"}
    ]
    assert describe_extract_data(DltResource(Pipe("rrr_extract"), None, False)) == [
        {"name": "rrr_extract", "data_type": "resource"}
    ]
    assert describe_extract_data([DltSource(schema, "sect")]) == [
        {"name": "test", "data_type": "source"}
    ]
    assert describe_extract_data([DltResource(Pipe("rrr_extract"), None, False)]) == [
        {"name": "rrr_extract", "data_type": "resource"}
    ]
    assert describe_extract_data(
        [DltResource(Pipe("rrr_extract"), None, False), DltSource(schema, "sect")]
    ) == [{"name": "rrr_extract", "data_type": "resource"}, {"name": "test", "data_type": "source"}]
    assert describe_extract_data([{"a": "b"}]) == [{"name": "", "data_type": "dict"}]
    from pandas import DataFrame

    # we assume that List content has same type
    assert describe_extract_data([DataFrame(), {"a": "b"}]) == [
        {"name": "", "data_type": "DataFrame"}
    ]
    # first unnamed element in the list breaks checking info
    assert describe_extract_data(
        [DltResource(Pipe("rrr_extract"), None, False), DataFrame(), DltSource(schema, "sect")]
    ) == [{"name": "rrr_extract", "data_type": "resource"}, {"name": "", "data_type": "DataFrame"}]


def test_slack_hook(environment: DictStrStr) -> None:
    stop_telemetry()
    hook_url = "https://hooks.slack.com/services/T04DHMAF13Q/B04E7B1MQ1H/TDHEI123WUEE"
    environment["COMPLETED_PROB"] = "1.0"
    environment["GITHUB_USER"] = "rudolfix"
    environment["RUNTIME__DLTHUB_TELEMETRY"] = "False"
    environment["RUNTIME__SLACK_INCOMING_HOOK"] = hook_url
    with requests_mock.mock() as m:
        m.post(hook_url, json={})
        load_info = dlt.pipeline().run([1, 2, 3], table_name="data", destination="dummy")
        assert slack_notify_load_success(load_info.pipeline.runtime_config.slack_incoming_hook, load_info, load_info.pipeline.last_trace) == 200  # type: ignore[attr-defined]
    assert m.called
    message = m.last_request.json()
    assert "rudolfix" in message["text"]
    assert "dummy" in message["text"]


def test_broken_slack_hook(environment: DictStrStr) -> None:
    environment["COMPLETED_PROB"] = "1.0"
    environment["RUNTIME__SLACK_INCOMING_HOOK"] = "http://localhost:22"
    load_info = dlt.pipeline().run([1, 2, 3], table_name="data", destination="dummy")
    # connection error
    assert slack_notify_load_success(load_info.pipeline.runtime_config.slack_incoming_hook, load_info, load_info.pipeline.last_trace) == -1  # type: ignore[attr-defined]
    # pipeline = dlt.pipeline()
    # assert pipeline.last_trace is not None
    # assert pipeline._trace is None
    # trace = load_trace(info.pipeline.working_dir)
    # assert len(trace.steps) == 4
    # run_step = trace.steps[-1]
    # assert run_step.step == "run"
    # assert run_step.step_exception is None


def _find_resolved_value(
    resolved: List[SerializableResolvedValueTrace], key: str, sections: List[str]
) -> SerializableResolvedValueTrace:
    return next((v for v in resolved if v.key == key and v.sections == sections), None)


ANON_TRACKER_SENT_ITEMS = []


def _mock_anon_tracker_before_send(event: DictStrAny) -> DictStrAny:
    ANON_TRACKER_SENT_ITEMS.append(event)
    return event


SENTRY_SENT_ITEMS = []


def _mock_sentry_before_send(event: DictStrAny, _unused_hint: Any = None) -> DictStrAny:
    SENTRY_SENT_ITEMS.append(event)
    return event


def assert_trace_serializable(trace: PipelineTrace) -> None:
    str(trace)
    trace.asstr(0)
    trace.asstr(1)
    trace_dict = deepcopy(trace.asdict())
    # check if we do not pop
    assert trace_dict == trace.asdict()
    with io.BytesIO() as b:
        json.typed_dump(trace, b, pretty=True)
        b.getvalue()
    json.dumps(trace)

    # load trace to duckdb
    from dlt.destinations import duckdb

    trace_pipeline = dlt.pipeline("trace", destination=duckdb(":pipeline:")).drop()
    trace_pipeline.run([trace], table_name="trace_data")

    # print(trace_pipeline.default_schema.to_pretty_yaml())
