import itertools
import logging
import os
import random
from typing import Any, Optional, Iterator, Dict, Any, cast
from tenacity import retry_if_exception, Retrying, stop_after_attempt
from pydantic import BaseModel

import pytest

import dlt
from dlt.common import json, sleep
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.aws_credentials import AwsCredentials
from dlt.common.configuration.specs.exceptions import NativeValueError
from dlt.common.configuration.specs.gcp_credentials import GcpOAuthCredentials
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.capabilities import TLoaderFileFormat
from dlt.common.exceptions import DestinationHasFailedJobs, DestinationTerminalException, PipelineStateNotAvailable, UnknownDestinationModule
from dlt.common.pipeline import PipelineContext
from dlt.common.runtime.collector import AliveCollector, EnlightenCollector, LogCollector, TqdmCollector
from dlt.common.schema.utils import new_column
from dlt.common.utils import uniq_id

from dlt.extract.exceptions import InvalidResourceDataTypeBasic, PipeGenInvalid, SourceExhausted
from dlt.extract.extract import ExtractorStorage
from dlt.extract.source import DltResource, DltSource
from dlt.load.exceptions import LoadClientJobFailed
from dlt.pipeline.exceptions import InvalidPipelineName, PipelineNotActive, PipelineStepFailed
from dlt.pipeline.helpers import retry_load
from dlt.pipeline import TCollectorArg

from tests.common.utils import TEST_SENTRY_DSN
from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration
from tests.utils import TEST_STORAGE_ROOT
from tests.common.configuration.utils import environment
from tests.extract.utils import expect_extracted_file
from tests.pipeline.utils import assert_load_info, airtable_emojis


def test_default_pipeline() -> None:
    p = dlt.pipeline()
    # this is a name of executing test harness or blank pipeline on windows
    possible_names = ["dlt_pytest", "dlt_pipeline"]
    possible_dataset_names = ["dlt_pytest_dataset", "dlt_pipeline_dataset"]
    assert p.pipeline_name in possible_names
    assert p.pipelines_dir == os.path.abspath(os.path.join(TEST_STORAGE_ROOT, ".dlt", "pipelines"))
    assert p.runtime_config.pipeline_name == p.pipeline_name
    # dataset that will be used to load data is the pipeline name
    assert p.dataset_name in possible_dataset_names
    assert p.destination is None
    assert p.default_schema_name is None

    # this is the same pipeline
    p2 = dlt.pipeline()
    assert p is p2

    # this will create default schema
    p.extract(["a", "b", "c"], table_name="data")
    # `_pipeline` is removed from default schema name
    assert p.default_schema_name in ["dlt_pytest", "dlt"]


def test_default_pipeline_dataset() -> None:
    # dummy does not need a dataset
    p = dlt.pipeline(destination="dummy")
    assert p.dataset_name is None  # so it is none

    # filesystem needs one
    possible_dataset_names = ["dlt_pytest_dataset", "dlt_pipeline_dataset"]
    p = dlt.pipeline(destination="filesystem")
    assert p.dataset_name in possible_dataset_names


def test_run_full_refresh_default_dataset() -> None:
    p = dlt.pipeline(full_refresh=True, destination="filesystem")
    assert p.dataset_name.endswith(p._pipeline_instance_id)
    # restore this pipeline
    r_p = dlt.attach(full_refresh=False)
    assert r_p.dataset_name.endswith(p._pipeline_instance_id)

    # dummy does not need dataset
    p = dlt.pipeline(full_refresh=True, destination="dummy")
    assert p.dataset_name is None
    # simulate set new dataset
    p._set_destinations("filesystem", None)
    assert p.dataset_name is None
    p._set_dataset_name(None)
    # full refresh is still observed
    assert p.dataset_name and p.dataset_name.endswith(p._pipeline_instance_id)


def test_run_full_refresh_underscored_dataset() -> None:
    p = dlt.pipeline(full_refresh=True, dataset_name="_main_")
    assert p.dataset_name.endswith(p._pipeline_instance_id)
    # restore this pipeline
    r_p = dlt.attach(full_refresh=False)
    assert r_p.dataset_name.endswith(p._pipeline_instance_id)


def test_pipeline_with_non_alpha_name() -> None:
    name = "another pipeline %__8329イロハニホヘト"
    # contains %
    with pytest.raises(InvalidPipelineName):
        p = dlt.pipeline(pipeline_name=name)

    name = "another pipeline __8329イロハニホヘト"
    p = dlt.pipeline(pipeline_name=name)
    assert p.pipeline_name == name
    # default dataset is set
    assert p.dataset_name == f"{name}_dataset"
    # also pipeline name in runtime must be correct
    assert p.runtime_config.pipeline_name == p.pipeline_name

    # this will create default schema
    p.extract(["a", "b", "c"], table_name="data")
    assert p.default_schema_name == "another_pipeline_8329x"


def test_invalid_dataset_name() -> None:
    # this is invalid dataset name but it will be normalized within a destination
    p = dlt.pipeline(dataset_name="!")
    assert p.dataset_name == "!"


def test_pipeline_context_deferred_activation() -> None:
    ctx = Container()[PipelineContext]
    assert ctx.is_active() is False
    # this creates default pipeline
    p = ctx.pipeline()
    # and we can get it here
    assert p is dlt.pipeline()


def test_pipeline_context() -> None:
    ctx = Container()[PipelineContext]
    assert ctx.is_active() is False
    # create pipeline
    p = dlt.pipeline()
    assert ctx.is_active() is True
    assert ctx.pipeline() is p
    assert p.is_active is True
    # has no destination context
    assert DestinationCapabilitiesContext not in Container()

    # create another pipeline
    p2 = dlt.pipeline(pipeline_name="another pipeline", destination="duckdb")
    assert ctx.pipeline() is p2
    assert p.is_active is False
    assert p2.is_active is True

    p3 = dlt.pipeline(pipeline_name="more pipelines", destination="dummy")
    assert ctx.pipeline() is p3
    assert p3.is_active is True
    assert p2.is_active is False
    assert Container()[DestinationCapabilitiesContext].naming_convention == "snake_case"

    # restore previous
    p2 = dlt.attach("another pipeline")
    assert ctx.pipeline() is p2
    assert p3.is_active is False
    assert p2.is_active is True


def test_import_unknown_destination() -> None:
    with pytest.raises(UnknownDestinationModule):
        dlt.pipeline(destination="!")


def test_configured_destination(environment) -> None:
    environment["DESTINATION_NAME"] = "postgres"
    environment["PIPELINE_NAME"] = "postgres_pipe"

    p = dlt.pipeline()
    assert p.destination is not None
    assert p.destination.__name__.endswith("postgres")
    assert p.pipeline_name == "postgres_pipe"


def test_deterministic_salt(environment) -> None:
    environment["PIPELINE_NAME"] = "postgres_pipe"
    p = dlt.pipeline()
    p2 = dlt.attach()
    assert p.pipeline_name == p2.pipeline_name == "postgres_pipe"
    assert p.pipeline_salt == p2.pipeline_salt

    p3 = dlt.pipeline(pipeline_name="postgres_redshift")
    assert p.pipeline_salt != p3.pipeline_salt


@pytest.mark.parametrize("destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name)
def test_create_pipeline_all_destinations(destination_config: DestinationTestConfiguration) -> None:
    # create pipelines, extract and normalize. that should be possible without installing any dependencies
    p = dlt.pipeline(pipeline_name=destination_config.destination + "_pipeline", destination=destination_config.destination, staging=destination_config.staging)
    # are capabilities injected
    caps = p._container[DestinationCapabilitiesContext]
    print(caps.naming_convention)
    # are right naming conventions created
    assert p._default_naming.max_length == min(caps.max_column_identifier_length, caps.max_identifier_length)
    p.extract([1, "2", 3], table_name="data")
    # is default schema with right naming convention
    assert p.default_schema.naming.max_length == min(caps.max_column_identifier_length, caps.max_identifier_length)
    p.normalize()
    assert p.default_schema.naming.max_length == min(caps.max_column_identifier_length, caps.max_identifier_length)


def test_destination_explicit_credentials(environment: Any) -> None:
    # test redshift
    p = dlt.pipeline(pipeline_name="postgres_pipeline", destination="redshift", credentials="redshift://loader:loader@localhost:5432/dlt_data")
    config = p._get_destination_client_initial_config()
    assert config.credentials.is_resolved()
    # with staging
    p = dlt.pipeline(pipeline_name="postgres_pipeline", staging="filesystem", destination="redshift", credentials="redshift://loader:loader@localhost:5432/dlt_data")
    config = p._get_destination_client_initial_config(p.destination)
    assert config.credentials.is_resolved()
    config = p._get_destination_client_initial_config(p.staging, as_staging=True)
    assert config.credentials is None
    p._wipe_working_folder()
    # try filesystem which uses union of credentials that requires bucket_url to resolve
    p = dlt.pipeline(pipeline_name="postgres_pipeline", destination="filesystem", credentials={"aws_access_key_id": "key_id", "aws_secret_access_key": "key"})
    config = p._get_destination_client_initial_config(p.destination)
    assert isinstance(config.credentials, AwsCredentials)
    assert config.credentials.is_resolved()
    # resolve gcp oauth
    p = dlt.pipeline(pipeline_name="postgres_pipeline", destination="filesystem", credentials={"project_id": "pxid", "refresh_token": "123token", "client_id": "cid", "client_secret": "s"})
    config = p._get_destination_client_initial_config(p.destination)
    assert isinstance(config.credentials, GcpOAuthCredentials)
    assert config.credentials.is_resolved()


@pytest.mark.skip(reason="does not work on CI. probably takes right credentials from somewhere....")
def test_destination_explicit_invalid_credentials_filesystem(environment: Any) -> None:
    # if string cannot be parsed
    p = dlt.pipeline(pipeline_name="postgres_pipeline", destination="filesystem", credentials="PR8BLEM")
    with pytest.raises(NativeValueError):
        p._get_destination_client_initial_config(p.destination)


def test_extract_source_twice() -> None:

    def some_data():
        yield [1, 2, 3]
        yield [1, 2, 3]

    s = DltSource("source", "module", dlt.Schema("source"), [dlt.resource(some_data())])
    dlt.pipeline().extract(s)
    with pytest.raises(PipelineStepFailed) as py_ex:
        dlt.pipeline().extract(s)
    assert type(py_ex.value.exception) == SourceExhausted
    assert py_ex.value.exception.source_name == "source"


def test_disable_enable_state_sync(environment: Any) -> None:
    environment["RESTORE_FROM_DESTINATION"] = "False"
    p = dlt.pipeline(destination="redshift")

    def some_data():
        yield [1, 2, 3]

    s = DltSource("default", "module", dlt.Schema("default"), [dlt.resource(some_data())])
    dlt.pipeline().extract(s)
    storage = ExtractorStorage(p._normalize_storage_config)
    assert len(storage.list_files_to_normalize_sorted()) == 1
    expect_extracted_file(storage, "default", "some_data", json.dumps([1, 2, 3]))
    with pytest.raises(FileNotFoundError):
        expect_extracted_file(storage, "default", s.schema.state_table_name, "")

    p.config.restore_from_destination = True
    # extract to different schema, state must go to default schema
    s = DltSource("default_2", "module", dlt.Schema("default_2"), [dlt.resource(some_data())])
    dlt.pipeline().extract(s)
    expect_extracted_file(storage, "default", s.schema.state_table_name, "***")


def test_extract_multiple_sources() -> None:
    s1 = DltSource("default", "module", dlt.Schema("default"), [dlt.resource([1, 2, 3], name="resource_1"), dlt.resource([3, 4, 5], name="resource_2")])
    s2 = DltSource("default_2", "module", dlt.Schema("default_2"), [dlt.resource([6, 7, 8], name="resource_3"), dlt.resource([9, 10, 0], name="resource_4")])

    p = dlt.pipeline(destination="dummy")
    p.config.restore_from_destination = False
    p.extract([s1, s2])
    storage = ExtractorStorage(p._normalize_storage_config)
    expect_extracted_file(storage, "default", "resource_1", json.dumps([1, 2, 3]))
    expect_extracted_file(storage, "default", "resource_2", json.dumps([3, 4, 5]))
    expect_extracted_file(storage, "default_2", "resource_3", json.dumps([6, 7, 8]))
    expect_extracted_file(storage, "default_2", "resource_4", json.dumps([9, 10, 0]))
    assert len(storage.list_files_to_normalize_sorted()) == 4
    p.normalize()

    # make the last resource fail

    @dlt.resource
    def i_fail():
        raise NotImplementedError()

    s3 = DltSource("default_3", "module", dlt.Schema("default_3"), [dlt.resource([1, 2, 3], name="resource_1"), dlt.resource([3, 4, 5], name="resource_2")])
    s4 = DltSource("default_4", "module", dlt.Schema("default_4"), [dlt.resource([6, 7, 8], name="resource_3"), i_fail])

    with pytest.raises(PipelineStepFailed):
       p.extract([s3, s4])

    # nothing to normalize
    assert len(storage.list_files_to_normalize_sorted()) == 0
    # pipeline state is successfully rollbacked after the last extract and default_3 and 4 schemas are not present
    assert set(p.schema_names) == {"default", "default_2"}
    assert set(p._schema_storage.list_schemas()) == {"default", "default_2"}


def test_restore_state_on_dummy() -> None:
    os.environ["COMPLETED_PROB"] = "1.0"  # make it complete immediately

    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")
    p.config.restore_from_destination = True
    info = p.run([1, 2, 3], table_name="dummy_table")
    print(info)
    assert p.first_run is False
    # no effect
    p.sync_destination()
    assert p.state["_state_version"] == 2

    # wipe out storage
    p._wipe_working_folder()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")
    assert p.first_run is True
    p.sync_destination()
    assert p.first_run is True
    assert p.state["_state_version"] == 1


def test_first_run_flag() -> None:
    os.environ["COMPLETED_PROB"] = "1.0"  # make it complete immediately

    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")
    assert p.first_run is True
    # attach
    p = dlt.attach(pipeline_name=pipeline_name)
    assert p.first_run is True
    p.extract([1, 2, 3], table_name="dummy_table")
    assert p.first_run is True
    # attach again
    p = dlt.attach(pipeline_name=pipeline_name)
    assert p.first_run is True
    assert len(p.list_extracted_resources()) > 0
    p.normalize()
    assert len(p.list_normalized_load_packages()) > 0
    assert p.first_run is True
    # load will change the flag
    p.load()
    assert p.first_run is False
    # attach again
    p = dlt.attach(pipeline_name=pipeline_name)
    assert p.first_run is False
    # wipe the pipeline
    p._create_pipeline()
    assert p.first_run is True
    p._save_state(p._get_state())
    p = dlt.attach(pipeline_name=pipeline_name)
    assert p.first_run is True


def test_has_pending_data_flag() -> None:
    p = dlt.pipeline(pipeline_name="pipe_" + uniq_id(), destination="dummy")
    assert p.has_pending_data is False
    p.extract([1, 2, 3], table_name="dummy_table")
    assert p.has_pending_data is True
    p.normalize()
    assert p.has_pending_data is True
    p.load()
    assert p.has_pending_data is False


def test_sentry_tracing() -> None:
    import sentry_sdk

    os.environ["COMPLETED_PROB"] = "1.0"  # make it complete immediately
    os.environ["RUNTIME__SENTRY_DSN"] = TEST_SENTRY_DSN

    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")

    # def inspect_transaction(ctx):
    #     print(ctx)
    #     return 1.0

    # sentry_sdk.Hub.current.client.options["traces_sampler"] = inspect_transaction

    # def inspect_events(event, hint):
    #     print(event)
    #     print(hint)
    #     return event

    # sentry_sdk.Hub.current.client.options["before_send"] = inspect_events

    @dlt.resource
    def r_check_sentry():
        assert sentry_sdk.Hub.current.scope.span.op == "extract"
        assert sentry_sdk.Hub.current.scope.span.containing_transaction.name == "run"
        yield [1,2,3]

    p.run(r_check_sentry)
    assert sentry_sdk.Hub.current.scope.span is None
    sentry_sdk.flush()

    @dlt.resource
    def r_fail():
        raise NotImplementedError()

    # run pipeline with error in extract
    with pytest.raises(PipelineStepFailed) as py_ex:
        p.run(r_fail)
    assert py_ex.value.step == "extract"
    # sentry cleaned up
    assert sentry_sdk.Hub.current.scope.span is None

    # run pipeline with error in load
    os.environ["FAIL_SCHEMA_UPDATE"] = "true"
    with pytest.raises(PipelineStepFailed) as py_ex:
        p.run(r_check_sentry)
    assert py_ex.value.step == "load"
    assert sentry_sdk.Hub.current.scope.span is None



def test_pipeline_state_on_extract_exception() -> None:
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")


    @dlt.resource
    def data_piece_1():
        yield [1, 2, 3]
        yield [3, 4, 5]

    @dlt.resource
    def data_piece_2():
        yield [6, 7, 8]
        raise NotImplementedError()

    with pytest.raises(PipelineStepFailed):
        p.run([data_piece_1, data_piece_2], write_disposition="replace")

    # first run didn't really happen
    assert p.first_run is True
    assert p.has_data is False
    assert p._schema_storage.list_schemas() == []
    assert p.default_schema_name is None

    # restore the pipeline
    p = dlt.attach(pipeline_name)
    assert p.first_run is True
    assert p.has_data is False
    assert p._schema_storage.list_schemas() == []
    assert p.default_schema_name is None

    # same but with multiple sources generating many schemas

    @dlt.source
    def data_schema_1():
        return data_piece_1

    @dlt.source
    def data_schema_2():
        return data_piece_1

    @dlt.source
    def data_schema_3():
        return data_piece_2

    # new pipeline
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")

    with pytest.raises(PipelineStepFailed):
        p.run([data_schema_1(), data_schema_2(), data_schema_3()], write_disposition="replace")

    # first run didn't really happen
    assert p.first_run is True
    assert p.has_data is False
    assert p._schema_storage.list_schemas() == []
    assert p.default_schema_name is None

    os.environ["COMPLETED_PROB"] = "1.0"  # make it complete immediately
    p.run([data_schema_1(), data_schema_2()], write_disposition="replace")
    assert p.schema_names == p._schema_storage.list_schemas()


def test_run_with_table_name_exceeding_path_length() -> None:
    pipeline_name = "pipe_" + uniq_id()
    # os.environ["COMPLETED_PROB"] = "1.0"  # make it complete immediately
    p = dlt.pipeline(pipeline_name=pipeline_name)

    # we must fix that
    with pytest.raises(PipelineStepFailed) as sf_ex:
        p.extract([1, 2, 3], table_name="TABLE_" + "a" * 230)
    assert isinstance(sf_ex.value.__context__, OSError)


def test_raise_on_failed_job() -> None:
    os.environ["FAIL_PROB"] = "1.0"
    os.environ["RAISE_ON_FAILED_JOBS"] = "true"
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")
    with pytest.raises(PipelineStepFailed) as py_ex:
        p.run([1, 2, 3], table_name="numbers")
    assert py_ex.value.step == "load"
    # get package info
    package_info = p.get_load_package_info(py_ex.value.step_info.loads_ids[0])
    assert package_info.state == "aborted"
    assert isinstance(py_ex.value.__context__, LoadClientJobFailed)
    assert isinstance(py_ex.value.__context__, DestinationTerminalException)
    # next call to run does nothing
    load_info = p.run()
    assert load_info is None


def test_load_info_raise_on_failed_jobs() -> None:
    os.environ["COMPLETED_PROB"] = "1.0"
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")
    load_info = p.run([1, 2, 3], table_name="numbers")
    assert load_info.has_failed_jobs is False
    load_info.raise_on_failed_jobs()
    os.environ["COMPLETED_PROB"] = "0.0"
    os.environ["FAIL_PROB"] = "1.0"

    load_info = p.run([1, 2, 3], table_name="numbers")
    assert load_info.has_failed_jobs is True
    with pytest.raises(DestinationHasFailedJobs) as py_ex:
        load_info.raise_on_failed_jobs()
    assert py_ex.value.destination_name == "dummy"
    assert py_ex.value.load_id == load_info.loads_ids[0]

    os.environ["RAISE_ON_FAILED_JOBS"] = "true"
    with pytest.raises(PipelineStepFailed) as py_ex_2:
        p.run([1, 2, 3], table_name="numbers")
    load_info = py_ex_2.value.step_info
    assert load_info.has_failed_jobs is True
    with pytest.raises(DestinationHasFailedJobs) as py_ex:
        load_info.raise_on_failed_jobs()
    assert py_ex.value.destination_name == "dummy"
    assert py_ex.value.load_id == load_info.loads_ids[0]


def test_run_load_pending() -> None:
    # prepare some data and complete load with run
    os.environ["COMPLETED_PROB"] = "1.0"
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")


    def some_data():
        yield from [1, 2, 3]

    @dlt.source
    def source():
        return dlt.resource(some_data(), name="numbers")

    s = source()
    p.extract(s)
    assert s.exhausted
    # will normalize and load, the data, the source will not be evaluated so there's no exception
    load_info = p.run(s)
    assert len(load_info.loads_ids) == 1
    # now it is
    with pytest.raises(PipelineStepFailed) as py_ex:
        p.run(s)
    assert isinstance(py_ex.value.__context__, SourceExhausted)

    # now only load
    s = source()
    p.extract(s)
    p.normalize()
    load_info = p.run(s)
    assert len(load_info.loads_ids) == 1


def test_retry_load() -> None:
    retry_count = 2

    os.environ["COMPLETED_PROB"] = "1.0"
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")

    @dlt.resource
    def fail_extract():
        nonlocal retry_count
        retry_count -= 1
        if retry_count == 0:
            yield [1, 2, 3]
        else:
            raise Exception("Transient")

    attempt = None

    for attempt in Retrying(stop=stop_after_attempt(3), retry=retry_if_exception(retry_load(("load", "extract"))), reraise=True):
        with attempt:
            p.run(fail_extract())
    # it retried
    assert retry_count == 0

    # now it fails (extract is terminal exception)
    retry_count = 2
    with pytest.raises(PipelineStepFailed) as py_ex:
        for attempt in Retrying(stop=stop_after_attempt(3), retry=retry_if_exception(retry_load(())), reraise=True):
            with attempt:
                p.run(fail_extract())
    assert isinstance(py_ex.value, PipelineStepFailed)
    assert py_ex.value.step == "extract"

    os.environ["COMPLETED_PROB"] = "0.0"
    os.environ["RAISE_ON_FAILED_JOBS"] = "true"
    os.environ["FAIL_PROB"] = "1.0"
    with pytest.raises(PipelineStepFailed) as py_ex:
        for attempt in Retrying(stop=stop_after_attempt(3), retry=retry_if_exception(retry_load(("load", "extract"))), reraise=True):
            with attempt:
                p.run(fail_extract())
    assert isinstance(py_ex.value, PipelineStepFailed)
    assert py_ex.value.step == "load"


@pytest.mark.skip("Not implemented")
def test_extract_exception() -> None:
    # make sure that PipelineStepFailed contains right step information
    # TODO: same tests for normalize and load
    pass


@pytest.mark.skip("Not implemented")
def test_extract_all_data_types() -> None:
    # list, iterators, generators, resource, source, list of resources, list of sources
    pass


def test_set_get_local_value() -> None:
    p = dlt.pipeline(destination="dummy", full_refresh=True)
    value = uniq_id()
    # value is set
    p.set_local_state_val(value, value)
    assert p.get_local_state_val(value) == value
    # check if this is actual local state
    assert p.state["_local"][value] == value  # type: ignore[literal-required]

    new_val = uniq_id()
    # check in context manager
    @dlt.resource
    def _w_local_state():
        # join existing managed state
        p.set_local_state_val(new_val, new_val)
        yield 1

    p.extract(_w_local_state)
    assert p.state["_local"][new_val] == new_val  # type: ignore[literal-required]


def test_changed_write_disposition() -> None:
    os.environ["COMPLETED_PROB"] = "1.0"
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")

    @dlt.resource
    def resource_1():
        yield [1, 2, 3]

    p.run(resource_1, write_disposition="append")
    assert p.default_schema.get_table("resource_1")["write_disposition"] == "append"

    p.run(resource_1, write_disposition="append")
    assert p.default_schema.get_table("resource_1")["write_disposition"] == "append"

    p.run(resource_1, write_disposition="replace")
    assert p.default_schema.get_table("resource_1")["write_disposition"] == "replace"


@dlt.transformer(name="github_repo_events", primary_key="id", write_disposition="merge", table_name=lambda i: i['type'])
def github_repo_events(page):
    yield page


@dlt.transformer(name="github_repo_events", primary_key="id", write_disposition="merge")
def github_repo_events_table_meta(page):
    yield from [dlt.mark.with_table_name(p, p['type']) for p in page]


@dlt.resource
def _get_shuffled_events():
    with open("tests/normalize/cases/github.events.load_page_1_duck.json", "r", encoding="utf-8") as f:
        issues = json.load(f)
        yield issues


@pytest.mark.parametrize('github_resource', (github_repo_events_table_meta, github_repo_events))
def test_dispatch_rows_to_tables(github_resource: DltResource):

    os.environ["COMPLETED_PROB"] = "1.0"
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")

    info = p.run(_get_shuffled_events | github_resource)
    assert_load_info(info)

    # get all expected tables
    events = list(_get_shuffled_events)
    expected_tables = set(map(lambda e: p.default_schema.naming.normalize_identifier(e["type"]), events))

    # all the tables present
    assert expected_tables.intersection([t["name"] for t in p.default_schema.data_tables()]) == expected_tables

    # all the columns have primary keys and merge disposition derived from resource
    for table in  p.default_schema.data_tables():
        if table.get("parent") is None:
            assert table["write_disposition"] == "merge"
            assert table["columns"]["id"]["primary_key"] is True


def test_resource_name_in_schema() -> None:
    @dlt.resource(table_name='some_table')
    def static_data():
        yield {'a': 1, 'b': 2}

    @dlt.resource(table_name=lambda x: 'dynamic_func_table')
    def dynamic_func_data():
        yield {'a': 1, 'b': 2}

    @dlt.resource
    def dynamic_mark_data():
        yield dlt.mark.with_table_name({'a': 1, 'b': 2}, 'dynamic_mark_table')

    @dlt.resource(table_name='parent_table')
    def nested_data():
        yield {'a': 1, 'items': [{'c': 2}, {'c': 3}, {'c': 4}]}

    @dlt.source
    def some_source():
        return [static_data(), dynamic_func_data(), dynamic_mark_data(), nested_data()]


    source = some_source()
    p = dlt.pipeline(pipeline_name=uniq_id(), destination='dummy')
    p.run(source)

    schema = p.default_schema

    assert schema.tables['some_table']['resource'] == 'static_data'
    assert schema.tables['dynamic_func_table']['resource'] == 'dynamic_func_data'
    assert schema.tables['dynamic_mark_table']['resource'] == 'dynamic_mark_data'
    assert schema.tables['parent_table']['resource'] == 'nested_data'
    assert 'resource' not in schema.tables['parent_table__items']


def test_preserve_fields_order() -> None:
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")

    item = {"col_1": 1, "col_2": 2, "col_3": "list"}
    p.extract([item], table_name="order_1")
    p.normalize()

    @dlt.resource(name="order_2")
    def ordered_dict():
        yield {"col_1": 1, "col_2": 2, "col_3": "list"}

    def reverse_order(item):
        rev_dict = {}
        for k in reversed(item.keys()):
            rev_dict[k] = item[k]
        return rev_dict

    p.extract(ordered_dict().add_map(reverse_order))
    p.normalize()

    assert list(p.default_schema.tables["order_1"]["columns"].keys()) == ["col_1", "col_2", "col_3", '_dlt_load_id', '_dlt_id']
    assert list(p.default_schema.tables["order_2"]["columns"].keys()) == ["col_3", "col_2", "col_1", '_dlt_load_id', '_dlt_id']


def run_deferred(iters):

    @dlt.defer
    def item(n):
        sleep(random.random() / 2)
        return n

    for n in range(iters):
        yield item(n)


@dlt.source
def many_delayed(many, iters):
    for n in range(many):
        yield dlt.resource(run_deferred(iters), name="resource_" + str(n))


@pytest.mark.parametrize("progress", ["tqdm", "enlighten", "log", "alive_progress"])
def test_pipeline_progress(progress: TCollectorArg) -> None:

    os.environ["TIMEOUT"] = "3.0"

    p = dlt.pipeline(destination="dummy", progress=progress)
    p.extract(many_delayed(5, 10))
    p.normalize()

    collector = p.collector

    # attach pipeline
    p = dlt.attach(progress=collector)
    p.extract(many_delayed(5, 10))
    p.run(dataset_name="dummy")

    assert collector == p.drop().collector

    # make sure a valid logger was used
    if progress == "tqdm":
        assert isinstance(collector, TqdmCollector)
    if progress == "enlighten":
        assert isinstance(collector, EnlightenCollector)
    if progress == "alive_progress":
        assert isinstance(collector, AliveCollector)
    if progress == "log":
        assert isinstance(collector, LogCollector)


def test_pipeline_log_progress() -> None:

    os.environ["TIMEOUT"] = "3.0"

    # will attach dlt logger
    p = dlt.pipeline(destination="dummy", progress=dlt.progress.log(0.5, logger=None, log_level=logging.WARNING))
    # collector was created before pipeline so logger is not attached
    assert cast(LogCollector, p.collector).logger is None
    p.extract(many_delayed(2, 10))
    # dlt logger attached
    assert cast(LogCollector, p.collector).logger is not None

    # pass explicit root logger
    p = dlt.attach(progress=dlt.progress.log(0.5, logger=logging.getLogger()))
    assert cast(LogCollector, p.collector).logger is not None
    p.extract(many_delayed(2, 10))


def test_pipeline_source_state_activation() -> None:

    appendix_yielded = None

    @dlt.source
    def reads_state(source_st, resource_st):
        if dlt.current.source_state().get("appendix"):

            @dlt.resource
            def appendix():
                nonlocal appendix_yielded
                appendix_yielded = dlt.current.source_state().get("appendix") or ["NO"]
                yield appendix_yielded

            yield appendix

        @dlt.resource
        def writes_state():
            dlt.current.source_state()["appendix"] = source_st
            dlt.current.resource_state()["RX"] = resource_st
            yield from [1,2,3]

        yield writes_state

    # activate first pipeline
    p_appendix = dlt.pipeline(pipeline_name="appendix_p")
    assert p_appendix.is_active
    s_appendix = reads_state("appendix", "r_appendix")
    assert s_appendix.state == {}
    # create state by running extract
    p_appendix.extract(s_appendix)
    assert s_appendix.state == {'appendix': 'appendix', 'resources': {'writes_state': {'RX': 'r_appendix'}}}
    assert s_appendix.writes_state.state == {'RX': 'r_appendix'}

    # change the active pipeline
    p_postfix = dlt.pipeline(pipeline_name="postfix_p")
    # now state comes from active pipeline which has an empty state
    assert s_appendix.state == {}
    # and back
    p_appendix.activate()
    assert s_appendix.writes_state.state == {'RX': 'r_appendix'}

    # create another source
    s_w_appendix = reads_state("appendix", "r_appendix")
    # has appendix because state is present
    assert hasattr(s_w_appendix, "appendix")

    # now extract the source in p_postfix that does not have a state
    p_postfix.activate()
    p_postfix.extract(s_w_appendix)
    # so appendix yielded NO
    # NOTE: this behavior is not intuitive, s_w_appendix was created with p_appendix active so appendix resource was created because state was set
    # but we evaluated it in p_postfix pipeline without the state
    assert appendix_yielded == ["NO"]

    with pytest.raises(PipelineNotActive):
        p_appendix.deactivate()

    p_postfix.deactivate()
    with pytest.raises(PipelineStateNotAvailable):
        assert s_appendix.state == {}


def test_extract_add_tables() -> None:
    # we extract and make sure that tables are added to schema
    s = airtable_emojis()
    assert list(s.resources.keys()) == ["💰Budget", "📆 Schedule", "🦚Peacock", "🦚WidePeacock"]
    assert s.resources["🦚Peacock"].compute_table_schema()["resource"] == "🦚Peacock"
    # only name will be normalized
    assert s.resources["🦚Peacock"].compute_table_schema()["name"] == "🦚Peacock"
    assert s.resources["💰Budget"].compute_table_schema()["columns"]["🔑book_id"]["name"] == "🔑book_id"
    pipeline = dlt.pipeline(pipeline_name="emojis", destination="dummy")
    info = pipeline.extract(s)
    assert info.extract_data_info[0]["name"] == "airtable_emojis"
    schema = pipeline.default_schema
    assert schema.tables["_schedule"]["resource"] == "📆 Schedule"
    assert len(schema.tables["_schedule"]["columns"]) == 0
    assert "_budget" not in schema.tables

    # extract peacock
    s = airtable_emojis()
    s.resources["🦚Peacock"].selected = True
    pipeline.extract(s)
    # live schema
    assert schema.tables["_peacock"]["resource"] == "🦚Peacock"
    assert len(schema.tables["_peacock"]["columns"]) == 1
    assert "_wide_peacock" not in schema.tables

    # extract wide peacock
    s = airtable_emojis()
    s.resources["🦚WidePeacock"].selected = True
    pipeline.extract(s)
    assert len(schema.tables["_wide_peacock"]["columns"]) == 0
    assert schema.tables["_wide_peacock"]["resource"] == "🦚WidePeacock"

    # now normalize, we should have columns
    pipeline.normalize()
    # dlt tables added
    assert len(schema.tables["_wide_peacock"]["columns"]) == 2
    # resource still not normalized
    assert schema.tables["_wide_peacock"]["resource"] == "🦚WidePeacock"

    # reload schema
    schema = pipeline._schema_storage.load_schema("airtable_emojis")
    assert len(schema.tables["_wide_peacock"]["columns"]) == 2
    # resource still not normalized
    assert schema.tables["_wide_peacock"]["resource"] == "🦚WidePeacock"


def test_emojis_resource_names() -> None:
    pipeline = dlt.pipeline(pipeline_name="emojis", destination="duckdb")
    info = pipeline.run(airtable_emojis())
    assert_load_info(info)
    # make sure that resource in schema update has a right name
    table = info.load_packages[0].schema_update["_schedule"]
    assert table["resource"] == "📆 Schedule"
    # only schedule is added
    assert set(info.load_packages[0].schema_update.keys()) == {"_dlt_version", "_dlt_loads", "_schedule", "_dlt_pipeline_state"}
    info = pipeline.run(airtable_emojis())
    assert_load_info(info)
    # here we add _peacock with has primary_key (so at least single column)
    s = airtable_emojis()
    s.resources["🦚Peacock"].selected = True
    info = pipeline.run(s)
    assert_load_info(info)
    table = info.load_packages[0].schema_update["_peacock"]
    assert table["resource"] == "🦚Peacock"
    # here we add _wide_peacock which has no columns
    s = airtable_emojis()
    s.resources["🦚WidePeacock"].selected = True
    info = pipeline.run(s)
    assert_load_info(info)
    table = info.load_packages[0].schema_update["_wide_peacock"]
    assert table["resource"] == "🦚WidePeacock"


def test_apply_hints_infer_hints() -> None:
    os.environ["COMPLETED_PROB"] = "1.0"

    @dlt.source
    def infer():
        yield dlt.resource([{"id": 1, "timestamp": "NOW"}], name="table1", columns=[new_column("timestamp", nullable=True)])

    new_new_hints = {
        "not_null": ["timestamp"],
        "primary_key": ["id"]
    }
    s = infer()
    s.schema.merge_hints(new_new_hints)  # type: ignore[arg-type]
    pipeline = dlt.pipeline(pipeline_name="inf", destination="dummy")
    pipeline.run(s)
    # check schema
    table = pipeline.default_schema.get_table("table1")
    # nullable True coming from hint overrides inferred hint
    assert table["columns"]["timestamp"] == {"name": "timestamp", "data_type": "text", "nullable": True}
    # fully from data
    assert table["columns"]["id"] == {"name": "id", "data_type": "bigint", "nullable": True, "primary_key": True}

    # remove primary key and change nullable
    s = infer()
    s.table1.apply_hints(columns=[{"name": "timestamp", "nullable": False}, {"name": "id", "nullable": False, "primary_key": False}])
    pipeline.run(s)
    table = pipeline.default_schema.get_table("table1")
    # hints overwrite pipeline schema
    assert table["columns"]["timestamp"] == {"name": "timestamp", "data_type": "text", "nullable": False}
    assert table["columns"]["id"] == {"name": "id", "data_type": "bigint", "nullable": False, "primary_key": False}
    # print(pipeline.default_schema.to_pretty_yaml())


def test_invalid_data_edge_cases() -> None:
    # pass not evaluated source function
    @dlt.source
    def my_source():
        return dlt.resource(itertools.count(start=1), name="infinity").add_limit(5)

    pipeline = dlt.pipeline(pipeline_name="invalid", destination="dummy")
    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(my_source)
    assert isinstance(pip_ex.value.__context__, PipeGenInvalid)
    assert "dlt.source" in str(pip_ex.value)

    def res_return():
        return dlt.resource(itertools.count(start=1), name="infinity").add_limit(5)

    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(res_return)
    assert isinstance(pip_ex.value.__context__, PipeGenInvalid)
    assert "dlt.resource" in str(pip_ex.value)

    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run({"a": "b"}, table_name="data")
    assert isinstance(pip_ex.value.__context__, InvalidResourceDataTypeBasic)

    # check same cases but that yield
    @dlt.source
    def my_source_yield():
        yield dlt.resource(itertools.count(start=1), name="infinity").add_limit(5)

    pipeline = dlt.pipeline(pipeline_name="invalid", destination="dummy")
    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(my_source_yield)
    assert isinstance(pip_ex.value.__context__, PipeGenInvalid)
    assert "dlt.source" in str(pip_ex.value)

    def res_return_yield():
        return dlt.resource(itertools.count(start=1), name="infinity").add_limit(5)

    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(res_return_yield)
    assert isinstance(pip_ex.value.__context__, PipeGenInvalid)
    assert "dlt.resource" in str(pip_ex.value)


@pytest.mark.parametrize('method', ('extract', 'run'))
def test_column_argument_pydantic(method: str) -> None:
    """Test columns schema is created from pydantic model"""
    p = dlt.pipeline(destination='duckdb')

    @dlt.resource
    def some_data() -> Iterator[Dict[str, Any]]:
        yield {}

    class Columns(BaseModel):
        a: Optional[int]
        b: Optional[str]

    if method == 'run':
        p.run(some_data(), columns=Columns)
    else:
        p.extract(some_data(), columns=Columns)

    assert p.default_schema.tables['some_data']['columns']['a']['data_type'] == 'bigint'
    assert p.default_schema.tables['some_data']['columns']['a']['nullable'] is True
    assert p.default_schema.tables['some_data']['columns']['b']['data_type'] == 'text'
    assert p.default_schema.tables['some_data']['columns']['b']['nullable'] is True


def test_extract_pydantic_models() -> None:
    pipeline = dlt.pipeline(destination='duckdb')

    class User(BaseModel):
        user_id: int
        name: str

    @dlt.resource
    def users() -> Iterator[User]:
        yield User(user_id=1, name="a")
        yield User(user_id=2, name="b")

    pipeline.extract(users())

    storage = ExtractorStorage(pipeline._normalize_storage_config)
    expect_extracted_file(
        storage, pipeline.default_schema_name, "users", json.dumps([{"user_id": 1, "name": "a"}, {"user_id": 2, "name": "b"}])
    )


def test_resource_rename_same_table():
    @dlt.resource(write_disposition="replace")
    def generic(start):
        dlt.current.resource_state()["start"] = start
        yield [{"id": idx, "text": "A"*idx} for idx in range(start, start + 10)]

    pipeline = dlt.pipeline(destination='duckdb')
    load_info = pipeline.run([
        generic(10).with_name("state1"),
        generic(20).with_name("state2")
    ], table_name="single_table")
    assert_load_info(load_info)
    # both resources loaded
    assert pipeline.last_trace.last_normalize_info.row_counts["single_table"] == 20
    # only this table and state
    assert len(pipeline.last_trace.last_normalize_info.row_counts) == 2

    # check state
    # state1 should have 10
    assert generic(0).with_name("state1").state["start"] == 10
    # state2 is 10
    assert generic(0).with_name("state2").state["start"] == 20

    # NOTE: only one resource will be set in table
    assert pipeline.default_schema.get_table("single_table")["resource"] == "state2"

    # now load only state1
    load_info = pipeline.run([
        generic(5).with_name("state1"),
    ], table_name="single_table")
    assert_load_info(load_info)
    # both resources loaded
    assert pipeline.last_trace.last_normalize_info.row_counts["single_table"] == 10
    assert generic(0).with_name("state1").state["start"] == 5
    # resource got swapped to the most recent one
    assert pipeline.default_schema.get_table("single_table")["resource"] == "state1"


@pytest.mark.parametrize("file_format", ("parquet", "insert_values", "jsonl"))
def test_columns_hint_with_file_formats(file_format: TLoaderFileFormat) -> None:

    @dlt.resource(write_disposition="replace", columns=[{"name": "text", "data_type": "text"}])
    def generic(start=8):
        yield [{"id": idx, "text": "A"*idx} for idx in range(start, start + 10)]

    pipeline = dlt.pipeline(destination='duckdb')
    pipeline.run(generic(), loader_file_format=file_format)
