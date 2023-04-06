import os
import shutil
import pytest

import dlt

from dlt.common.exceptions import PipelineStateNotAvailable
from dlt.common.schema import Schema
from dlt.common.storages import FileStorage
from dlt.common import pipeline as state_module
from dlt.common.utils import uniq_id

from dlt.pipeline.exceptions import PipelineStateEngineNoUpgradePathException
from dlt.pipeline.pipeline import Pipeline
from dlt.pipeline.state_sync import migrate_state, STATE_ENGINE_VERSION

from tests.utils import autouse_test_storage, test_storage, patch_home_dir, preserve_environ
from tests.pipeline.utils import drop_dataset_from_env, json_case_path, load_json_case, drop_pipeline


@dlt.resource()
def some_data():
    last_value = dlt.current.state().get("last_value", 0)
    yield [1,2,3]
    dlt.current.state()["last_value"] = last_value + 1


def test_managed_state() -> None:
    p = dlt.pipeline(pipeline_name="managed_state")
    p.extract(some_data)
    sources_state = p.state["sources"]
    # standalone resources get state in the section named same as pipeline
    assert "test_pipeline_state" in sources_state
    assert sources_state["test_pipeline_state"]["last_value"] == 1
    # run again - increases the last_value
    p.extract(some_data())
    sources_state = p.state["sources"]
    assert sources_state["test_pipeline_state"]["last_value"] == 2
    # attach to different source that will get separate state

    @dlt.source(section="separate_section")
    def some_source():
        assert "last_value" not in dlt.current.state()
        return some_data

    s = some_source()
    p.extract(s)
    sources_state = p.state["sources"]
    # source and all the resources within get the same state (section named after source section)
    assert sources_state[s.section]["last_value"] == 1
    assert sources_state["test_pipeline_state"]["last_value"] == 2  # the state for standalone resource not affected

    @dlt.source
    def source_same_section():
        # default section of the source and standalone resource are the same if defined in the same module, so they share the state
        assert dlt.current.state()["last_value"] == 2
        return some_data

    s = source_same_section()
    p.extract(s)
    sources_state = p.state["sources"]
    # share the state
    assert sources_state["test_pipeline_state"]["last_value"] == 3

    # the state of the standalone resource does not depend on the schema
    p.extract(some_data(), schema=Schema("default"))
    sources_state = p.state["sources"]
    assert sources_state["separate_section"]["last_value"] == 1
    assert sources_state["test_pipeline_state"]["last_value"] == 4  # increased
    assert "default" not in sources_state

    # resource without section gets the pipeline name as section
    def _gen_inner():
        dlt.state()["gen"] = True
        yield 1

    assert p.pipeline_name not in sources_state
    p.extract(_gen_inner())
    sources_state = p.state["sources"]
    assert p.pipeline_name in sources_state


def test_no_active_pipeline_required_for_resource() -> None:
    # iterate resource
    for _ in some_data():
        pass


def test_active_pipeline_required_for_source() -> None:
    # call source that reads state
    @dlt.source
    def some_source():
        dlt.current.state().get("last_value", 0)
        return some_data

    with pytest.raises(PipelineStateNotAvailable) as py_ex:
        list(some_source())
    assert py_ex.value.source_name == "test_pipeline_state"


def test_source_state_iterator():
    os.environ["COMPLETED_PROB"] = "1.0"
    pipeline_name = "pipe_" + uniq_id()

    @dlt.resource(selected=False)
    def main():
        state = dlt.current.state()
        mark = state.setdefault("mark", 1)
        # increase the multiplier each time state is obtained
        state["mark"] *= 2
        yield [1, 2, 3]
        assert dlt.current.state()["mark"] == mark*2

    @dlt.transformer(data_from=main)
    def feeding(item):
        # we must have state
        assert dlt.current.state()["mark"] > 1
        mark = dlt.current.state()["mark"]
        yield from map(lambda i: i*mark, item)

    @dlt.source(section="pass_the_state")
    def pass_the_state():
        return main, feeding

    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")
    p.extract(pass_the_state())
    p.extract(pass_the_state())
    assert p.state["sources"]["pass_the_state"]["mark"] == 4

    # evaluate source: pipeline stored value should be used
    assert list(pass_the_state()) == [8, 16, 24]


def test_unmanaged_state() -> None:
    p = dlt.pipeline(pipeline_name="unmanaged")
    # evaluate generator that reads and writes state
    list(some_data())
    # state is not in pipeline
    assert "sources" not in p.state
    # state must be available in resource section if available - exactly like in pipeline
    assert state_module._last_full_state["sources"]["test_pipeline_state"]["last_value"] == 1
    # this state is discarded
    list(some_data())
    # state is not in pipeline
    assert "sources" not in p.state
    assert state_module._last_full_state["sources"]["test_pipeline_state"]["last_value"] == 1

    # resource without section gets pipeline name as section
    def _gen_inner():
        dlt.state()["gen"] = True
        yield 1

    list(dlt.resource(_gen_inner()))
    assert state_module._last_full_state["sources"]["unmanaged"]["gen"] is True

    @dlt.source
    def some_source():
        state = dlt.current.state()
        value = state.get("last_value", 0)
        state["last_value"] = value + 1
        return some_data

    s = some_source()
    # this time the source is there
    assert state_module._last_full_state["sources"][s.section]["last_value"] == 1
    # but the state is discarded
    some_source()
    assert state_module._last_full_state["sources"][s.section]["last_value"] == 1

    # but when you run it inside pipeline
    p.extract(some_source())
    sources_state = p.state["sources"]
    assert sources_state[s.section]["last_value"] == 1

    # the unmanaged call later gets the correct pipeline state
    some_source()
    assert state_module._last_full_state["sources"][s.section]["last_value"] == 2
    # again - discarded
    sources_state = p.state["sources"]
    assert sources_state[s.section]["last_value"] == 1


def test_unmanaged_state_no_pipeline() -> None:
    list(some_data())
    assert state_module._last_full_state["sources"]["test_pipeline_state"]["last_value"] == 1

    def _gen_inner():
        dlt.state()["gen"] = True
        yield 1

    list(dlt.resource(_gen_inner()))
    fk = next(iter(state_module._last_full_state["sources"]))
    assert state_module._last_full_state["sources"][fk]["gen"] is True


def test_migrate_state(test_storage: FileStorage) -> None:
    state_v1 = load_json_case("state/state.v1")
    state = migrate_state("test_pipeline", state_v1, state_v1["_state_engine_version"], STATE_ENGINE_VERSION)
    assert state["_state_engine_version"] == STATE_ENGINE_VERSION
    assert "_local" in state

    with pytest.raises(PipelineStateEngineNoUpgradePathException) as py_ex:
        state_v1 = load_json_case("state/state.v1")
        migrate_state("test_pipeline", state_v1, state_v1["_state_engine_version"], STATE_ENGINE_VERSION + 1)
    assert py_ex.value.init_engine == state_v1["_state_engine_version"]
    assert py_ex.value.from_engine == STATE_ENGINE_VERSION
    assert py_ex.value.to_engine == STATE_ENGINE_VERSION + 1

    # also test pipeline init where state is old
    test_storage.create_folder("debug_pipeline")
    shutil.copy(json_case_path("state/state.v1"), test_storage.make_full_path(f"debug_pipeline/{Pipeline.STATE_FILE}"))
    p = dlt.attach(pipeline_name="debug_pipeline", pipelines_dir=test_storage.storage_path)
    assert p.dataset_name == "debug_pipeline_data"
    assert p.default_schema_name == "example_source"
