import os
import shutil
import pytest

import dlt

from dlt.common.exceptions import PipelineStateNotAvailable, ResourceNameNotAvailable
from dlt.common.schema import Schema
from dlt.common.source import get_current_pipe_name
from dlt.common.storages import FileStorage
from dlt.common import pipeline as state_module
from dlt.common.utils import uniq_id

from dlt.pipeline.exceptions import PipelineStateEngineNoUpgradePathException, PipelineStepFailed
from dlt.pipeline.pipeline import Pipeline
from dlt.pipeline.state_sync import migrate_state, STATE_ENGINE_VERSION

from tests.utils import test_storage
from tests.pipeline.utils import json_case_path, load_json_case


@dlt.resource()
def some_data():
    last_value = dlt.current.source_state().get("last_value", 0)
    yield [1,2,3]
    dlt.current.source_state()["last_value"] = last_value + 1


@dlt.resource()
def some_data_resource_state():
    last_value = dlt.current.resource_state().get("last_value", 0)
    yield [1,2,3]
    dlt.current.resource_state()["last_value"] = last_value + 1


def test_restore_state_props() -> None:
    p = dlt.pipeline(pipeline_name="restore_state_props", destination="redshift", staging="filesystem", dataset_name="the_dataset")
    p.extract(some_data())
    state = p.state
    assert state["dataset_name"] == "the_dataset"
    assert state["destination"].endswith("redshift")
    assert state["staging"].endswith("filesystem")

    p = dlt.pipeline(pipeline_name="restore_state_props")
    state = p.state
    assert state["dataset_name"] == "the_dataset"
    assert state["destination"].endswith("redshift")
    assert state["staging"].endswith("filesystem")
    # also instances are restored
    assert p.destination.__name__.endswith("redshift")
    assert p.staging.__name__.endswith("filesystem")


def test_managed_state() -> None:
    p = dlt.pipeline(pipeline_name="managed_state_pipeline")
    p.extract(some_data())
    sources_state = p.state["sources"]
    # standalone resources get state in the section named same as the default schema
    assert "managed_state" in sources_state
    assert sources_state["managed_state"]["last_value"] == 1
    assert p.default_schema_name == "managed_state"

    # run again - increases the last_value
    p.extract(some_data())
    sources_state = p.state["sources"]
    assert sources_state["managed_state"]["last_value"] == 2

    # attach to different source that will get separate state

    @dlt.source(name="separate_state", section="different_section")
    def some_source():
        assert "last_value" not in dlt.current.source_state()
        return some_data

    s = some_source()
    p.extract(s)
    sources_state = p.state["sources"]
    # the source name is the source state key
    assert sources_state[s.name]["last_value"] == 1
    assert sources_state["managed_state"]["last_value"] == 2  # the state for standalone resource not affected

    @dlt.source
    def source_same_section():
        # source has separate state key
        assert "last_value" not in dlt.current.source_state()
        return some_data

    s = source_same_section()
    p.extract(s)
    sources_state = p.state["sources"]
    # share the state
    assert sources_state["source_same_section"]["last_value"] == 1

    # the state of standalone resource will be attached to default source which derives from the passed schema
    p.extract(some_data(), schema=Schema("default"))
    sources_state = p.state["sources"]
    assert sources_state["separate_state"]["last_value"] == 1
    assert sources_state["default"]["last_value"] == 1

    # resource without section gets the default schema name as state key
    def _gen_inner():
        dlt.current.source_state()["gen"] = True
        yield 1

    p.extract(_gen_inner())
    sources_state = p.state["sources"]
    assert sources_state[p.default_schema_name]["gen"] is True


def test_no_active_pipeline_required_for_resource() -> None:
    # resource can be iterated without pipeline context
    for _ in some_data():
        pass


def test_active_pipeline_required_for_source() -> None:

    @dlt.source
    def some_source():
        dlt.current.source_state().get("last_value", 0)
        return some_data

    # source cannot be instantiated without pipeline context
    # this is to prevent users to instantiate sources before pipeline is created
    # such source would get mock state (empty) which would fail initialization
    with pytest.raises(PipelineStateNotAvailable) as py_ex:
        some_source()
    assert py_ex.value.source_state_key == "some_source"

    p = dlt.pipeline(pipeline_name="managed_state_pipeline")
    s = some_source()

    # but can be iterated without pipeline context after it is created
    p.deactivate()
    list(s)

def test_source_state_iterator():
    os.environ["COMPLETED_PROB"] = "1.0"
    pipeline_name = "pipe_" + uniq_id()

    @dlt.resource(selected=False)
    def main():
        state = dlt.current.source_state()
        print(f"main state: {state}")
        mark = state.setdefault("mark", 1)
        # increase the multiplier each time state is obtained
        state["mark"] *= 2
        yield [1, 2, 3]
        assert dlt.current.source_state()["mark"] == mark*2

    @dlt.transformer(data_from=main)
    def feeding(item):
        # we must have state
        assert dlt.current.source_state()["mark"] > 1
        print(f"feeding state {dlt.current.source_state()}")
        mark = dlt.current.source_state()["mark"]
        yield from map(lambda i: i*mark, item)

    @dlt.source
    def pass_the_state():
        return main, feeding

    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")
    p.extract(pass_the_state())
    p.extract(pass_the_state())
    assert p.state["sources"]["pass_the_state"]["mark"] == 4

    # evaluate source: pipeline stored value should be used
    print(pass_the_state().state)
    assert list(pass_the_state()) == [8, 16, 24]


def test_unmanaged_state() -> None:
    p = dlt.pipeline(pipeline_name="unmanaged_pipeline")
    # evaluate generator that reads and writes state
    list(some_data())
    # state is not in pipeline
    assert "sources" not in p.state
    # state must be available in default schema name if available - exactly like in pipeline
    assert state_module._last_full_state["sources"]["unmanaged"]["last_value"] == 1
    # this state is discarded
    list(some_data())
    # state is not in pipeline
    assert "sources" not in p.state
    assert state_module._last_full_state["sources"]["unmanaged"]["last_value"] == 1

    # resource without section gets default schema name as source state key
    def _gen_inner():
        dlt.state()["gen"] = True
        yield 1
    list(dlt.resource(_gen_inner))
    list(dlt.resource(_gen_inner()))
    assert state_module._last_full_state["sources"]["unmanaged"]["gen"] is True

    @dlt.source
    def some_source():
        state = dlt.current.source_state()
        value = state.get("last_value", 0)
        state["last_value"] = value + 1
        return some_data

    s = some_source()
    # this time the source is there
    assert state_module._last_full_state["sources"][s.name]["last_value"] == 1
    # but the state is discarded
    some_source()
    assert state_module._last_full_state["sources"][s.name]["last_value"] == 1

    # but when you run it inside pipeline
    p.extract(some_source())
    sources_state = p.state["sources"]
    assert sources_state[s.name]["last_value"] == 1

    # the unmanaged call later gets the correct pipeline state
    some_source()
    assert state_module._last_full_state["sources"][s.name]["last_value"] == 2
    # again - discarded
    sources_state = p.state["sources"]
    assert sources_state[s.name]["last_value"] == 1


def test_unmanaged_state_no_pipeline() -> None:
    list(some_data())
    print(state_module._last_full_state)
    assert state_module._last_full_state["sources"]["test_pipeline_state"]["last_value"] == 1

    def _gen_inner():
        dlt.current.state()["gen"] = True
        yield 1

    list(dlt.resource(_gen_inner()))
    fk = next(iter(state_module._last_full_state["sources"]))
    assert state_module._last_full_state["sources"][fk]["gen"] is True


def test_resource_state_write() -> None:
    r = some_data_resource_state()
    assert list(r) == [1, 2, 3]
    assert state_module._last_full_state["sources"]["test_pipeline_state"]["resources"]["some_data_resource_state"]["last_value"] == 1
    with pytest.raises(ResourceNameNotAvailable):
        get_current_pipe_name()

    def _gen_inner():
        dlt.current.resource_state()["gen"] = True
        yield 1

    p = dlt.pipeline()
    r = dlt.resource(_gen_inner(), name="name_ovrd")
    assert list(r) == [1]
    assert state_module._last_full_state["sources"][p._make_schema_with_default_name().name]["resources"]["name_ovrd"]["gen"] is True
    with pytest.raises(ResourceNameNotAvailable):
        get_current_pipe_name()


def test_resource_state_in_pipeline() -> None:
    p = dlt.pipeline()
    r = some_data_resource_state()
    p.extract(r)
    assert r.state["last_value"] == 1
    with pytest.raises(ResourceNameNotAvailable):
        get_current_pipe_name()

    def _gen_inner(tv="df"):
        dlt.current.resource_state()["gen"] = tv
        yield 1

    r = dlt.resource(_gen_inner("gen_tf"), name="name_ovrd")
    p.extract(r)
    assert r.state["gen"] == "gen_tf"
    assert state_module._last_full_state["sources"][p.default_schema_name]["resources"]["name_ovrd"]["gen"] == "gen_tf"
    with pytest.raises(ResourceNameNotAvailable):
        get_current_pipe_name()

    r = dlt.resource(_gen_inner, name="pure_function")
    p.extract(r)
    assert r.state["gen"] == "df"
    assert state_module._last_full_state["sources"][p.default_schema_name]["resources"]["pure_function"]["gen"] == "df"
    with pytest.raises(ResourceNameNotAvailable):
        get_current_pipe_name()

    # get resource state in defer function
    def _gen_inner_defer(tv="df"):

        @dlt.defer
        def _run():
            dlt.current.resource_state()["gen"] = tv
            return 1

        yield _run()

    r = dlt.resource(_gen_inner_defer, name="defer_function")
    # you cannot get resource name in `defer` function
    with pytest.raises(PipelineStepFailed) as pip_ex:
        p.extract(r)
    assert isinstance(pip_ex.value.__context__, ResourceNameNotAvailable)

    # get resource state in defer explicitly
    def _gen_inner_defer_explicit_name(resource_name, tv="df"):

        @dlt.defer
        def _run():
            dlt.current.resource_state(resource_name)["gen"] = tv
            return 1

        yield _run()

    r = dlt.resource(_gen_inner_defer_explicit_name, name="defer_function_explicit")
    p.extract(r("defer_function_explicit", "expl"))
    assert r.state["gen"] == "expl"
    assert state_module._last_full_state["sources"][p.default_schema_name]["resources"]["defer_function_explicit"]["gen"] == "expl"

    # get resource state in yielding defer (which btw is invalid and will be resolved in main thread)
    def _gen_inner_defer_yielding(tv="yielding"):

        @dlt.defer
        def _run():
            dlt.current.resource_state()["gen"] = tv
            yield from [1, 2, 3]

        yield _run()

    r = dlt.resource(_gen_inner_defer_yielding, name="defer_function_yielding")
    p.extract(r)
    assert r.state["gen"] == "yielding"
    assert state_module._last_full_state["sources"][p.default_schema_name]["resources"]["defer_function_yielding"]["gen"] == "yielding"

    # get resource state in async function
    def _gen_inner_async(tv="async"):

        async def _run():
            dlt.current.resource_state()["gen"] = tv
            return 1

        yield _run()

    r = dlt.resource(_gen_inner_async, name="async_function")
    # you cannot get resource name in `defer` function
    with pytest.raises(PipelineStepFailed) as pip_ex:
        p.extract(r)
    assert isinstance(pip_ex.value.__context__, ResourceNameNotAvailable)


def test_transformer_state_write() -> None:
    r = some_data_resource_state()

    # yielding transformer
    def _gen_inner(item):
        dlt.current.resource_state()["gen"] = True
        yield map(lambda i: i * 2, item)

    # p = dlt.pipeline()
    # p.extract(dlt.transformer(_gen_inner, data_from=r, name="tx_other_name"))
    assert list(dlt.transformer(_gen_inner, data_from=r, name="tx_other_name")) == [2, 4, 6]
    assert state_module._last_full_state["sources"]["test_pipeline_state"]["resources"]["some_data_resource_state"]["last_value"] == 1
    assert state_module._last_full_state["sources"]["test_pipeline_state"]["resources"]["tx_other_name"]["gen"] is True

    # returning transformer
    def _gen_inner_rv(item):
        dlt.current.resource_state()["gen"] = True
        return item * 2

    r = some_data_resource_state()
    assert list(dlt.transformer(_gen_inner_rv, data_from=r, name="tx_other_name_rv")) == [1, 2, 3, 1, 2, 3]
    assert state_module._last_full_state["sources"]["test_pipeline_state"]["resources"]["tx_other_name_rv"]["gen"] is True

    # deferred transformer
    @dlt.defer
    def _gen_inner_rv_defer(item):
        dlt.current.resource_state()["gen"] = True
        return item

    r = some_data_resource_state()
    # not available because executed in a pool
    with pytest.raises(ResourceNameNotAvailable):
        print(list(dlt.transformer(_gen_inner_rv_defer, data_from=r, name="tx_other_name_defer")))

    # async transformer
    async def _gen_inner_rv_async(item):
        dlt.current.resource_state()["gen"] = True
        return item

    r = some_data_resource_state()
    # not available because executed in a pool
    with pytest.raises(ResourceNameNotAvailable):
        print(list(dlt.transformer(_gen_inner_rv_async, data_from=r, name="tx_other_name_async")))

    # async transformer with explicit resource name
    async def _gen_inner_rv_async_name(item, r_name):
        dlt.current.resource_state(r_name)["gen"] = True
        return item

    r = some_data_resource_state()
    assert list(dlt.transformer(_gen_inner_rv_async_name, data_from=r, name="tx_other_name_async")("tx_other_name_async")) == [1, 2, 3]
    assert state_module._last_full_state["sources"]["test_pipeline_state"]["resources"]["tx_other_name_async"]["gen"] is True


def test_transform_function_state_write() -> None:
    r = some_data_resource_state()

    # transform executed within the same thread
    def transform(item):
        dlt.current.resource_state()["form"] = item
        return item*2

    r.add_map(transform)
    assert list(r) == [2, 4, 6]
    assert state_module._last_full_state["sources"]["test_pipeline_state"]["resources"]["some_data_resource_state"]["form"] == 3


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
