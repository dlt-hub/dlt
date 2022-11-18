import os
from typing import Any, Iterator

import pytest

import dlt
from dlt.common.configuration.container import Container
from dlt.common.exceptions import UnknownDestinationModule
from dlt.common.pipeline import PipelineContext
from dlt.common.schema.exceptions import InvalidDatasetName
from dlt.common.schema import Schema

from dlt.extract.exceptions import SourceExhausted
from dlt.extract.source import DltSource
from dlt.pipeline import state as state_module
from dlt.pipeline.exceptions import InvalidPipelineName, PipelineStateNotAvailable, PipelineStepFailed

from tests.utils import ALL_DESTINATIONS, TEST_STORAGE_ROOT, preserve_environ, autouse_test_storage
from tests.common.configuration.utils import environment
from tests.pipeline.utils import drop_dataset_from_env, patch_working_dir, drop_pipeline

@dlt.resource
def some_data():
    last_value = dlt.state().get("last_value", 0)
    yield [1,2,3]
    dlt.state()["last_value"] = last_value + 1


def test_managed_state() -> None:

    p = dlt.pipeline(pipeline_name="managed_state")
    p.extract(some_data)
    # managed state becomes the source name
    state = p.state["sources"]
    assert "managed_state" in state
    assert state["managed_state"]["last_value"] == 1
    # run again - increases the last_value
    p.extract(some_data())
    state = p.state["sources"]
    assert state["managed_state"]["last_value"] == 2
    # attach to different source that will get separate state

    @dlt.source
    def some_source():
        return some_data

    p.extract(some_source())
    state = p.state["sources"]
    assert state["some_source"]["last_value"] == 1
    assert state["managed_state"]["last_value"] == 2
    # attach to a different source by forcing to different schema
    p.extract(some_data(), schema=Schema("default"))
    state = p.state["sources"]
    assert state["some_source"]["last_value"] == 1
    assert state["managed_state"]["last_value"] == 2
    assert state["default"]["last_value"] == 1


def test_must_have_active_pipeline() -> None:
    # iterate resource
    with pytest.raises(PipelineStateNotAvailable) as py_ex:
        for _ in some_data():
            pass
    assert py_ex.value.source_name is None

    # call source that reads state
    @dlt.source
    def some_source():
        dlt.state().get("last_value", 0)
        return some_data

    with pytest.raises(PipelineStateNotAvailable) as py_ex:
        some_source()
    assert py_ex.value.source_name == "some_source"


def test_unmanaged_state() -> None:
    p = dlt.pipeline(pipeline_name="unmanaged")
    # evaluate generator that reads and writes state
    list(some_data())
    # state is not in pipeline
    assert "sources" not in p.state
    # but last state global will have it directly in source because resource was evaluated outside of pipeline
    assert state_module._last_full_state["sources"]["last_value"] == 1
    # this state is discarded
    list(some_data())
    # state is not in pipeline
    assert "sources" not in p.state
    assert state_module._last_full_state["sources"]["last_value"] == 1

    @dlt.source
    def some_source():
        state = dlt.state()
        value = state.get("last_value", 0)
        state["last_value"] = value + 1
        return some_data

    some_source()
    # this time the source is there
    assert state_module._last_full_state["sources"]["some_source"]["last_value"] == 1
    # but the state is discarded
    some_source()
    assert state_module._last_full_state["sources"]["some_source"]["last_value"] == 1

    # but when you run it inside pipeline
    p.extract(some_source())
    state = p.state["sources"]
    assert state["some_source"]["last_value"] == 1

    # the unmanaged call later gets the correct pipeline state
    some_source()
    assert state_module._last_full_state["sources"]["some_source"]["last_value"] == 2
    # again - discarded
    state = p.state["sources"]
    assert state["some_source"]["last_value"] == 1
