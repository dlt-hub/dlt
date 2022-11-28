import os
from typing import Any, Iterator

import pytest

import dlt
from dlt.common.configuration.container import Container
from dlt.common.exceptions import UnknownDestinationModule
from dlt.common.pipeline import PipelineContext
from dlt.common.schema.exceptions import InvalidDatasetName
from dlt.extract.exceptions import SourceExhausted
from dlt.extract.source import DltSource
from dlt.pipeline.exceptions import InvalidPipelineName, PipelineStepFailed

from tests.utils import ALL_DESTINATIONS, TEST_STORAGE_ROOT, preserve_environ, autouse_test_storage
from tests.common.configuration.utils import environment
from tests.pipeline.utils import drop_dataset_from_env, patch_working_dir, drop_pipeline


def test_default_pipeline() -> None:
    p = dlt.pipeline()
    # this is a name of executing test harness or blank pipeline on windows
    possible_names = ["dlt_pytest", "dlt_pipeline"]
    assert p.pipeline_name in possible_names
    assert p.pipelines_dir == os.path.join(TEST_STORAGE_ROOT, ".dlt", "pipelines")
    # dataset that will be used to load data is the pipeline name
    assert p.dataset_name in possible_names
    assert p.destination is None
    assert p.default_schema_name is None

    # this is the same pipeline
    p2 = dlt.pipeline()
    assert p is p2

    # this will create default schema
    p.extract(["a", "b", "c"], table_name="data")
    assert p.default_schema_name in possible_names


def test_run_full_refresh_default_dataset() -> None:
    p = dlt.pipeline(full_refresh=True)
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
    assert p.dataset_name == "another_pipeline_8329_"

    # this will create default schema
    p.extract(["a", "b", "c"], table_name="data")
    assert p.default_schema_name == "another_pipeline_8329_"


def test_invalid_dataset_name() -> None:
    with pytest.raises(InvalidDatasetName):
        dlt.pipeline(dataset_name="!")


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

    # create another pipeline
    p2 = dlt.pipeline(pipeline_name="another pipeline")
    assert ctx.pipeline() is p2

    p3 = dlt.pipeline(pipeline_name="more pipelines")
    assert ctx.pipeline() is p3

    # restore previous
    p2 = dlt.attach("another pipeline")
    assert ctx.pipeline() is p2


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


def test_create_pipeline_all_destinations() -> None:
    for dest in ALL_DESTINATIONS:
        # create pipelines, extract and normalize. that should be possible without installing any dependencies
        p = dlt.pipeline(pipeline_name=dest + "_pipeline", destination=dest)
        p.extract([1, "2", 3], table_name="data")
        p.normalize()


def test_extract_source_twice() -> None:

    def some_data():
        yield [1, 2, 3]
        yield [1, 2, 3]

    s = DltSource("source", dlt.Schema("default"), [dlt.resource(some_data())])
    dlt.pipeline().extract(s)
    with pytest.raises(PipelineStepFailed) as py_ex:
        dlt.pipeline().extract(s)
    assert type(py_ex.value.exception) == SourceExhausted
    assert py_ex.value.exception.source_name == "source"


@pytest.mark.skip("Not implemented")
def test_extract_exception() -> None:
    # make sure that PipelineStepFailed contains right step information
    # TODO: same tests for normalize and load
    pass


@pytest.mark.skip
def test_extract_all_data_types() -> None:
    # list, iterators, generators, resource, source, list of resources, list of sources
    pass