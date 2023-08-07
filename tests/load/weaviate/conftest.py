from typing import Iterator

import dlt
from dlt.common.pipeline import PipelineContext

import pytest

from dlt.common.schema import Schema
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.utils import uniq_id
from dlt.destinations import weaviate
from dlt.destinations.weaviate.weaviate import WeaviateClient

@pytest.fixture(autouse=True)
def drop_weaviate_schema() -> None:
    yield
    drop_active_pipeline_data()


def drop_active_pipeline_data() -> None:
    if Container()[PipelineContext].is_active():
        # take existing pipeline
        p = dlt.pipeline()
        db_client = p._destination_client().db_client
        db_client.schema.delete_all()

        p._wipe_working_folder()
        # deactivate context
        Container()[PipelineContext].deactivate()
