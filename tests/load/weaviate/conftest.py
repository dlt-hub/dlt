import pytest

import dlt
from dlt.common.pipeline import PipelineContext
from dlt.common.configuration.container import Container
from tests.utils import preserve_environ, autouse_test_storage, patch_home_dir


@pytest.fixture(autouse=True)
def drop_weaviate_schema() -> None:
    yield
    drop_active_pipeline_data()


def drop_active_pipeline_data() -> None:
    def schema_has_classes(client):
        schema = client.db_client.schema.get()
        return schema["classes"]

    if Container()[PipelineContext].is_active():
        # take existing pipeline
        p = dlt.pipeline()
        client = p._destination_client()

        if schema_has_classes(client):
            client.drop_dataset()

        p._wipe_working_folder()
        # deactivate context
        Container()[PipelineContext].deactivate()
