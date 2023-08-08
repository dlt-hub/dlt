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
    if Container()[PipelineContext].is_active():
        # take existing pipeline
        p = dlt.pipeline()
        db_client = p._destination_client().db_client
        # TODO: drop only the dataset otherwise you destroy data for all parallel tests
        db_client.schema.delete_all()

        # p._wipe_working_folder()
        # deactivate context
        Container()[PipelineContext].deactivate()
