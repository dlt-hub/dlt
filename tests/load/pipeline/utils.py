from typing import Iterator
import pytest

import dlt

from dlt.common.configuration.container import Container
from dlt.common.pipeline import PipelineContext
from dlt.pipeline.exceptions import SqlClientNotAvailable


@pytest.fixture(autouse=True)
def drop_pipeline() -> Iterator[None]:
    yield
    if Container()[PipelineContext].is_active():
        # take existing pipeline
        p = dlt.pipeline()
        # take all schemas and if destination was set
        if p.destination:
            for schema_name in p.schema_names:
                # for each schema, drop the dataset
                try:
                    with p.sql_client(schema_name) as c:
                        try:
                            c.drop_dataset()
                            # print("dropped")
                        except Exception as exc:
                            print(exc)
                except SqlClientNotAvailable:
                    pass

        p._wipe_working_folder()
        # deactivate context
        Container()[PipelineContext].deactivate()
