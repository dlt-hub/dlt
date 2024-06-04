from typing import Any, Iterator, List, Sequence, TYPE_CHECKING, Callable
import pytest

import dlt
from dlt.common.destination.reference import WithStagingDataset

from dlt.common.configuration.container import Container
from dlt.common.pipeline import LoadInfo, PipelineContext

from tests.load.utils import DestinationTestConfiguration, destinations_configs
from dlt.destinations.exceptions import CantExtractTablePrefix

if TYPE_CHECKING:
    from dlt.destinations.impl.filesystem.filesystem import FilesystemClient

REPLACE_STRATEGIES = ["truncate-and-insert", "insert-from-staging", "staging-optimized"]


@pytest.fixture(autouse=True)
def drop_pipeline(request) -> Iterator[None]:
    yield
    if "no_load" in request.keywords:
        return
    try:
        drop_active_pipeline_data()
    except CantExtractTablePrefix:
        # for some tests we test that this exception is raised,
        # so we suppress it here
        pass


def drop_active_pipeline_data() -> None:
    """Drops all the datasets for currently active pipeline, wipes the working folder and then deactivated it."""
    if Container()[PipelineContext].is_active():
        # take existing pipeline
        p = dlt.pipeline()

        def _drop_dataset(schema_name: str) -> None:
            with p.destination_client(schema_name) as client:
                try:
                    client.drop_storage()
                    print("dropped")
                except Exception as exc:
                    print(exc)
                if isinstance(client, WithStagingDataset):
                    with client.with_staging_dataset():
                        try:
                            client.drop_storage()
                            print("staging dropped")
                        except Exception as exc:
                            print(exc)

        # drop_func = _drop_dataset_fs if _is_filesystem(p) else _drop_dataset_sql
        # take all schemas and if destination was set
        if p.destination:
            if p.config.use_single_dataset:
                # drop just the dataset for default schema
                if p.default_schema_name:
                    _drop_dataset(p.default_schema_name)
            else:
                # for each schema, drop the dataset
                for schema_name in p.schema_names:
                    _drop_dataset(schema_name)

        # p._wipe_working_folder()
        # deactivate context
        Container()[PipelineContext].deactivate()
