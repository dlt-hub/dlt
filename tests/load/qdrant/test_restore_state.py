import pytest
from qdrant_client import models

import dlt
from tests.load.utils import destinations_configs, DestinationTestConfiguration

from dlt.destinations.impl.qdrant.qdrant_job_client import QdrantClient


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_vector_configs=True, subset=["qdrant"]),
    ids=lambda x: x.name,
)
def test_uncommitted_state(destination_config: DestinationTestConfiguration):
    """Load uncommitted state into qdrant, meaning that data is written to the state
    table but load is not completed (nothing is added to loads table)

    Ensure that state restoration does not include such state
    """
    # Type hint of JobClientBase with WithStateSync mixin

    pipeline = destination_config.setup_pipeline("uncommitted_state", dev_mode=True)

    state_val = 0

    @dlt.resource
    def dummy_table():
        dlt.current.resource_state("dummy_table")["val"] = state_val
        yield [1, 2, 3]

    # Create > 10 load packages to be above pagination size when restoring state
    for _ in range(12):
        state_val += 1
        pipeline.extract(dummy_table)

    pipeline.normalize()
    info = pipeline.load()

    client: QdrantClient
    with pipeline.destination_client() as client:  # type: ignore[assignment]
        state = client.get_stored_state(pipeline.pipeline_name)

    assert state and state.version == state_val

    # Delete last 10 _dlt_loads entries so pagination is triggered when restoring state
    with pipeline.destination_client() as client:  # type: ignore[assignment]
        table_name = client._make_qualified_collection_name(
            pipeline.default_schema.loads_table_name
        )
        p_load_id = pipeline.default_schema.naming.normalize_identifier("load_id")

        client.db_client.delete(
            table_name,
            points_selector=models.Filter(
                must=[
                    models.FieldCondition(
                        key=p_load_id, match=models.MatchAny(any=info.loads_ids[2:])
                    )
                ]
            ),
        )

    with pipeline.destination_client() as client:  # type: ignore[assignment]
        state = client.get_stored_state(pipeline.pipeline_name)

    # Latest committed state is restored
    assert state and state.version == 2
