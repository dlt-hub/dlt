import dlt
import pytest

from dlt.destinations.impl.lance.exceptions import LanceEmbeddingsConfigurationMissing
from dlt.destinations.impl.lancedb.lancedb_adapter import lancedb_adapter
from dlt.pipeline.exceptions import PipelineStepFailed
from tests.load.utils import destinations_configs, DestinationTestConfiguration


pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_vector_configs=True, subset=("lance",)),
    ids=lambda x: x.name,
)
def test_lance_pipeline_raises_on_embed_column_without_embeddings_config(
    destination_config: DestinationTestConfiguration,
) -> None:
    # create resource with embed column
    @dlt.resource
    def items():
        yield [{"id": 1, "content": "hello"}]

    lancedb_adapter(items, embed=["content"])

    # create destination without embeddings config
    destination = destination_config.destination_factory()
    destination.config_params["embeddings"] = None

    # running pipeline should raise LanceEmbeddingsConfigurationMissing
    pipe = destination_config.setup_pipeline(
        pipeline_name="test_lance_pipe_embed_column_no_config",
        destination=destination,
        dev_mode=True,
    )
    with pytest.raises(PipelineStepFailed, match="content") as exc_info:
        pipe.run(items())
    assert isinstance(exc_info.value.exception, LanceEmbeddingsConfigurationMissing)
