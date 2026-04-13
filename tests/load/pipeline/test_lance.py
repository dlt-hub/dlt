import dlt
import pytest

from typing import cast

from dlt.destinations.impl.lance.exceptions import LanceEmbeddingsConfigurationMissing
from dlt.destinations.impl.lance.lance_adapter import lance_adapter
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

    lance_adapter(items, embed=["content"])

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


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_vector_configs=True, subset=("lance",)),
    ids=lambda x: x.name,
)
def test_lance_pipeline_branching(
    destination_config: DestinationTestConfiguration,
) -> None:
    from dlt.destinations.impl.lance.lance_client import LanceClient

    pipe = destination_config.setup_pipeline(
        pipeline_name="test_lance_branch_isolation",
        dev_mode=True,
    )

    # first run: write to main (no branch)
    pipe.run([{"id": 1, "text": "main-record"}], table_name="items")

    # second run: write to "staging" branch
    pipe.destination.config_params["storage"] = {"branch_name": "staging"}
    pipe.run(
        [{"id": 2, "text": "branch-record-1"}, {"id": 3, "text": "branch-record-2"}],
        table_name="items",
    )

    # third run: write to "dev" branch with schema evolution (extra column)
    pipe.destination.config_params["storage"] = {"branch_name": "dev"}
    pipe.run(
        [{"id": 4, "text": "dev-record-1", "a_new_column": 1}],
        table_name="items",
    )

    # fourth run: write to "dev" again (verifies schema reads from branch, not main)
    pipe.run(
        [
            {"id": 5, "text": "dev-record-2", "a_new_column": 2},
            {"id": 6, "text": "dev-record-3", "a_new_column": 3},
        ],
        table_name="items",
    )

    # verify branch isolation
    with pipe.destination_client() as client:
        client = cast(LanceClient, client)

        # open datasets for each branch
        main_ds = client.open_lance_dataset("items")
        staging_ds = client.open_lance_dataset("items", branch_name="staging")
        dev_ds = client.open_lance_dataset("items", branch_name="dev")

        # branches fork from main, not from each other
        assert main_ds.count_rows() == 1
        assert staging_ds.count_rows() == 3  # 1 from main + 2 new
        assert dev_ds.count_rows() == 4  # 1 from main + 1 new + 2 new

        # schema evolution: "a_new_column" exists on dev branch only
        assert "a_new_column" in dev_ds.schema.names
        assert "a_new_column" not in main_ds.schema.names
        assert "a_new_column" not in staging_ds.schema.names

        # get_table_schema respects configured branch
        assert client.config.storage.branch_name == "dev"
        assert "a_new_column" in client.get_table_schema("items").names

        # 1 load into main, 2 loads into staging (1 main + 1 staging), 3 loads into dev (1 main + 2 dev)
        loads_table_name = client.schema.loads_table_name
        assert client.open_lance_dataset(loads_table_name).count_rows() == 1
        assert client.open_lance_dataset(loads_table_name, branch_name="staging").count_rows() == 2
        assert client.open_lance_dataset(loads_table_name, branch_name="dev").count_rows() == 3

        # other dlt tables are also branch specific
        for branch_name in [None, "staging", "dev"]:
            for table_name in (
                client.schema.version_table_name,
                client.schema.state_table_name,
            ):
                assert client.open_lance_dataset(table_name, branch_name).count_rows() > 0


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_vector_configs=True, subset=("lance",)),
    ids=lambda x: x.name,
)
def test_lance_pipeline_replace_in_branch(
    destination_config: DestinationTestConfiguration,
) -> None:
    from dlt.destinations.impl.lance.lance_client import LanceClient

    pipe = destination_config.setup_pipeline(
        pipeline_name="test_lance_pipeline_replace_in_branch",
        dev_mode=True,
    )

    # first run: write to main
    pipe.run([{"id": 1, "text": "main-record"}], table_name="items")

    # second run: write to "dev" branch
    pipe.destination.config_params["storage"] = {"branch_name": "dev"}
    pipe.run(
        [{"id": 2, "text": "dev-record-1"}, {"id": 3, "text": "dev-record-2"}],
        table_name="items",
    )

    # third run: replace on "dev" branch — should only replace branch data
    pipe.run(
        [{"id": 4, "text": "dev-replaced"}],
        table_name="items",
        write_disposition="replace",
    )

    with pipe.destination_client() as client:
        client = cast(LanceClient, client)

        # main is untouched
        main_ds = client.open_lance_dataset("items")
        assert main_ds.count_rows() == 1
        assert main_ds.to_table().column("text").to_pylist() == ["main-record"]

        # "dev" only has data from replace run
        dev_ds = client.open_lance_dataset("items", branch_name="dev")
        assert dev_ds.count_rows() == 1
        assert dev_ds.to_table().column("text").to_pylist() == ["dev-replaced"]
