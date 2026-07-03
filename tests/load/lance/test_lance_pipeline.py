import dlt
import pytest

from typing import Any, List, cast

from dlt.common.utils import uniq_id
from dlt.common.destination.exceptions import DestinationTransientException

from dlt.destinations.impl.lance.exceptions import LanceEmbeddingsConfigurationMissing
from dlt.destinations.impl.lance.lance_adapter import lance_adapter
from dlt.destinations.impl.lance.lance_client import LanceClient
from dlt.pipeline.exceptions import PipelineStepFailed

from tests.load.utils import destinations_configs, DestinationTestConfiguration
from tests.load.lance.lance_utils import lance_rest_destination_configs


pytestmark = pytest.mark.essential

# directory namespace configs (from `destinations_configs`) plus the local REST namespace config
# that runs against the in-process REST server fixture
lance_destination_configs = [
    *destinations_configs(default_vector_configs=True, subset=("lance",)),
    *lance_rest_destination_configs(),
]


@pytest.mark.parametrize(
    "destination_config",
    lance_destination_configs,
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
    lance_destination_configs,
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
    pipe.destination.config_params["branch_name"] = "staging"
    pipe.run(
        [{"id": 2, "text": "branch-record-1"}, {"id": 3, "text": "branch-record-2"}],
        table_name="items",
    )

    # third run: write to "dev" branch with schema evolution (extra column)
    pipe.destination.config_params["branch_name"] = "dev"
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
        assert client.config.branch_name == "dev"
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
    lance_destination_configs,
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
    pipe.destination.config_params["branch_name"] = "dev"
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


@pytest.mark.parametrize(
    "destination_config",
    lance_destination_configs,
    ids=lambda x: x.name,
)
def test_lance_pipeline_branching_root_namespace(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Branching and schema evolution work for tables in the ROOT namespace (no `dataset_name`,
    single-level table ids)."""
    from dlt.destinations.impl.lance.lance_client import LanceClient

    # build the configured destination but do NOT pass a dataset_name -> root namespace
    pipe = dlt.pipeline(
        pipeline_name="test_lance_root_branch_" + uniq_id(),
        destination=destination_config.destination_factory(),
        dev_mode=True,
    )
    assert pipe.dataset_name is None

    # first run: write to main (no branch)
    pipe.run([{"id": 1, "text": "main-record"}], table_name="items")
    # second run: write to "staging" branch (forks from main)
    pipe.destination.config_params["branch_name"] = "staging"
    pipe.run([{"id": 2, "text": "b1"}, {"id": 3, "text": "b2"}], table_name="items")
    # third run: write to "dev" branch with schema evolution (extra column)
    pipe.destination.config_params["branch_name"] = "dev"
    pipe.run([{"id": 4, "text": "d1", "a_new_column": 1}], table_name="items")

    with pipe.destination_client() as client:
        client = cast(LanceClient, client)
        assert client.dataset_name is None
        # tables are addressed at the root namespace with single-level ids
        assert client.make_table_id("items") == ["items"]

        main_ds = client.open_lance_dataset("items")
        staging_ds = client.open_lance_dataset("items", branch_name="staging")
        dev_ds = client.open_lance_dataset("items", branch_name="dev")

        # branches fork from main, not from each other
        assert main_ds.count_rows() == 1
        assert staging_ds.count_rows() == 3  # 1 from main + 2 new
        assert dev_ds.count_rows() == 2  # 1 from main + 1 new

        # schema evolution is branch-isolated
        assert "a_new_column" in dev_ds.schema.names
        assert "a_new_column" not in main_ds.schema.names
        assert "a_new_column" not in staging_ds.schema.names


@pytest.mark.parametrize(
    "destination_config",
    lance_destination_configs,
    ids=lambda x: x.name,
)
def test_lance_pipeline_single_commit_per_table(
    destination_config: DestinationTestConfiguration,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # rotate job files so each table loads from multiple parquet files
    monkeypatch.setenv("NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS", "10")
    pipe = destination_config.setup_pipeline(
        pipeline_name="test_lance_single_commit",
        dev_mode=True,
    )
    pipe.run([{"id": i} for i in range(35)], table_name="items")

    with pipe.destination_client() as client:
        client = cast(LanceClient, client)
        assert client.open_lance_dataset("items").count_rows() == 35
        versions_after_first = len(client.open_lance_dataset("items").versions())

    # append: all job files of a load commit as one version
    info = pipe.run([{"id": i} for i in range(35, 70)], table_name="items")
    items_jobs = [
        j
        for j in info.load_packages[0].jobs["completed_jobs"]
        if j.job_file_info.table_name == "items" and j.job_file_info.file_format == "parquet"
    ]
    assert len(items_jobs) > 1
    with pipe.destination_client() as client:
        client = cast(LanceClient, client)
        ds = client.open_lance_dataset("items")
        assert ds.count_rows() == 70
        assert len(ds.versions()) == versions_after_first + 1
        versions_after_second = len(ds.versions())

    # replace: a single Overwrite commit replaces all rows
    pipe.run([{"id": i} for i in range(15)], table_name="items", write_disposition="replace")
    with pipe.destination_client() as client:
        client = cast(LanceClient, client)
        ds = client.open_lance_dataset("items")
        assert ds.count_rows() == 15
        assert len(ds.versions()) == versions_after_second + 1

    # merge: multiple job files merge in one operation, updates and inserts apply
    pipe.run(
        [{"id": i, "v": "old"} for i in range(25)],
        table_name="merge_items",
        write_disposition="merge",
        primary_key="id",
    )
    pipe.run(
        [{"id": i, "v": "new"} for i in range(15, 40)],
        table_name="merge_items",
        write_disposition="merge",
        primary_key="id",
    )
    with pipe.destination_client() as client:
        client = cast(LanceClient, client)
        ds = client.open_lance_dataset("merge_items")
        table = ds.to_table()
        assert table.num_rows == 40
        updated = {r["id"]: r["v"] for r in table.to_pylist()}
        assert updated[10] == "old" and updated[20] == "new" and updated[39] == "new"


@pytest.mark.parametrize(
    "destination_config",
    lance_destination_configs,
    ids=lambda x: x.name,
)
def test_lance_pipeline_replace_truncates_jobless_nested_table(
    destination_config: DestinationTestConfiguration,
) -> None:
    from dlt.destinations.impl.lance.lance_client import LanceClient

    pipe = destination_config.setup_pipeline(
        pipeline_name="test_lance_replace_nested",
        dev_mode=True,
    )

    # nested lists create a nested table in the replace chain
    pipe.run(
        [{"id": 1, "kids": [{"k": 1}, {"k": 2}]}],
        table_name="items",
        write_disposition="replace",
    )
    with pipe.destination_client() as client:
        client = cast(LanceClient, client)
        assert client.open_lance_dataset("items").count_rows() == 1
        assert client.open_lance_dataset("items__kids").count_rows() == 2
        items_versions = len(client.open_lance_dataset("items").versions())

    # no nested data: the jobless nested table is truncated, the parent is replaced
    # atomically in a single Overwrite commit without an intermediate truncation
    pipe.run([{"id": 2}, {"id": 3}], table_name="items", write_disposition="replace")
    with pipe.destination_client() as client:
        client = cast(LanceClient, client)
        ds = client.open_lance_dataset("items")
        assert sorted(r["id"] for r in ds.to_table().to_pylist()) == [2, 3]
        assert len(ds.versions()) == items_versions + 1
        assert client.open_lance_dataset("items__kids").count_rows() == 0


@pytest.mark.parametrize(
    "destination_config",
    lance_destination_configs,
    ids=lambda x: x.name,
)
def test_lance_pipeline_refresh_drop_data(
    destination_config: DestinationTestConfiguration,
) -> None:
    from dlt.destinations.impl.lance.lance_client import LanceClient

    pipe = destination_config.setup_pipeline(
        pipeline_name="test_lance_refresh_drop",
        dev_mode=True,
    )

    @dlt.source
    def two_tables(events: Any, snap: Any) -> Any:
        return [
            dlt.resource(events, name="events", write_disposition="append"),
            dlt.resource(snap, name="snap", write_disposition="replace"),
        ]

    pipe.run(two_tables([{"id": 1}, {"id": 2}], [{"id": 10}]))
    with pipe.destination_client() as client:
        client = cast(LanceClient, client)
        snap_versions = len(client.open_lance_dataset("snap").versions())

    # drop_data must truncate the append table even though it receives a data file,
    # the replace table is still overwritten in a single commit
    pipe.run(two_tables([{"id": 3}], [{"id": 20}]), refresh="drop_data")
    with pipe.destination_client() as client:
        client = cast(LanceClient, client)
        events = sorted(r["id"] for r in client.open_lance_dataset("events").to_table().to_pylist())
        assert events == [3]
        snap_ds = client.open_lance_dataset("snap")
        assert [r["id"] for r in snap_ds.to_table().to_pylist()] == [20]
        assert len(snap_ds.versions()) == snap_versions + 1


@pytest.mark.parametrize(
    "destination_config",
    lance_destination_configs,
    ids=lambda x: x.name,
)
def test_lance_commit_job_retry_idempotency(
    destination_config: DestinationTestConfiguration,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A commit job retried after its lance commit succeeded must not apply the commit
    twice: a re-run append detects its fragments in the dataset and skips, a re-run
    replace repeats the Overwrite commit with the same fragments."""
    import lance
    from dlt.destinations.impl.lance.lance_client import LanceClient

    pipe = destination_config.setup_pipeline(
        pipeline_name="test_lance_commit_retry",
        dev_mode=True,
    )
    pipe.run([{"id": i} for i in range(10)], table_name="items")
    with pipe.destination_client() as client:
        client = cast(LanceClient, client)
        versions_after_first = len(client.open_lance_dataset("items").versions())

    # crash the items commit job once, right between a successful commit and job completion
    orig_commit = lance.LanceDataset.commit
    crashed: List[bool] = []

    def crash_once_after_commit(ds: Any, *args: Any, **kwargs: Any) -> Any:
        result = orig_commit(ds, *args, **kwargs)
        if "items" in ds.uri and not crashed:
            crashed.append(True)
            raise DestinationTransientException("crash between commit and job completion")
        return result

    monkeypatch.setattr(lance.LanceDataset, "commit", crash_once_after_commit)

    # append: the retried job must skip the already applied commit, one new version only
    pipe.run([{"id": i} for i in range(10, 20)], table_name="items")
    assert crashed
    with pipe.destination_client() as client:
        client = cast(LanceClient, client)
        ds = client.open_lance_dataset("items")
        assert ds.count_rows() == 20
        assert len(ds.versions()) == versions_after_first + 1

    # replace: the retried job repeats the Overwrite commit, same rows come out
    crashed.clear()
    pipe.run([{"id": i} for i in range(5)], table_name="items", write_disposition="replace")
    assert crashed
    with pipe.destination_client() as client:
        client = cast(LanceClient, client)
        ds = client.open_lance_dataset("items")
        assert sorted(r["id"] for r in ds.to_table().to_pylist()) == list(range(5))
