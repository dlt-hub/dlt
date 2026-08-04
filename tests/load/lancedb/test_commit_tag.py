"""`commit_tag` tags every table of a dataset so a load can be read back as one tagged version."""

import os
from typing import TYPE_CHECKING, Any, Dict, Generator, cast

import pyarrow as pa
import pytest

import dlt
from dlt.common.destination import Destination
from dlt.common.typing import DictStrAny
from dlt.common.utils import uniq_id
from dlt.destinations.impl.lancedb.exceptions import LanceDBCommitTagNotApplied
from dlt.destinations.impl.lancedb.lancedb_adapter import rollback_to_commit_tag

from tests.load.lancedb.utils import LANCEDB_ONLY_CONFS, read_over_sql
from tests.load.utils import DestinationTestConfiguration
from tests.pipeline.utils import assert_load_info

if TYPE_CHECKING:
    from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient
else:
    LanceDBClient = Any

pytestmark = pytest.mark.essential


@dlt.resource(name="alpha")
def alpha(rows: int, offset: int = 0) -> Generator[DictStrAny, Any, None]:
    yield from ({"id": offset + i} for i in range(rows))


@dlt.resource(name="beta")
def beta(rows: int, offset: int = 0) -> Generator[DictStrAny, Any, None]:
    yield from ({"id": offset + i} for i in range(rows))


def tagged(
    destination_config: DestinationTestConfiguration, commit_tag: str
) -> Destination[Any, Any]:
    """Returns the destination carrying `commit_tag`, to be passed to a single `run`."""
    return destination_config.destination_factory(commit_tag=commit_tag)


def tags_of(client: "LanceDBClient", table_name: str) -> Dict[str, Any]:
    return cast(Dict[str, Any], client.open_table(table_name).tags.list())


def rows_at_tag(client: "LanceDBClient", table_name: str, tag: str) -> int:
    """Counts the rows a tag names, on a handle of its own so the checkout does not leak."""
    table = client.open_table(table_name)
    table.checkout(tag)
    return cast(int, table.count_rows())


@pytest.mark.parametrize(
    "destination_config",
    LANCEDB_ONLY_CONFS,
    ids=lambda x: x.name,
)
def test_commit_tag_snapshots_every_table_at_each_load(
    destination_config: DestinationTestConfiguration,
) -> None:
    """A tag must name every table of the dataset, including tables the load did not touch.

    Only then does the tag describe the whole dataset as it stood after that load.
    """
    pipeline = dlt.pipeline(
        pipeline_name="lancedb_tag_flow_" + uniq_id(),
        destination=tagged(destination_config, "load_1"),
        dataset_name="commit_tag_" + uniq_id(),
    )
    try:
        # step 1: both tables get data
        assert_load_info(pipeline.run([alpha(rows=2), beta(rows=3)]))

        with pipeline.destination_client() as client:
            client = cast("LanceDBClient", client)
            table_names = client.list_table_names()
            assert set(table_names) >= {"alpha", "beta", "_dlt_loads", "_dlt_version"}
            for table_name in table_names:
                table = client.open_table(table_name)
                assert tags_of(client, table_name)["load_1"]["version"] == table.version, table_name
            beta_version = client.open_table("beta").version

        # step 2: only `alpha` loads, so `beta` keeps its version but must still be tagged
        assert_load_info(
            pipeline.run(
                alpha(rows=1, offset=100), destination=tagged(destination_config, "load_2")
            )
        )

        with pipeline.destination_client() as client:
            client = cast("LanceDBClient", client)
            assert client.open_table("beta").version == beta_version
            beta_tags = tags_of(client, "beta")
            assert "load_2" in beta_tags
            # an untouched table is tagged where it already stood
            assert beta_tags["load_2"]["version"] == beta_tags["load_1"]["version"]

            alpha_tags = tags_of(client, "alpha")
            assert alpha_tags["load_2"]["version"] > alpha_tags["load_1"]["version"]

            # the `load_2` tag is alpha after its second load and beta after its first
            assert rows_at_tag(client, "alpha", "load_2") == 3
            assert rows_at_tag(client, "beta", "load_2") == 3
            assert rows_at_tag(client, "alpha", "load_1") == 2

        # step 3: `beta` loads on top of the version its `load_2` tag names
        assert_load_info(
            pipeline.run(beta(rows=4, offset=200), destination=tagged(destination_config, "load_3"))
        )

        with pipeline.destination_client() as client:
            client = cast("LanceDBClient", client)
            assert client.open_table("beta").version > beta_version
            beta_tags = tags_of(client, "beta")
            assert beta_tags["load_3"]["version"] > beta_tags["load_2"]["version"]

            # the earlier tags still read what they named
            assert rows_at_tag(client, "beta", "load_1") == 3
            assert rows_at_tag(client, "beta", "load_2") == 3
            assert rows_at_tag(client, "beta", "load_3") == 7
            # alpha did not move in step 3, so its tag points where load_2 left it
            assert tags_of(client, "alpha")["load_3"]["version"] == alpha_tags["load_2"]["version"]
            assert rows_at_tag(client, "alpha", "load_3") == 3
    finally:
        with pipeline.destination_client() as client:
            client.drop_storage()


@pytest.mark.parametrize(
    "destination_config",
    LANCEDB_ONLY_CONFS,
    ids=lambda x: x.name,
)
def test_commit_tag_is_not_applied_when_unset(
    destination_config: DestinationTestConfiguration,
) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="lancedb_tag_none_" + uniq_id(),
        destination=destination_config.destination_factory(),
        dataset_name="commit_tag_" + uniq_id(),
    )
    try:
        assert_load_info(pipeline.run(alpha(rows=1)))

        with pipeline.destination_client() as client:
            client = cast("LanceDBClient", client)
            assert tags_of(client, "alpha") == {}
    finally:
        with pipeline.destination_client() as client:
            client.drop_storage()


@pytest.mark.parametrize(
    "destination_config",
    LANCEDB_ONLY_CONFS,
    ids=lambda x: x.name,
)
def test_commit_tag_moves_when_reused(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Loading again under an existing tag moves it to the version of the newer load."""
    pipeline = dlt.pipeline(
        pipeline_name="lancedb_tag_reuse_" + uniq_id(),
        destination=tagged(destination_config, "rolling"),
        dataset_name="commit_tag_" + uniq_id(),
    )
    try:
        assert_load_info(pipeline.run(alpha(rows=2)))
        with pipeline.destination_client() as client:
            client = cast("LanceDBClient", client)
            first_version = tags_of(client, "alpha")["rolling"]["version"]

        assert_load_info(
            pipeline.run(
                alpha(rows=1, offset=100), destination=tagged(destination_config, "rolling")
            )
        )

        with pipeline.destination_client() as client:
            client = cast("LanceDBClient", client)
            tags = tags_of(client, "alpha")
            assert set(tags) == {"rolling"}
            assert tags["rolling"]["version"] > first_version
            assert rows_at_tag(client, "alpha", "rolling") == 3
    finally:
        with pipeline.destination_client() as client:
            client.drop_storage()


# a configured database is shared with every other dataset, so this must not run beside them
@pytest.mark.serial
@pytest.mark.parametrize(
    "destination_config",
    LANCEDB_ONLY_CONFS,
    ids=lambda x: x.name,
)
def test_commit_tag_leaves_foreign_dataset_alone(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Only tables `dlt` owns can be tagged, which matters in a database a foreign dataset shares."""
    pinned_database = "pinned_tag_" + uniq_id()
    os.environ["DESTINATION__LANCEDB__CREDENTIALS__DATABASE"] = pinned_database
    pipeline = dlt.pipeline(
        pipeline_name="lancedb_tag_foreign_" + uniq_id(),
        destination=tagged(destination_config, "load_1"),
    )
    with pipeline.destination_client() as client:
        client = cast("LanceDBClient", client)
        client.initialize_storage()
        foreign_table_name = "foreign_" + uniq_id()
        client.db_client.create_table(foreign_table_name, data=pa.table({"id": pa.array([1])}))
        try:
            assert_load_info(pipeline.run(alpha(rows=1)))

            assert "load_1" in tags_of(client, "alpha")
            assert tags_of(client, foreign_table_name) == {}
            assert foreign_table_name not in client.list_owned_table_names()
        finally:
            client.db_client.drop_table(foreign_table_name)
            client.drop_storage()


@pytest.mark.parametrize(
    "destination_config",
    LANCEDB_ONLY_CONFS,
    ids=lambda x: x.name,
)
def test_rollback_to_commit_tag_reloads_different_data(
    destination_config: DestinationTestConfiguration,
) -> None:
    """A tag is a rollback target: restore it, then load different data on top of the restored state."""
    pipeline = dlt.pipeline(
        pipeline_name="lancedb_rollback_" + uniq_id(),
        destination=tagged(destination_config, "before_mistake"),
        dataset_name="commit_tag_" + uniq_id(),
    )
    try:
        assert_load_info(pipeline.run(alpha(rows=2)))
        assert read_over_sql(pipeline, "alpha", expected_rows=2).num_rows == 2

        # a load we regret, under a tag of its own
        assert_load_info(
            pipeline.run(
                alpha(rows=3, offset=100), destination=tagged(destination_config, "mistake")
            )
        )
        assert read_over_sql(pipeline, "alpha", expected_rows=5).num_rows == 5

        restored = rollback_to_commit_tag(pipeline.dataset(), "before_mistake")
        assert "alpha" in restored
        assert "_dlt_loads" in restored

        # the regretted rows are gone, over SQL as well as through the managed client
        assert read_over_sql(pipeline, "alpha", expected_rows=2).num_rows == 2

        # loading again continues from the restored state
        assert_load_info(
            pipeline.run(
                alpha(rows=1, offset=200), destination=tagged(destination_config, "after_rollback")
            )
        )
        landed = read_over_sql(pipeline, "alpha", expected_rows=3)
        assert sorted(landed.to_pydict()["id"]) == [0, 1, 200]
    finally:
        with pipeline.destination_client() as client:
            client.drop_storage()


def test_post_commit_tag_failure_tells_the_user_what_to_create() -> None:
    """The loads table is tagged after the commit, so a failure there must be actionable."""
    error = LanceDBCommitTagNotApplied("nightly", "_dlt_loads", 7, "analytics", "1234.5678")

    message = str(error)
    assert "nightly" in message
    assert "_dlt_loads" in message
    assert "version 7" in message
    # the remediation must be runnable as written
    assert 'db.open_table("_dlt_loads").tags.create("nightly", 7)' in message
    assert 'lancedb.connect("db://analytics"' in message
    assert "1234.5678" in message


@pytest.mark.parametrize(
    "destination_config",
    LANCEDB_ONLY_CONFS,
    ids=lambda x: x.name,
)
def test_tag_failure_before_the_commit_propagates(
    destination_config: DestinationTestConfiguration,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pre-commit tagging failure aborts the load, which `dlt` retries, so it must not be wrapped."""
    from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient

    def fail(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("tagging is down")

    pipeline = dlt.pipeline(
        pipeline_name="lancedb_tag_fails_" + uniq_id(),
        destination=tagged(destination_config, "load_1"),
        dataset_name="commit_tag_" + uniq_id(),
    )
    try:
        # retries are exhausted quickly, the wrapped call keeps raising
        monkeypatch.setattr(LanceDBClient, "_tag_table_version", fail)
        with pytest.raises(Exception) as failure:
            pipeline.run(alpha(rows=1))
        assert not isinstance(failure.value, LanceDBCommitTagNotApplied)
        assert "tagging is down" in str(failure.value)
    finally:
        monkeypatch.undo()
        with pipeline.destination_client() as client:
            client.drop_storage()
