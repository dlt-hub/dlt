"""Cross-dataset joins that attach a foreign dataset into the primary duckdb connection."""

import os
from typing import Any

import pytest

import dlt
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.utils import uniq_id
from dlt.extract.hints import make_hints
from dlt.pipeline.exceptions import PipelineStepFailed

from tests.utils import get_test_storage_root

USERS = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
PURCHASES = [
    {"purchase_id": 10, "user_id": 1, "sku": "W-001"},
    {"purchase_id": 11, "user_id": 1, "sku": "G-001"},
    {"purchase_id": 12, "user_id": 2, "sku": "W-001"},
    {"purchase_id": 13, "user_id": 99, "sku": "X-999"},  # orphan, dropped by INNER JOIN
]


def _duckdb_destination(name: str, **caps: Any) -> Any:
    return dlt.destinations.duckdb(
        os.path.join(get_test_storage_root(), f"{name}_{uniq_id()}.duckdb"), **caps
    )


def _purchases_pipeline(destination: Any, **run_kwargs: Any) -> dlt.Pipeline:
    pipeline = dlt.pipeline(
        "attach_purchases_" + uniq_id(), destination=destination, dataset_name="ds_purchases"
    )
    pipeline.run(PURCHASES, table_name="purchases", **run_kwargs)
    return pipeline


@pytest.fixture
def users_pipeline() -> dlt.Pipeline:
    """Primary pipeline holding `users` in its own duckdb database."""
    pipeline = dlt.pipeline(
        "attach_users_" + uniq_id(),
        destination=_duckdb_destination("users"),
        dataset_name="ds_users",
    )
    pipeline.run(USERS, table_name="users")
    return pipeline


def _join_purchases(primary: dlt.Pipeline, foreign: dlt.Pipeline) -> dlt.Relation:
    return (
        primary.dataset()
        .table("users")
        .join(foreign.dataset().table("purchases"), on="users.id = purchases.user_id")
    )


def _assert_joined(relation: dlt.Relation) -> None:
    df = relation.order_by("purchases__purchase_id").df()
    assert list(df["name"]) == ["Alice", "Alice", "Bob"]
    assert list(df["purchases__sku"]) == ["W-001", "G-001", "W-001"]


def test_attach_foreign_duckdb_eager(users_pipeline: dlt.Pipeline) -> None:
    foreign = _purchases_pipeline(_duckdb_destination("purchases"))
    joined = _join_purchases(users_pipeline, foreign)

    # the foreign database is reached under a prefixed ATTACH catalog, not a bare schema
    (info,) = joined._attach_infos()
    assert info["attach_type"] == "duckdb"
    assert info["alias"] == "attach_ds_purchases"
    assert "READ_ONLY" in info["statements"][0]["sql"]
    assert '"attach_ds_purchases"."ds_purchases"."purchases"' in joined.to_sql()

    _assert_joined(joined)


def test_attach_foreign_filesystem_eager(users_pipeline: dlt.Pipeline) -> None:
    """A filesystem foreign dataset is attached as a catalog of duckdb views over its files."""
    bucket_url = FilesystemConfiguration.make_file_url(
        os.path.join(get_test_storage_root(), "purchases_" + uniq_id())
    )
    foreign = _purchases_pipeline(
        dlt.destinations.filesystem(bucket_url), loader_file_format="parquet"
    )
    joined = _join_purchases(users_pipeline, foreign)

    assert [info["attach_type"] for info in joined._attach_infos()] == ["duckdb"]
    _assert_joined(joined)


def test_attach_foreign_lazy_model_round_trip(users_pipeline: dlt.Pipeline) -> None:
    """The join is serialized into a `.model` file and re-attached by the load job."""
    foreign = _purchases_pipeline(_duckdb_destination("purchases"))
    joined = _join_purchases(users_pipeline, foreign)

    @dlt.resource(table_name="user_purchases")
    def joined_purchases() -> Any:
        yield dlt.mark.with_hints(joined, hints=make_hints(columns=joined.columns_schema))

    users_pipeline.run(joined_purchases(), loader_file_format="model")

    _assert_joined(users_pipeline.dataset().table("user_purchases"))


def test_model_cannot_be_written_at_another_data_location(users_pipeline: dlt.Pipeline) -> None:
    """A model attaches the datasets it joins, never the one it selects from, so it can only be
    written where that data already is.
    """
    foreign = _purchases_pipeline(_duckdb_destination("purchases"))
    joined = _join_purchases(users_pipeline, foreign)

    # the foreign side is attached under its alias, the primary is selected from by bare schema
    assert [info["alias"] for info in joined._attach_infos()] == ["attach_ds_purchases"]
    assert '"ds_users"."users"' in joined.to_sql()

    output = dlt.pipeline(
        "attach_output_" + uniq_id(),
        destination=_duckdb_destination("output"),
        dataset_name="ds_output",
    )
    output.run(USERS, table_name="users")

    primary_config = users_pipeline.dataset().destination_client.config
    output_config = output.dataset().destination_client.config
    # the join reads across the two databases by attaching, but a model may not be written across
    assert output_config.can_read_from(primary_config)
    assert output_config.can_write_from(primary_config) is False
    # where the data already is, both hold
    assert primary_config.can_write_from(primary_config)

    # what the predicate protects against: the model selects `ds_users` from a database
    # that has no such schema
    @dlt.resource(table_name="user_purchases")
    def joined_purchases() -> Any:
        yield dlt.mark.with_hints(joined, hints=make_hints(columns=joined.columns_schema))

    with pytest.raises(PipelineStepFailed):
        output.run(joined_purchases(), loader_file_format="model")


def test_attach_foreign_casefold(users_pipeline: dlt.Pipeline) -> None:
    """A foreign dataset binds its identifiers with its own casefolding rules."""
    foreign = _purchases_pipeline(
        _duckdb_destination(
            "purchases", casefold_identifier=str.upper, has_case_sensitive_identifiers=True
        )
    )
    joined = _join_purchases(users_pipeline, foreign)

    sql = joined.to_sql()
    # foreign schema, table and columns fold upper while the primary side keeps its casing;
    # the ATTACH alias belongs to the primary namespace so it folds with the primary rules
    assert '"attach_ds_purchases"."DS_PURCHASES"."PURCHASES"' in sql
    assert '"ds_users"."users"' in sql
    assert '"USER_ID"' in sql
    # foreign-folded output columns keep their logical names
    assert '"purchases__purchase_id"' in sql

    _assert_joined(joined)
