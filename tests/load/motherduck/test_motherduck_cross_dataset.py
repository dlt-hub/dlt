"""Cross-dataset joins between MotherDuck and a DuckLake, executed on the destination.

A MotherDuck connection attaches everything but another MotherDuck database locally, so a local
DuckLake is read by the client and its rows are shipped to MotherDuck as the query runs.
"""

from pathlib import Path
from typing import Any

import pytest

import dlt
from dlt.common.utils import uniq_id
from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials
from dlt.extract.hints import make_hints

from tests.dataset.utils import crm, inventory
from tests.load.utils import DestinationTestConfiguration, destinations_configs


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["motherduck"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "motherduck_is_primary", [True, False], ids=["motherduck-primary", "ducklake-primary"]
)
def test_attach_join_motherduck_and_ducklake(
    destination_config: DestinationTestConfiguration,
    motherduck_is_primary: bool,
    tmp_path: Path,
) -> None:
    suffix = uniq_id()

    ducklake = dlt.pipeline(
        "attach_lake_" + suffix,
        # the lake gets its own catalog and storage: the default ones are shared between tests
        destination=dlt.destinations.ducklake(
            credentials=DuckLakeCredentials(
                catalog=f"sqlite:///{tmp_path / 'lake_catalog.sqlite'}",
                storage=str(tmp_path / "lake_data"),
            )
        ),
        dataset_name="ds_lake_" + suffix,
    )

    motherduck = destination_config.setup_pipeline(
        "attach_md_" + suffix, dataset_name="ds_md_" + suffix
    )

    primary, foreign = (motherduck, ducklake) if motherduck_is_primary else (ducklake, motherduck)
    primary.run(crm(0))
    foreign.run(inventory(), **destination_config.run_kwargs)

    joined = (
        primary.dataset()
        .table("users")
        .join(foreign.dataset().table("purchases"), on="users.id = purchases.user_id")
    )
    # attaching MotherDuck is its own mechanism; a DuckLake is a plain duckdb attach
    expected_attach_type = "duckdb" if motherduck_is_primary else "motherduck"
    assert [info["attach_type"] for info in joined._attach_infos()] == [expected_attach_type]

    # the foreign dataset is attached and the join runs on the primary, no data is pulled here
    @dlt.resource(table_name="user_purchases")
    def joined_purchases() -> Any:
        yield dlt.mark.with_hints(joined, hints=make_hints(columns=joined.columns_schema))

    primary.run(joined_purchases(), loader_file_format="model")

    df = primary.dataset().table("user_purchases").order_by("purchases__purchase_id").df()
    # orphan user_id=99 dropped by INNER
    assert list(df["name"]) == ["Alice", "Alice", "Bob"]
    assert list(df["purchases__sku"]) == ["W-001", "G-001", "W-001"]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["motherduck"]),
    ids=lambda x: x.name,
)
def test_motherduck_foreign_dataset_needs_no_attach(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Datasets sharing a token are reachable without ATTACH: MotherDuck attaches the workspace."""
    suffix = uniq_id()
    primary = destination_config.setup_pipeline(
        "attach_md_p_" + suffix, dataset_name="ds_md_p_" + suffix
    )
    foreign = destination_config.setup_pipeline(
        "attach_md_f_" + suffix, dataset_name="ds_md_f_" + suffix
    )
    primary.run(crm(0))
    foreign.run(inventory())

    foreign_dataset = foreign.dataset()
    joined = (
        primary.dataset()
        .table("users")
        .join(foreign_dataset.table("purchases"), on="users.id = purchases.user_id")
    )
    assert joined._attach_infos() == []
    # both sides qualify with the real catalog, no attach alias is introduced
    sql = joined.to_sql()
    assert f'"{foreign_dataset.sql_client.dataset_name}"."purchases"' in sql
    assert "attach_" not in sql

    df = joined.order_by("purchase_id").df()
    assert list(df["purchases__sku"]) == ["W-001", "G-001", "W-001"]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["motherduck"]),
    ids=lambda x: x.name,
)
def test_motherduck_rejects_foreign_motherduck_account(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Another MotherDuck account is unreachable: its token cannot be set on a live connection."""
    suffix = uniq_id()
    primary = destination_config.setup_pipeline(
        "attach_md_acct_" + suffix, dataset_name="ds_md_acct_" + suffix
    )
    primary.run(crm(0))

    # a schema is passed so resolving the foreign dataset needs no connection
    other_account = dlt.dataset(
        dlt.destinations.motherduck(credentials="md:other_db?motherduck_token=NOT_OUR_TOKEN"),
        "ds_other_" + suffix,
        schema=primary.default_schema,
    )
    # a MotherDuck connection cannot attach another account, and neither can the reverse
    with pytest.raises(ValueError, match="cannot reach dataset") as reject:
        primary.dataset().table("users").join(
            other_account.table("products"), on="users.id = products.product_id"
        )
    # the two accounts are named apart by their token digests, which no credentials display shows
    assert "Materialize" in str(reject.value)
    assert "NOT_OUR_TOKEN" not in str(reject.value)
