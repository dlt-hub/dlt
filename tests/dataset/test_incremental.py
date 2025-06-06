import ibis

import dlt


@dlt.resource(primary_key="id", write_disposition="merge")
def customers():
    yield from [
        {"id": 1, "name": "andrea", "city": "berlin"},
        {"id": 2, "name": "violetta", "city": "berlin"},
        {"id": 3, "name": "marcin", "city": "kreuzberg"},
        {"id": 4, "name": "dave", "city": "neukölln"},
    ]

@dlt.resource
def purchases(load_idx):
    records = {
        0: [
            {"date": "2025-05-29", "customer_id": 3, "cost": 5},
            {"date": "2025-05-29", "customer_id": 3, "cost": 5},
            {"date": "2025-05-29", "customer_id": 4, "cost": 2},
            {"date": "2025-05-29", "customer_id": 4, "cost": 2},
            {"date": "2025-05-29", "customer_id": 2, "cost": 4},
        ],
        1: [
            {"date": "2025-05-30", "customer_id": 1, "cost": 6},
            {"date": "2025-05-30", "customer_id": 2, "cost": 3},
            {"date": "2025-05-30", "customer_id": 2, "cost": 5},
            {"date": "2025-05-30", "customer_id": 3, "cost": 5},
            {"date": "2025-05-30", "customer_id": 2, "cost": 4},
            {"date": "2025-05-30", "customer_id": 1, "cost": 2},
        ],
        2: [
            {"date": "2025-06-01", "customer_id": 2, "cost": 7},
            {"date": "2025-06-01", "customer_id": 3, "cost": 8},
            {"date": "2025-06-01", "customer_id": 2, "cost": 4},
            {"date": "2025-06-01", "customer_id": 4, "cost": 6},
            {"date": "2025-06-01", "customer_id": 1, "cost": 6},
            {"date": "2025-06-01", "customer_id": 4, "cost": 1},
            {"date": "2025-06-01", "customer_id": 3, "cost": 7},
            {"date": "2025-06-01", "customer_id": 2, "cost": 8},
        ],
    }

    yield from records[load_idx]


# TODO accept `.incremental("TABLE.COLUMN")`
# TODO write tests that go through the mock SQLGlot backend
def test_incremental_golden_path(autouse_test_storage) -> None:
    """Test the main intended way to use incremental transformations

    Considerations:
    - 1 pipeline for resources Extract & Load (EL)
    - 1 pipeline for Transformations (T)
    - One-to-one mapping between `pipeline - dataset - schema name`
    - The pipelines can share the same destination
    - The incremental range for Transformation is set at `pipeline.run()`
    """
    @dlt.transformation
    def full_t(dataset: dlt.Dataset):
        customers = dataset.table("customers")
        purchases = dataset.table("purchases")

        query = (
            customers.join(purchases, customers.id == purchases.customer_id)
            .group_by("city")
            .aggregate(avg_per_city=ibis._.cost.sum())
        )
        return query.query()
    
    @dlt.transformation
    def incremental_t(
        dataset: dlt.Dataset,
        load_id = dlt.sources.incremental("_dlt_load_id"),
    ):
        customers = dataset.table("customers")
        incremental_purchases = dataset.table("purchases", incremental=load_id)

        query = (
            customers
            .join(incremental_purchases, customers.id == incremental_purchases.customer_id)
            .group_by("city")
            .aggregate(avg_per_city=ibis._.cost.sum())
        )

        return query.query()

    destination = dlt.destinations.duckdb("incremental_golden.duckdb")
    el_pipeline = dlt.pipeline("el", destination=destination)
    t_pipeline = dlt.pipeline("t", destination=destination)
    el_dataset = el_pipeline.dataset(dataset_type="ibis")
    t_dataset = t_pipeline.dataset(dataset_type="ibis")

    # EL runs twice (0, 1), T runs once (0)
    el_pipeline.run([customers(), purchases(0)])
    el_pipeline.run([customers(), purchases(1)])
    t_pipeline.run([full_t(el_dataset), incremental_t(el_dataset)])

    full_results_0 = {row[0]: row[1] for row in t_dataset.table("full_t").fetchall()}
    incremental_results_0 = {row[0]: row[1] for row in t_dataset.table("incremental_t").fetchall()}

    # we expect `full` and `incremental` to match on first T run
    assert (
        full_results_0
        == incremental_results_0
        == {'kreuzberg': 15, 'berlin': 24, 'neukölln': 4}
    )

    # EL runs once (2), T runs once (1)
    el_pipeline.run([customers(), purchases(2)])
    t_pipeline.run([full_t(el_dataset), incremental_t(el_dataset)])

    full_results_1 = {row[0]: row[1] for row in t_dataset.table("full_t").fetchall()}
    incremental_results_1 = {row[0]: row[1] for row in t_dataset.table("incremental_t").fetchall()}

    # we expect `full` and `incremental` to diverge on subsequent T run
    assert full_results_1 != incremental_results_1
    assert full_results_1 == {'neukölln': 11, 'berlin': 49, 'kreuzberg': 30}
    assert incremental_results_1 != {'kreuzberg': 15, 'neukölln': 7, 'berlin': 25}
