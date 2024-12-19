import dlt

from dlt.common.destination.reference import SupportsReadableDataset, SupportsReadableRelation

from functools import reduce


def test_simple_transformation() -> None:
    # load some stuff into items table

    @dlt.resource(table_name="items")
    def items_resource():
        for i in range(10):
            yield {"id": i, "value": i * 2}

    p = dlt.pipeline("test_pipeline", destination="duckdb", dataset_name="test_dataset")
    p.run(items_resource)

    print(p.dataset().items.df())

    @dlt.transformation(table_name="quadrupled_items")
    def simple_transformation(dataset: SupportsReadableDataset) -> SupportsReadableRelation:
        items_table = dataset.items
        return items_table.mutate(quadruple_id=items_table.id * 4).select("id", "quadruple_id")

    @dlt.transformation(table_name="aggregated_items")
    def aggregate_transformation(dataset: SupportsReadableDataset) -> SupportsReadableRelation:
        items_table = dataset.items
        return items_table.aggregate(sum_id=items_table.id.sum(), value_sum=items_table.value.sum())

    # we run two transformations
    p.transform([simple_transformation, aggregate_transformation])

    # check table with quadrupled ids
    assert list(p.dataset().quadrupled_items.df()["quadruple_id"]) == [i * 4 for i in range(10)]

    # check aggregated table for both fields
    assert p.dataset().aggregated_items.fetchone()[0] == reduce(lambda a, b: a + b, range(10))
    assert p.dataset().aggregated_items.fetchone()[1] == (reduce(lambda a, b: a + b, range(10)) * 2)
