import dlt
from dlt.destinations.dataset.ibis_relation import ReadableIbisRelation
import narwhals as nw

from dlt.sources._single_file_templates.fruitshop_pipeline import (
    fruitshop as fruitshop_source,
)
from tests.load.transformations.utils import (
    transformation_configs,
    setup_transformation_pipelines,
)


@dlt.transformation
def customers_by_city(dataset: dlt.Dataset):
    customers_ibis = dataset.table("customers")._ibis_object
    t = (
        nw.from_native(customers_ibis)
        .group_by(nw.col("city"))
        .agg(nw.col("name").count().alias("count"))
        .to_native()
    )
    return ReadableIbisRelation(readable_dataset=dataset, ibis_object=t)


"""We could rewrite the above as follow and handle the automatically rest behind the scene.
@dlt.transformation
def customers_by_city(customers):
    return (
        customers
        .group_by(nw.col('city'))
        .agg(nw.col('name').count().alias('count'))
    )
"""


def test_narwhals_transform() -> None:
    duckdb_conf = next(
        conf for conf in transformation_configs() if conf.destination_type == "duckdb"
    )
    fruit_p, dest_p = setup_transformation_pipelines(duckdb_conf)
    fruit_p.run(fruitshop_source())

    dest_p.run(customers_by_city(fruit_p.dataset()))
    rows = dest_p.dataset().table("customers_by_city").df().to_dict(orient="records")
    expected_count = {
        "new york": 1,
        "rome": 1,
        "berlin": 4,
        "paris": 1,
        "mumbai": 1,
        "london": 1,
        "tokyo": 1,
        "sydney": 1,
        "shanghai": 1,
        "madrid": 1,
    }
    for row in rows:
        assert row["count"] == expected_count[row["city"]]