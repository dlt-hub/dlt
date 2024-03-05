import dlt
import faker
import random

from tests.common.utils import json_case_path

data = [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
]

faker.Faker.seed(random.randint(0, 10000))
fakes = faker.Faker("de_DE")


@dlt.source
def users_source():
    return dlt.resource([data], name="users_resource")


@dlt.source
def taxi_demand_source():
    @dlt.resource(primary_key="city")
    def locations():
        for idx in range(10):
            yield {
                "id": idx,
                "address": fakes.address(),
                "city": fakes.city(),
            }

    @dlt.resource(primary_key="id")
    def demand_map():
        for idx in range(10):
            yield {
                "id": idx,
                "city": fakes.city(),
                "demand": fakes.random.randint(0, 10000),
            }

    return [locations, demand_map]


def test_pipeline_load_info_metrics_schema_is_not_chaning() -> None:
    """Test if load info schema is idempotent throughout multiple load cycles

    ## Setup
        We will run the same pipeline with
        1. A single source returning one resource and collect `schema.version_hash`,
        2. Another source returning 2 resources with more complex data and collect `schema.version_hash`,
        3. At last we run both sources,
        4. For each 1. 2. 3. we load `last_extract_info`, `last_normalize_info` and `last_load_info` and collect `schema.version_hash`

    ## Expected
        `version_hash` collected in each stage should remain the same at all times.
    """
    schema = dlt.Schema(name="nice_load_info_schema")
    pipeline = dlt.pipeline(
        pipeline_name="quick_start",
        destination="duckdb",
        dataset_name="mydata",
    )

    users_load_info = pipeline.run(
        users_source(),
        table_name="users",
        primary_key="id",
    )

    schema_hashset = set()
    pipeline.run([users_load_info], table_name="_load_info", schema=schema)

    pipeline.run(
        [pipeline.last_trace.last_normalize_info], table_name="_normalize_info", schema=schema
    )

    pipeline.run([pipeline.last_trace.last_extract_info], table_name="_extract_info", schema=schema)
    schema_hashset.add(pipeline.schemas["nice_load_info_schema"].version_hash)

    taxi_load_info = pipeline.run(
        taxi_demand_source(),
        table_name="taxi_demands",
        primary_key="id",
        write_disposition="replace",
    )

    pipeline.run([taxi_load_info], table_name="_load_info", schema=schema)
    schema_hashset.add(pipeline.schemas["nice_load_info_schema"].version_hash)

    pipeline.run(
        [pipeline.last_trace.last_normalize_info], table_name="_normalize_info", schema=schema
    )
    schema_hashset.add(pipeline.schemas["nice_load_info_schema"].version_hash)

    pipeline.run([pipeline.last_trace.last_extract_info], table_name="_extract_info", schema=schema)
    schema_hashset.add(pipeline.schemas["nice_load_info_schema"].version_hash)

    assert len(schema_hashset) == 1

    users_load_info = pipeline.run(
        users_source(),
        table_name="users",
        primary_key="id",
    )

    taxi_load_info = pipeline.run(
        taxi_demand_source(),
        table_name="taxi_demands",
        primary_key="id",
        write_disposition="replace",
    )

    pipeline.run([users_load_info], table_name="_load_info", schema=schema)
    schema_hashset.add(pipeline.schemas["nice_load_info_schema"].version_hash)

    pipeline.run([taxi_load_info], table_name="_load_info", schema=schema)
    schema_hashset.add(pipeline.schemas["nice_load_info_schema"].version_hash)

    pipeline.run(
        [pipeline.last_trace.last_normalize_info], table_name="_normalize_info", schema=schema
    )
    schema_hashset.add(pipeline.schemas["nice_load_info_schema"].version_hash)

    pipeline.run([pipeline.last_trace.last_extract_info], table_name="_extract_info", schema=schema)
    schema_hashset.add(pipeline.schemas["nice_load_info_schema"].version_hash)

    assert len(schema_hashset) == 1
