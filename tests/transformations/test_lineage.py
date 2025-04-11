import pytest

import dlt
from dlt.sources._single_file_templates.fruitshop_pipeline import (
    fruitshop as fruitshop_source,
)
from dlt.transformations import lineage
from tests.utils import autouse_test_storage


@pytest.fixture
def dataset(autouse_test_storage):
    pipeline = dlt.pipeline(pipeline_name="test_fruitshop", destination="duckdb", dev_mode=True)
    pipeline.run(fruitshop_source())
    yield pipeline.dataset(dataset_type="default")


def test_build_scope(dataset):
    sql_query = "SELECT AVG(id) as mean_id, name, CAST(LEN(name) as DOUBLE) FROM customers"
    expected_schema = {
        "mean_id": {"name": "mean_id", "data_type": "double"},
        "name": {"name": "name", "data_type": "text", "nullable": True},
        "_col_2": {"name": "_col_2", "data_type": "double"},  # anonymous columns
    }

    dialect: str = dataset._destination.capabilities().sqlglot_dialect
    sqlglot_schema = lineage.create_sqlglot_schema(dataset, dialect)
    manual_schema = lineage.compute_columns_schema(sql_query, sqlglot_schema, dialect)

    relation_schema = dataset(sql_query).compute_columns_schema()

    assert expected_schema == manual_schema == relation_schema
