import dataclasses
from pathlib import Path

import dlt
from dlt.common.schema import Schema
from dlt._dataset.data_generation import generate_data, populate_dataset, table_schema_to_dataclass

from tests.common.utils import load_yml_case


def test_table_schema_to_dataclass():
    case = load_yml_case("schemas/eth/ethereum_schema_v5")
    schema = Schema.from_dict(case, bump_version=True)  # type: ignore[arg-type]
    table_name = "blocks"
    table_schema = schema.tables[table_name]

    dataclass = table_schema_to_dataclass(table_schema)

    assert dataclasses.is_dataclass(dataclass)
    assert dataclass.__name__ == f"_{table_name.capitalize()}"
    assert all(col in dataclass.__dict__ for col in table_schema["columns"])


def test_generate_data():
    case = load_yml_case("schemas/eth/ethereum_schema_v5")
    schema = Schema.from_dict(case, bump_version=True)  # type: ignore[arg-type]
    size = 10
    table_name = "blocks"
    expected_model_name = f"_{table_name.capitalize()}"

    data = generate_data(schema, table_name, size)

    assert len(data) == 10
    assert data[0].__class__.__name__ == expected_model_name


def test_populate_destination(tmp_path: Path):
    case = load_yml_case("schemas/eth/ethereum_schema_v5")
    schema = Schema.from_dict(case, bump_version=True)  # type: ignore[arg-type]
    table_name = "blocks"
    size = 10
    dataset = dlt.dataset(destination="duckdb", schema=schema)

    # TODO assertion on the dataset's schema
    # assert dataset.row_counts(table_names=[table_name]) == 0

    dataset = populate_dataset(dataset, table_name, size)

    # TODO assert schema hasn't changed
    row_counts = dataset.row_counts(table_names=[table_name]).fetchall()
    assert row_counts[0][0] == table_name
    assert row_counts[0][1] == size
    
    assert False
    