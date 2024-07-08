import pytest

from dlt.common import Decimal

from tests.pipeline.utils import assert_load_info
from tests.load.utils import destinations_configs, DestinationTestConfiguration

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_numeric_types(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline("test_bigquery_numeric_types", dev_mode=True)

    columns = [
        {"name": "col_big_numeric", "data_type": "decimal", "precision": 47, "scale": 9},
        {"name": "col_numeric", "data_type": "decimal", "precision": 38, "scale": 9},
    ]

    data = [
        {
            # Valid BIGNUMERIC and NUMERIC values
            "col_big_numeric": Decimal("12345678901234567890123456789012345678.123456789"),
            "col_numeric": Decimal("12345678901234567890123456789.123456789"),
        },
    ]

    info = pipeline.run(iter(data), table_name="big_numeric", columns=columns)  # type: ignore[arg-type]
    assert_load_info(info)

    with pipeline.sql_client() as client:
        with client.execute_query("SELECT col_big_numeric, col_numeric FROM big_numeric;") as q:
            row = q.fetchone()
            assert row[0] == data[0]["col_big_numeric"]
            assert row[1] == data[0]["col_numeric"]
