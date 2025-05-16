from tests.extract.test_extract import extract_step
from tests.normalize.test_normalize import raw_normalize, extract_items, load_or_create_schema, caps
from tests.load.utils import destinations_configs, DestinationTestConfiguration
from dlt.extract.extract import Extract
from dlt.normalize import Normalize
from dlt.destinations import duckdb
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common import logger
import dlt
import pyarrow as pa

from typing import Optional
from pytest_mock import MockerFixture
import pytest

from concurrent.futures import ThreadPoolExecutor


@pytest.mark.parametrize("is_none", [True, False])
def test_warning_from_arrow_extractor(
    extract_step: Extract, mocker: MockerFixture, is_none: bool
) -> None:
    """
    Test that the ArrowExtractor emits a warning when a pyarrow table is yielded
    with a column (`col1`) that contains only null values.
    """

    @dlt.source()
    def my_source():
        @dlt.resource
        def my_resource():
            col1: list[Optional[str]] = [None, None] if is_none else ["a", "b"]

            table = pa.table(
                {
                    "id": pa.array([1, 2]),
                    "col1": pa.array(col1),
                }
            )

            yield table

        return [my_resource()]

    logger_spy = mocker.spy(logger, "warning")

    extract_step.extract(my_source(), 1, 1)

    if is_none:
        logger_spy.assert_called_once()
        expected_warning = (
            "Null type(s) detected for column(s) ['col1'] in table 'my_resource' of schema"
            " 'my_source'. Unless type hint(s) are provided, the column(s) will not be loaded."
        )
        assert expected_warning in logger_spy.call_args_list[0][0][0]
    else:
        logger_spy.assert_not_called()


@pytest.mark.parametrize("is_none", [True, False])
@pytest.mark.parametrize(
    "caps", [duckdb().capabilities], indirect=True, ids=["json_normalizer_with_duckdb"]
)
def test_warning_from_json_normalizer(
    caps: DestinationCapabilitiesContext,
    raw_normalize: Normalize,
    mocker: MockerFixture,
    is_none: bool,
) -> None:
    """
    Test that the JsonLItemsNormalizer step emits a warning when a resource is yielded
    with a column (`col1`) that contains only null values.
    """

    nested_data_item = {
        "id": 1,
        "children": [
            {"id": 2, "col1": None if is_none else "a"},
            {"id": 3, "col1": None if is_none else "b"},
        ],
    }

    logger_spy = mocker.spy(logger, "warning")

    extract_items(
        raw_normalize.normalize_storage,
        [nested_data_item],
        load_or_create_schema(raw_normalize, "test_schema"),
        "nested_table",
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        raw_normalize.run(pool)

    if is_none:
        logger_spy.assert_called()
        assert logger_spy.call_count == 1
        expected_warning = (
            "Schema coercion could not infer type(s) for column(s) ['col1'] in table"
            " 'nested_table__children' of schema 'test_schema'. Unless type hint(s) are provided,"
            " the column(s) will not be loaded."
        )
        assert expected_warning in logger_spy.call_args_list[0][0][0]
    else:
        logger_spy.assert_not_called()
