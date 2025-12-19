# NOTE: this module only tests `s3_tables` specific logic; logic inherited from `snake_case` (e.g. lowercasing)
# or the base naming convention (e.g. shortening) is assumed to be tested elsewhere

import pytest

from dlt.common.normalizers.naming.s3_tables import (
    S3_TABLES_MAX_IDENTIFIER_LENGTH,
    NamingConvention,
)


@pytest.fixture
def naming_unlimited() -> NamingConvention:
    return NamingConvention()


def test_max_length_enforcement(naming_unlimited: NamingConvention) -> None:
    assert naming_unlimited.max_length == S3_TABLES_MAX_IDENTIFIER_LENGTH
    with pytest.raises(ValueError):
        _ = NamingConvention(max_length=S3_TABLES_MAX_IDENTIFIER_LENGTH + 1)


def test_normalize_table_identifier(naming_unlimited: NamingConvention) -> None:
    # removes leading underscores
    assert naming_unlimited.normalize_table_identifier("_foo") == "foo"
    assert naming_unlimited.normalize_table_identifier("__foo") == "foo"
