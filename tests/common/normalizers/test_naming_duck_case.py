import pytest

from dlt.common.normalizers.naming.duck_case import NamingConvention


@pytest.fixture
def naming_unlimited() -> NamingConvention:
    return NamingConvention()


def test_normalize_identifier(naming_unlimited: NamingConvention) -> None:
    assert naming_unlimited.normalize_identifier("+1") == "+1"
    assert naming_unlimited.normalize_identifier("-1") == "-1"