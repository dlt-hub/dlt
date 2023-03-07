import pytest

from dlt.common.normalizers.naming.duck_case import NamingConvention
from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeNamingConvention


@pytest.fixture
def naming_unlimited() -> NamingConvention:
    return NamingConvention()


def test_normalize_identifier(naming_unlimited: NamingConvention) -> None:
    assert naming_unlimited.normalize_identifier("+1") == "+1"
    assert naming_unlimited.normalize_identifier("-1") == "-1"


def test_alphabet_reduction(naming_unlimited: NamingConvention) -> None:
    assert naming_unlimited.normalize_identifier(NamingConvention._REDUCE_ALPHABET[0]) == NamingConvention._REDUCE_ALPHABET[1]


def test_duck_snake_case_compat(naming_unlimited: NamingConvention) -> None:
    snake_unlimited = SnakeNamingConvention()
    # same reduction duck -> snake
    assert snake_unlimited.normalize_identifier(NamingConvention._REDUCE_ALPHABET[0]) == NamingConvention._REDUCE_ALPHABET[1]
    # but there are differences in the reduction
    assert naming_unlimited.normalize_identifier(SnakeNamingConvention._REDUCE_ALPHABET[0]) != snake_unlimited.normalize_identifier(SnakeNamingConvention._REDUCE_ALPHABET[0])
