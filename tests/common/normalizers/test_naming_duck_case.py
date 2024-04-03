import pytest

from dlt.common.normalizers.naming.duck_case import NamingConvention
from dlt.common.normalizers.naming.snake_case import (
    NamingConvention as SnakeNamingConvention,
)


@pytest.fixture
def naming_unlimited() -> NamingConvention:
    return NamingConvention()


def test_normalize_identifier(naming_unlimited: NamingConvention) -> None:
    assert naming_unlimited.normalize_identifier("+1") == "+1"
    assert naming_unlimited.normalize_identifier("-1") == "-1"
    assert naming_unlimited.normalize_identifier("1-1") == "1-1"
    assert naming_unlimited.normalize_identifier("🦚Peacock") == "🦚Peacock"
    assert naming_unlimited.normalize_identifier("🦚🦚Peacocks") == "🦚🦚Peacocks"
    assert naming_unlimited.normalize_identifier("🦚🦚peacocks") == "🦚🦚peacocks"
    # non latin alphabets
    assert (
        naming_unlimited.normalize_identifier("Ölübeµrsईउऊऋऌऍऎएc⇨usǁs⛔lÄnder")
        == "Ölübeµrsईउऊऋऌऍऎएc⇨usǁs⛔lÄnder"
    )


def test_alphabet_reduction(naming_unlimited: NamingConvention) -> None:
    assert naming_unlimited.normalize_identifier('A\nB"C\rD') == "A_B_C_D"
