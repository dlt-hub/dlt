from typing import Type
import pytest

from dlt.common.normalizers.naming import NamingConvention
from dlt.common.normalizers.naming.snake_case import (
    NamingConvention as SnakeCaseNamingConvention,
)
from dlt.common.normalizers.naming.duck_case import (
    NamingConvention as DuckCaseNamingConvention,
)


@pytest.fixture
def naming_unlimited() -> NamingConvention:
    return SnakeCaseNamingConvention()


def test_normalize_identifier(naming_unlimited: NamingConvention) -> None:
    assert naming_unlimited.normalize_identifier("event_value") == "event_value"
    assert naming_unlimited.normalize_identifier("event value") == "event_value"
    assert naming_unlimited.normalize_identifier("event-.!:*<>value") == "event_x_value"
    # prefix leading digits
    assert naming_unlimited.normalize_identifier("1event_n'") == "_1event_nx"
    assert naming_unlimited.normalize_identifier("123event_n'") == "_123event_nx"
    # all lowercase and converted to snake
    assert naming_unlimited.normalize_identifier("123BaNaNa") == "_123_ba_na_na"
    # consecutive capital letters
    assert naming_unlimited.normalize_identifier("BANANA") == "banana"
    assert naming_unlimited.normalize_identifier("BAN_ANA") == "ban_ana"
    assert naming_unlimited.normalize_identifier("BANaNA") == "ba_na_na"
    # handling spaces
    assert (
        naming_unlimited.normalize_identifier("Small Love Potion")
        == "small_love_potion"
    )
    assert (
        naming_unlimited.normalize_identifier(" Small Love Potion ")
        == "small_love_potion"
    )
    # removes trailing _
    assert naming_unlimited.normalize_identifier("BANANA_") == "bananax"
    assert naming_unlimited.normalize_identifier("BANANA____") == "bananaxxxx"
    # current special characters translation table
    assert naming_unlimited.normalize_identifier("+-!$*@#=|:") == "x_xa_lx"
    # some other cases
    assert naming_unlimited.normalize_identifier("+1") == "x1"
    assert naming_unlimited.normalize_identifier("-1") == "_1"


def test_alphabet_reduction(naming_unlimited: NamingConvention) -> None:
    assert (
        naming_unlimited.normalize_identifier(
            SnakeCaseNamingConvention._REDUCE_ALPHABET[0]
        )
        == SnakeCaseNamingConvention._REDUCE_ALPHABET[1]
    )


def test_normalize_path(naming_unlimited: NamingConvention) -> None:
    assert naming_unlimited.normalize_path("small_love_potion") == "small_love_potion"
    assert (
        naming_unlimited.normalize_path("small__love__potion") == "small__love__potion"
    )
    assert naming_unlimited.normalize_path("Small_Love_Potion") == "small_love_potion"
    assert (
        naming_unlimited.normalize_path("Small__Love__Potion") == "small__love__potion"
    )
    assert naming_unlimited.normalize_path("Small Love Potion") == "small_love_potion"
    assert naming_unlimited.normalize_path("Small  Love  Potion") == "small_love_potion"


def test_normalize_non_alpha_single_underscore() -> None:
    assert SnakeCaseNamingConvention._RE_NON_ALPHANUMERIC.sub("_", "-=!*") == "_"
    assert SnakeCaseNamingConvention._RE_NON_ALPHANUMERIC.sub("_", "1-=!0*-") == "1_0_"
    assert (
        SnakeCaseNamingConvention._RE_NON_ALPHANUMERIC.sub("_", "1-=!_0*-") == "1__0_"
    )


@pytest.mark.parametrize(
    "convention", (SnakeCaseNamingConvention, DuckCaseNamingConvention)
)
def test_normalize_break_path(convention: Type[NamingConvention]) -> None:
    naming_unlimited = convention()
    assert naming_unlimited.break_path("A__B__C") == ["A", "B", "C"]
    # what if path has _a and _b which valid normalized idents
    assert naming_unlimited.break_path("_a___b__C___D") == ["_a", "_b", "C", "_D"]
    # skip empty identifiers from path
    assert naming_unlimited.break_path("_a_____b") == ["_a", "_b"]
    assert naming_unlimited.break_path("_a____b") == ["_a", "b"]
    assert naming_unlimited.break_path("_a__  \t\r__b") == ["_a", "b"]


@pytest.mark.parametrize(
    "convention", (SnakeCaseNamingConvention, DuckCaseNamingConvention)
)
def test_normalize_make_path(convention: Type[NamingConvention]) -> None:
    naming_unlimited = convention()
    assert naming_unlimited.make_path("A", "B") == "A__B"
    assert naming_unlimited.make_path("_A", "_B") == "_A___B"
    assert naming_unlimited.make_path("_A", "", "_B") == "_A___B"
    assert naming_unlimited.make_path("_A", "\t\n ", "_B") == "_A___B"


def test_normalizes_underscores(naming_unlimited: NamingConvention) -> None:
    assert (
        naming_unlimited.normalize_identifier("event__value_value2____")
        == "event_value_value2xxxx"
    )
    assert (
        naming_unlimited.normalize_path("e_vent__value_value2___")
        == "e_vent__value_value2__x"
    )
