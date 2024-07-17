import pytest
from typing import Type
from dlt.common.normalizers.naming import NamingConvention, sql_ci_v1, sql_cs_v1

ALL_NAMING_CONVENTIONS = {sql_ci_v1.NamingConvention, sql_cs_v1.NamingConvention}


@pytest.mark.parametrize("convention", ALL_NAMING_CONVENTIONS)
def test_normalize_identifier(convention: Type[NamingConvention]) -> None:
    naming = convention()
    assert naming.normalize_identifier("event_value") == "event_value"
    assert naming.normalize_identifier("event value") == "event_value"
    assert naming.normalize_identifier("event-.!:*<>value") == "event_value"
    # prefix leading digits
    assert naming.normalize_identifier("1event_n'") == "_1event_n"
    # remove trailing underscores
    assert naming.normalize_identifier("123event_n'") == "_123event_n"
    # contract underscores
    assert naming.normalize_identifier("___a___b") == "_a_b"
    # trim spaces
    assert naming.normalize_identifier(" small love potion ") == "small_love_potion"

    # special characters converted to _
    assert naming.normalize_identifier("+-!$*@#=|:") == "_"
    # leave single underscore
    assert naming.normalize_identifier("_") == "_"
    # some other cases
    assert naming.normalize_identifier("+1") == "_1"
    assert naming.normalize_identifier("-1") == "_1"


def test_case_sensitive_normalize() -> None:
    naming = sql_cs_v1.NamingConvention()
    # all lowercase and converted to snake
    assert naming.normalize_identifier("123BaNaNa") == "_123BaNaNa"
    # consecutive capital letters
    assert naming.normalize_identifier("BANANA") == "BANANA"
    assert naming.normalize_identifier("BAN_ANA") == "BAN_ANA"
    assert naming.normalize_identifier("BANaNA") == "BANaNA"
    # handling spaces
    assert naming.normalize_identifier("Small Love Potion") == "Small_Love_Potion"
    assert naming.normalize_identifier(" Small Love Potion ") == "Small_Love_Potion"


def test_case_insensitive_normalize() -> None:
    naming = sql_ci_v1.NamingConvention()
    # all lowercase and converted to snake
    assert naming.normalize_identifier("123BaNaNa") == "_123banana"
    # consecutive capital letters
    assert naming.normalize_identifier("BANANA") == "banana"
    assert naming.normalize_identifier("BAN_ANA") == "ban_ana"
    assert naming.normalize_identifier("BANaNA") == "banana"
    # handling spaces
    assert naming.normalize_identifier("Small Love Potion") == "small_love_potion"
    assert naming.normalize_identifier(" Small Love Potion ") == "small_love_potion"
