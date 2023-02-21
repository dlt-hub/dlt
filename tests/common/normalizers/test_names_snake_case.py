import pytest

from dlt.common.normalizers.names.snake_case import normalize_identifier, normalize_path, normalize_make_dataset_name, RE_NON_ALPHANUMERIC
from dlt.common.schema.exceptions import InvalidDatasetName


def test_normalize_identifier() -> None:
    assert normalize_identifier("event_value") == "event_value"
    assert normalize_identifier("event value") == "event_value"
    assert normalize_identifier("event-.!:<>value") == "event_value"
    # prefix leading digits
    assert normalize_identifier("1event_n'") == "_1event_n_"
    assert normalize_identifier("123event_n'") == "_123event_n_"
    # all lowercase and converted to snake
    assert normalize_identifier("123BaNaNa") == "_123_ba_na_na"
    # consecutive capital letters
    assert normalize_identifier("BANANA") == "banana"
    assert normalize_identifier("BAN_ANA") == "ban_ana"
    assert normalize_identifier("BANaNA") == "ba_na_na"
    # handling spaces
    assert normalize_identifier("Small Love Potion") == "small_love_potion"


def test_normalize_path() -> None:
    assert normalize_path("small_love_potion") == "small_love_potion"
    assert normalize_path("small__love__potion") == "small__love__potion"
    assert normalize_path("Small_Love_Potion") == "small_love_potion"
    assert normalize_path("Small__Love__Potion") == "small__love__potion"
    assert normalize_path("Small Love Potion") == "small_love_potion"
    assert normalize_path("Small  Love  Potion") == "small_love_potion"


def test_normalize_non_alpha_single_underscore() -> None:
    assert RE_NON_ALPHANUMERIC.sub("_", "-=!*") == "_"
    assert RE_NON_ALPHANUMERIC.sub("_", "1-=!0*-") == "1_0_"
    assert RE_NON_ALPHANUMERIC.sub("_", "1-=!_0*-") == "1__0_"


def test_normalizes_underscores() -> None:
    assert normalize_identifier("event__value_value2____") == "event_value_value2_"
    assert normalize_path("e_vent__value_value2___") == "e_vent__value_value2___"


def test_normalize_make_dataset_name() -> None:
    # schema name is not normalized, a proper schema name is assumed to be used
    assert normalize_make_dataset_name("ban_ana_dataset", "default", "BANANA") == "ban_ana_dataset_BANANA"
    assert normalize_make_dataset_name("ban_ana_dataset", "default", "default") == "ban_ana_dataset"

    # also the dataset name is not normalized. it is verified if it is properly normalizes though
    with pytest.raises(InvalidDatasetName):
        normalize_make_dataset_name("BaNaNa", "default", "BANANA")

    # empty schemas are invalid
    with pytest.raises(ValueError):
        normalize_make_dataset_name("banana_dataset", None, None)
    with pytest.raises(ValueError):
        normalize_make_dataset_name("banana_dataset", "", "")

    # empty dataset names are invalid
    with pytest.raises(ValueError):
        normalize_make_dataset_name("", "ban_schema", "schema_ana")
    with pytest.raises(ValueError):
        normalize_make_dataset_name(None, "BAN_ANA", "BAN_ANA")


def test_normalize_make_dataset_name_none_default_schema() -> None:
    # if default schema is None, suffix is not added
    assert normalize_make_dataset_name("ban_ana_dataset", None, "default") == "ban_ana_dataset"
