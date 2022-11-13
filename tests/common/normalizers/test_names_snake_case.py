import pytest

from dlt.common.normalizers.names.snake_case import normalize_column_name, normalize_table_name, normalize_make_dataset_name, RE_NON_ALPHANUMERIC
from dlt.common.schema.exceptions import InvalidDatasetName


def test_normalize_column_name() -> None:
    assert normalize_column_name("event_value") == "event_value"
    assert normalize_column_name("event value") == "event_value"
    assert normalize_column_name("event-.!:<>value") == "event_value"
    # prefix leading digits
    assert normalize_column_name("1event_n'") == "_1event_n_"
    assert normalize_column_name("123event_n'") == "_123event_n_"
    # all lowercase and converted to snake
    assert normalize_column_name("123BaNaNa") == "_123_ba_na_na"
    # consecutive capital letters
    assert normalize_column_name("BANANA") == "banana"
    assert normalize_column_name("BAN_ANA") == "ban_ana"
    assert normalize_column_name("BANaNA") == "ba_na_na"
    # handling spaces
    assert normalize_column_name("Small Love Potion") == "small_love_potion"


def test_normalize_table_name() -> None:
    assert normalize_table_name("small_love_potion") == "small_love_potion"
    assert normalize_table_name("small__love__potion") == "small__love__potion"
    assert normalize_table_name("Small_Love_Potion") == "small_love_potion"
    assert normalize_table_name("Small__Love__Potion") == "small__love__potion"
    assert normalize_table_name("Small Love Potion") == "small_love_potion"
    assert normalize_table_name("Small  Love  Potion") == "small_love_potion"


def test_normalize_non_alpha_single_underscore() -> None:
    assert RE_NON_ALPHANUMERIC.sub("_", "-=!*") == "_"
    assert RE_NON_ALPHANUMERIC.sub("_", "1-=!0*-") == "1_0_"
    assert RE_NON_ALPHANUMERIC.sub("_", "1-=!_0*-") == "1__0_"


def test_normalizes_underscores() -> None:
    assert normalize_column_name("event__value_value2____") == "event_value_value2_"
    assert normalize_table_name("e_vent__value_value2____") == "e_vent__value_value2__"


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
