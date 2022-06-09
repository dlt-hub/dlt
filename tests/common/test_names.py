from dlt.common.names import normalize_db_name, normalize_schema_name, normalize_table_name


def test_fix_field_name() -> None:
    assert normalize_db_name("event_value") == "event_value"
    assert normalize_db_name("event value") == "event_value"
    assert normalize_db_name("event-.!:value") == "event_value"
    # remove leading digits
    assert normalize_db_name("1event_n'") == "_event_n_"
    assert normalize_db_name("123event_n'") == "_event_n_"
    # all lowercase and converted to snake
    assert normalize_db_name("123BaNaNa") == "_ba_na_na"
    # consecutive capital letters
    assert normalize_db_name("BANANA") == "banana"
    assert normalize_db_name("BAN_ANA") == "ban_ana"
    assert normalize_db_name("BANaNA") == "ba_na_na"


def test_normalizes_underscores() -> None:
    assert normalize_db_name("event__value_value2____") == "event_value_value2_"
    assert normalize_table_name("e_vent__value_value2____") == "e_vent__value_value2__"


def test_normalize_schema_name() -> None:
    assert normalize_schema_name("BAN_ANA") == "banana"
    assert normalize_schema_name("event-.!:value") == "eventvalue"
