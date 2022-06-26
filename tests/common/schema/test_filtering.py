import pytest
from copy import deepcopy

from dlt.common.typing import StrAny
from dlt.common.schema import Schema
from dlt.common.schema.utils import new_table

from tests.common.utils import load_json_case


@pytest.fixture
def schema() -> Schema:
    return Schema("event")


def test_row_field_filter(schema: Schema) -> None:
    _add_excludes(schema)
    bot_case: StrAny = load_json_case("mod_bot_case")
    filtered_case = schema.filter_row("event_bot", deepcopy(bot_case))
    # metadata, is_flagged and data should be eliminated
    ref_case = deepcopy(bot_case)
    del ref_case["metadata"]
    del ref_case["is_flagged"]
    del ref_case["data"]
    del ref_case["data__custom__goes"]
    del ref_case["custom_data"]
    assert ref_case == filtered_case
    # one of the props was included form the excluded (due to ^event_bot__data__custom$)
    assert ref_case["data__custom"] == "remains"


def test_whole_row_filter(schema: Schema) -> None:
    _add_excludes(schema)
    bot_case: StrAny = load_json_case("mod_bot_case")
    # the whole row should be eliminated if the exclude matches all the rows
    filtered_case = schema.filter_row("event_bot__metadata", deepcopy(bot_case)["metadata"])
    assert filtered_case == {}
    # also child rows will be excluded
    filtered_case = schema.filter_row("event_bot__metadata__user", deepcopy(bot_case)["metadata"])
    assert filtered_case == {}


def test_whole_row_filter_with_exception(schema: Schema) -> None:
    _add_excludes(schema)
    bot_case: StrAny = load_json_case("mod_bot_case")
    # whole row will be eliminated
    filtered_case = schema.filter_row("event_bot__custom_data", deepcopy(bot_case)["custom_data"])
    # mind that path event_bot__custom_data__included_object was also eliminated
    assert filtered_case == {}
    # this child of the row has exception (^event_bot__custom_data__included_object__ - the __ at the end select all childern but not the parent)
    filtered_case = schema.filter_row("event_bot__custom_data__included_object", deepcopy(bot_case)["custom_data"]["included_object"])
    assert filtered_case == bot_case["custom_data"]["included_object"]
    filtered_case = schema.filter_row("event_bot__custom_data__excluded_path", deepcopy(bot_case)["custom_data"]["excluded_path"])
    assert filtered_case == {}


def _add_excludes(schema: Schema) -> None:
    bot_table = new_table("event_bot")
    bot_table.setdefault("filters", {})["excludes"] = ["re:^metadata", "re:^is_flagged$", "re:^data", "re:^custom_data"]
    bot_table["filters"]["includes"] = ["re:^data__custom$", "re:^custom_data__included_object__"]
    schema.update_schema(bot_table)
    schema._compile_regexes()
