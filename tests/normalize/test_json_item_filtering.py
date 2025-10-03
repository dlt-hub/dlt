from typing import Iterator
import pytest
from copy import deepcopy
from dlt.common.configuration.container import Container
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.schema.exceptions import ParentTableNotFoundException

from dlt.common.typing import DictStrAny
from dlt.common.schema import Schema
from dlt.common.schema.utils import new_table
from dlt.common.schema.typing import TSimpleRegex

from dlt.normalize.items_normalizers import JsonLItemsNormalizer
from dlt.normalize.normalize import Normalize
from tests.common.utils import load_json_case
from tests.normalize.utils import DEFAULT_CAPS, add_preferred_types


@pytest.fixture(autouse=True)
def default_caps() -> Iterator[DestinationCapabilitiesContext]:
    # set the postgres caps as default for the whole module
    with Container().injectable_context(DEFAULT_CAPS()) as caps:
        yield caps


@pytest.fixture
def item_normalizer() -> JsonLItemsNormalizer:
    n = Normalize()
    schema = Schema("event")
    _add_excludes(schema)
    return JsonLItemsNormalizer(None, None, schema, "load_id", n.config)


def test_row_field_filter(item_normalizer: JsonLItemsNormalizer) -> None:
    bot_case: DictStrAny = load_json_case("mod_bot_case")
    filtered_case = item_normalizer._filter_row("event_bot", deepcopy(bot_case))
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


def test_whole_row_filter(item_normalizer: JsonLItemsNormalizer) -> None:
    bot_case: DictStrAny = load_json_case("mod_bot_case")
    # the whole row should be eliminated if the exclude matches all the rows
    filtered_case = item_normalizer._filter_row(
        "event_bot__metadata", deepcopy(bot_case)["metadata"]
    )
    assert filtered_case == {}
    # also child rows will be excluded
    filtered_case = item_normalizer._filter_row(
        "event_bot__metadata__user", deepcopy(bot_case)["metadata"]
    )
    assert filtered_case == {}


def test_whole_row_filter_with_exception(item_normalizer: JsonLItemsNormalizer) -> None:
    bot_case: DictStrAny = load_json_case("mod_bot_case")
    # whole row will be eliminated
    filtered_case = item_normalizer._filter_row(
        "event_bot__custom_data", deepcopy(bot_case)["custom_data"]
    )
    # mind that path event_bot__custom_data__included_object was also eliminated
    assert filtered_case == {}
    # this child of the row has exception (^event_bot__custom_data__included_object__ - the __ at the end select all childern but not the parent)
    filtered_case = item_normalizer._filter_row(
        "event_bot__custom_data__included_object",
        deepcopy(bot_case)["custom_data"]["included_object"],
    )
    assert filtered_case == bot_case["custom_data"]["included_object"]
    filtered_case = item_normalizer._filter_row(
        "event_bot__custom_data__excluded_path", deepcopy(bot_case)["custom_data"]["excluded_path"]
    )
    assert filtered_case == {}


def test_filter_parent_table_schema_update(item_normalizer: JsonLItemsNormalizer) -> None:
    schema = item_normalizer.schema  # = Schema("event")
    # _add_excludes(schema)
    # add_preferred_types(schema)
    # filter out parent table and leave just child one. that should break the child-parent relationship and reject schema update
    source_row = {
        "metadata": [
            {
                "elvl1": [{"elvl2": [{"id": "level3_kept"}], "f": "elvl1_removed"}],
                "f": "metadata_removed",
            }
        ]
    }

    updates = []

    for (t, p, _), row in schema.normalize_data_item(source_row, "load_id", "event_bot"):
        row = item_normalizer._filter_row(t, row)
        if not row:
            # those rows are fully removed
            assert t in ["event_bot__metadata", "event_bot__metadata__elvl1"]
        else:
            row, partial_table = item_normalizer._coerce_row(
                t, schema.naming.shorten_fragments(*p), row
            )
            updates.append(partial_table)

    # try to apply updates
    assert len(updates) == 2
    # event bot table
    schema.update_table(updates[0])
    # event_bot__metadata__elvl1__elvl2
    with pytest.raises(ParentTableNotFoundException) as e:
        schema.update_table(updates[1])
    assert e.value.table_name == "event_bot__metadata__elvl1__elvl2"
    assert e.value.parent_table_name == "event_bot__metadata__elvl1"

    # add include filter that will preserve both tables
    updates.clear()
    schema = item_normalizer.schema = Schema("event")
    _add_excludes(schema)
    schema.get_table("event_bot")["filters"]["includes"].extend(
        [TSimpleRegex("re:^metadata___dlt_"), TSimpleRegex("re:^metadata__elvl1___dlt_")]
    )
    schema._compile_settings()
    for (t, p, _), row in schema.normalize_data_item(source_row, "load_id", "event_bot"):
        row = item_normalizer._filter_row(t, row)
        if p == ():
            assert "_dlt_id" in row
        else:
            # full linking not wiped out
            assert set(row.keys()).issuperset(["_dlt_id", "_dlt_parent_id", "_dlt_list_idx"])
        row, partial_table = item_normalizer._coerce_row(
            t, schema.naming.shorten_fragments(*p), row
        )
        updates.append(partial_table)
        schema.update_table(partial_table)

    assert len(updates) == 4
    # we must have leaf table
    schema.get_table("event_bot__metadata__elvl1__elvl2")


def _add_excludes(schema: Schema) -> None:
    bot_table = new_table("event_bot")
    bot_table.setdefault("filters", {})["excludes"] = ["re:^metadata", "re:^is_flagged$", "re:^data", "re:^custom_data"]  # type: ignore[typeddict-item]
    bot_table["filters"]["includes"] = [
        TSimpleRegex("re:^data__custom$"),
        TSimpleRegex("re:^custom_data__included_object__"),
        TSimpleRegex("re:^metadata__elvl1__elvl2__"),
    ]
    schema.update_table(bot_table)
    schema._compile_settings()
