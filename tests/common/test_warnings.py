import warnings
from typing import Any, Dict, Optional

import pytest

from dlt.common.typing import Annotated, TypedDict
from dlt.common.warnings import (
    Deprecated,
    DltDeprecationWarning,
    SkipDeprecation,
    apply_deprecations,
)


def _double(value: int) -> int:
    return value * 2


def _bool_or_skip(value: bool) -> Any:
    return "yes" if value else SkipDeprecation


class TDeprecated(TypedDict, total=False):
    old_a: Annotated[str, Deprecated(maps_to="new_a")]
    old_b: Annotated[int, Deprecated(maps_to="new_b", convert=_double)]
    old_c: Annotated[bool, Deprecated(maps_to="new_c", convert=_bool_or_skip)]
    old_d: Annotated[str, Deprecated(maps_to="new_d", message="use new_d")]
    old_e: Annotated[str, Deprecated(maps_to="new_e", since="2.5.0")]
    plain: str


class TNestedDeprecated(TypedDict, total=False):
    old_x: Annotated[str, Deprecated(maps_to="new_x")]


class TOuterDeprecated(TypedDict, total=False):
    old_a: Annotated[str, Deprecated(maps_to="new_a")]
    child: TNestedDeprecated


def test_apply_deprecations_converts_warns_and_removes() -> None:
    doc = {
        "old_a": "x",  # identity convert -> new_a
        "old_b": 21,  # custom convert doubles -> new_b
        "old_c": False,  # convert returns SkipDeprecation -> nothing written
        "plain": "p",  # schema field without a Deprecated marker -> untouched
        "keep": 1,  # not in the schema at all -> untouched
    }
    # one call migrates every deprecated key present, warning once per key
    with pytest.warns(DltDeprecationWarning) as record:
        result = apply_deprecations(TDeprecated, doc, since="1.0.0")

    # mutates and returns the same dict
    assert result is doc
    # identity and custom converts land under the new keys, SkipDeprecation writes
    # nothing, old keys are removed, and plain/unknown keys are left as-is
    assert doc == {"new_a": "x", "new_b": 42, "plain": "p", "keep": 1}
    # a warning per converted deprecated key (old_a, old_b, old_c)
    assert len(record) == 3


def test_apply_deprecations_recurses_into_nested_schema() -> None:
    doc: Dict[str, Any] = {"old_a": "a", "child": {"old_x": "x", "keep": 1}}
    # one call migrates the top-level field and recurses into the nested schema
    with pytest.warns(DltDeprecationWarning) as record:
        apply_deprecations(TOuterDeprecated, doc, since="1.0.0")

    assert doc == {"new_a": "a", "child": {"new_x": "x", "keep": 1}}
    # one warning for the top-level field, one for the nested field
    assert len(record) == 2


@pytest.mark.parametrize(
    "spec,doc,kwargs,expected,warns",
    [
        (TDeprecated, {"old_a": "old", "new_a": "new"}, {}, {"new_a": "new"}, "deprecated"),
        (
            TDeprecated,
            {"old_a": "x"},
            {"remove": False},
            {"old_a": "x", "new_a": "x"},
            "deprecated",
        ),
        (TDeprecated, {"old_d": "x"}, {}, {"new_d": "x"}, "use new_d"),
        (TDeprecated, {"old_e": "x"}, {}, {"new_e": "x"}, "2.5.0"),
        (TDeprecated, {"old_a": "x"}, {"warn": False}, {"new_a": "x"}, None),
        (TDeprecated, {"keep": 1}, {}, {"keep": 1}, None),
        (TOuterDeprecated, {"child": "not-a-dict"}, {}, {"child": "not-a-dict"}, None),
    ],
    ids=[
        "prefer-new-keeps-replacement",
        "remove-false-keeps-old-key",
        "marker-message-used-verbatim",
        "marker-since-overrides-call-site",
        "warn-false-converts-silently",
        "nothing-to-convert",
        "nested-value-not-a-dict",
    ],
)
def test_apply_deprecations_options(
    spec: Any,
    doc: Dict[str, Any],
    kwargs: Dict[str, Any],
    expected: Dict[str, Any],
    warns: Optional[str],
) -> None:
    if warns:
        with pytest.warns(DltDeprecationWarning, match=warns):
            apply_deprecations(spec, doc, since="1.0.0", **kwargs)
    else:
        # a silent path must emit nothing at all, so any warning is an error
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            apply_deprecations(spec, doc, since="1.0.0", **kwargs)
    assert doc == expected
