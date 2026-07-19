import warnings
from typing import Any, Dict

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


def test_apply_deprecations_options_and_messages() -> None:
    # both old and new present: prefer_new keeps the new value, drops the old, still warns
    doc: Dict[str, Any] = {"old_a": "old", "new_a": "new"}
    with pytest.warns(DltDeprecationWarning):
        apply_deprecations(TDeprecated, doc, since="1.0.0")
    assert doc == {"new_a": "new"}

    # remove=False keeps the old key alongside the new one
    doc = {"old_a": "x"}
    with pytest.warns(DltDeprecationWarning):
        apply_deprecations(TDeprecated, doc, since="1.0.0", remove=False)
    assert doc == {"old_a": "x", "new_a": "x"}

    # custom marker message is used verbatim
    doc = {"old_d": "x"}
    with pytest.warns(DltDeprecationWarning, match="use new_d"):
        apply_deprecations(TDeprecated, doc, since="1.0.0")

    # per-field `since` on the marker overrides the call-site default
    doc = {"old_e": "x"}
    with pytest.warns(DltDeprecationWarning, match="2.5.0"):
        apply_deprecations(TDeprecated, doc, since="1.0.0")

    # warn=False converts silently; nothing to convert is a no-op — both raise on any warning
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        doc = {"old_a": "x"}
        apply_deprecations(TDeprecated, doc, since="1.0.0", warn=False)
        assert doc == {"new_a": "x"}

        doc = {"keep": 1}
        apply_deprecations(TDeprecated, doc, since="1.0.0")
        assert doc == {"keep": 1}
