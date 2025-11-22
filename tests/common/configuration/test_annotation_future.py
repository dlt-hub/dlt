from __future__ import annotations

import pytest
from typing import Optional

import dlt
from dlt.common.configuration import configspec
from dlt.common.configuration.inject import get_fun_spec, with_config
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.typing import ForwardRef

from tests.utils import preserve_environ
from tests.common.configuration.utils import environment
from tests.common._annotated_futures_module import AnnConfigSpec


def test_configspec_annotation(environment) -> None:
    @configspec
    class DataConf(BaseConfiguration):
        x: int = None
        x_7: Optional[int] = 3

    assert DataConf.__annotations__ == {"x": "int", "x_7": "Optional[int]"}
    assert DataConf.get_resolvable_fields() == {"x": int, "x_7": Optional[int]}

    # resolve it
    environment["X"] = "10"
    c = resolve_configuration(DataConf())
    assert c.x == 10


def test_global_configspec_global_annotation(environment) -> None:
    fields = AnnConfigSpec.get_resolvable_fields()
    assert fields["word"] is str
    # NOTE: do not import _Sentinel - this will add it to globals of this module and make this
    # tests irrelevant
    assert fields["sentinel"].__name__ == "_Sentinel"
    assert fields["sentinel_f"].__name__ == "_Sentinel"

    # resolve
    environment["WORD"] = "W"
    environment["SENTINEL"] = "S"
    environment["SENTINEL_F"] = "SF"
    c = resolve_configuration(AnnConfigSpec())
    assert c.word == "W"
    assert c.sentinel == "S"
    assert c.sentinel_f == "SF"


def test_configspec_inner_annotation(environment) -> None:
    # make it resolvable by deriving from str
    class _Sentinel(str):
        pass

    # will not resolve inner annotation, we must be passing locals which is beyond the scope
    # of configspec (could be done by inspecting caller frame)

    with pytest.raises(NameError):

        @configspec
        class InnerAnnotation(BaseConfiguration):
            sentinel: _Sentinel = None
            word: str = None


class _SentinelG(str):
    pass


@with_config
def ann_func(
    sentinel: _SentinelG,
    sentinel_f: ForwardRef("_SentinelG") = dlt.config.value,  # type: ignore
    word: str = "word",
):
    return sentinel, sentinel_f, word


def test_with_config_global_annotation(environment) -> None:
    spec = get_fun_spec(ann_func)
    assert spec.get_resolvable_fields() == {"sentinel_f": _SentinelG, "word": str}

    environment["WORD"] = "W"
    environment["SENTINEL_F"] = "SF"

    assert ann_func(_SentinelG("F")) == ("F", "SF", "W")


def test_with_config_inner_annotation(environment) -> None:
    class _SentinelI(str):
        pass

    @with_config
    def ann_func(
        sentinel: _SentinelI,
        sentinel_f: ForwardRef("_SentinelI") = dlt.config.value,  # type: ignore
        word: str = "word",
    ):
        return sentinel, sentinel_f, word

    spec = get_fun_spec(ann_func)
    # only "word" will be included in SPEC, sentinel_f could not be resolved and is invalid hint
    assert spec.get_resolvable_fields() == {"word": str}
