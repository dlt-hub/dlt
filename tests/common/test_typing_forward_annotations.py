from __future__ import annotations

import inspect
import pytest
from typing import TypedDict

try:
    # backport from inspect
    from typing_extensions import get_annotations  # type: ignore
except ImportError:
    from inspect import get_annotations  # type: ignore

from dlt.common.typing import AnyFun, get_type_hints, ForwardRef, resolve_single_annotation

from ._annotated_futures_module import AnnTypedDict, ann_func


def _produce_typed_dict_inner_annotations():
    class _Sentinel:
        pass

    class InnerAnnotation(TypedDict):
        sentinel: _Sentinel
        word: str

    # annotates with str but TypedDict converts into ForwardRef
    return InnerAnnotation


def _produce_func_inner_annotations() -> AnyFun:
    class _Sentinel:
        pass

    def _func(s: _Sentinel, arg1: bool = False) -> _Sentinel:
        pass

    return _func


def test_inner_forward_annotations():
    # tests various assumptions in forward annotations

    # typeddict converts forward str ann to ForwardRef 🤯
    # NOTE: re. the above: eval_str does nothing
    td_ann = get_annotations(_produce_typed_dict_inner_annotations(), eval_str=False)
    assert isinstance(td_ann["word"], ForwardRef)
    assert isinstance(td_ann["sentinel"], ForwardRef)
    # resolve forward ref
    assert resolve_single_annotation(td_ann["word"]) is str
    # inner forward ref cannot be resolved
    with pytest.raises(NameError):
        assert resolve_single_annotation(td_ann["sentinel"], raise_on_error=True) is str
    # forward ref could not be resolved
    assert isinstance(resolve_single_annotation(td_ann["sentinel"]), ForwardRef)
    # get_type_hints cannot resolve
    with pytest.raises(NameError):
        get_type_hints(_produce_typed_dict_inner_annotations())

    # function definition
    with pytest.raises(NameError):
        fun_ann = get_annotations(_produce_func_inner_annotations(), eval_str=True)
    fun_ann = get_annotations(
        _produce_func_inner_annotations(), eval_str=False
    )  # _produce_func_inner_annotations().__annotations__ #
    assert isinstance(fun_ann["arg1"], str)
    assert isinstance(fun_ann["s"], str)
    assert resolve_single_annotation(fun_ann["arg1"]) is bool
    # cannot resolve, keeps string
    assert isinstance(resolve_single_annotation(fun_ann["s"]), str)
    # get_type_hints cannot resolve
    with pytest.raises(NameError):
        get_type_hints(_produce_func_inner_annotations())
    # inspect signature
    sig = inspect.signature(_produce_func_inner_annotations())
    # not resolved
    assert isinstance(sig.parameters["s"].annotation, str)


def test_module_forward_annotations():
    from . import _annotated_futures_module

    td_ann = get_annotations(AnnTypedDict)
    assert isinstance(td_ann["word"], ForwardRef)
    assert isinstance(td_ann["sentinel"], ForwardRef)
    assert resolve_single_annotation(td_ann["word"]) is str
    # will resolve str in global typed dict
    # NOTE: do not import sentinel - it becomes visible in this module globals, this makes test meaningless
    assert (
        resolve_single_annotation(
            td_ann["sentinel"], globalns=_annotated_futures_module.__dict__
        ).__name__
        == "_Sentinel"
    )
    # typed hints should resolve
    get_type_hints(AnnTypedDict)

    # func definition
    fun_ann = get_annotations(ann_func, eval_str=True)
    assert fun_ann["sentinel"].__name__ == "_Sentinel"
    # kept as forward ref
    assert isinstance(fun_ann["sentinel_f"], ForwardRef)

    fun_ann = get_annotations(ann_func, eval_str=False)
    assert isinstance(fun_ann["sentinel"], str)
    assert isinstance(fun_ann["sentinel_f"], str)
    assert resolve_single_annotation(fun_ann["word"]) is str
    # must pass module
    with pytest.raises(NameError):
        resolve_single_annotation(fun_ann["sentinel"], raise_on_error=True)
    assert (
        resolve_single_annotation(
            fun_ann["sentinel"], globalns=_annotated_futures_module.__dict__
        ).__name__
        == "_Sentinel"
    )
    assert (
        resolve_single_annotation(
            fun_ann["sentinel_f"], globalns=_annotated_futures_module.__dict__
        ).__name__
        == "_Sentinel"
    )
