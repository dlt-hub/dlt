import os
import sys
from types import ModuleType
from typing import Any, Tuple
from unittest.mock import patch
from importlib import import_module

from dlt.common.exceptions import DltException
from dlt.common.typing import DictStrAny

from dlt.pipeline import Pipeline
from dlt.extract.source import DltSource, PipeIterator


def patch__init__(self: Any, *args: Any, **kwargs: Any) -> None:
    raise PipelineIsRunning(self, args, kwargs)


def inspect_pipeline_script(script_path: str) -> ModuleType:
    if not os.path.isfile(script_path):
        raise FileNotFoundError(script_path)
    # get module import data
    path, module = os.path.split(script_path)
    module, _ = os.path.splitext(module)

    # add path to module search
    sys_path: str = None
    if path not in sys.path:
        sys_path = path
        sys.path.append(sys_path)
    try:

        # patch entry points to pipeline, sources and resources to prevent pipeline from running
        with patch.object(Pipeline, '__init__', patch__init__), patch.object(DltSource, '__init__', patch__init__), patch.object(PipeIterator, '__init__', patch__init__):
            return import_module(module)
    finally:
        # remove script module path
        if sys_path:
            sys.path.remove(sys_path)


class PipelineIsRunning(DltException):
    def __init__(self, obj: object, args: Tuple[str, ...], kwargs: DictStrAny) -> None:
        super().__init__(f"The pipeline script instantiates the pipeline on import. Did you forget to use if __name__ == 'main':? in {obj.__class__.__name__}", obj, args, kwargs)
