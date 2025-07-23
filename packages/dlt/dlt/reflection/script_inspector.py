import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Tuple
from unittest.mock import patch
from importlib import import_module

from dlt.common import logger
from dlt.common.exceptions import DltException
from dlt.common.typing import DictStrAny
from dlt.common.reflection.ref import import_module_with_missing

from dlt.pipeline import Pipeline
from dlt.extract import DltSource
from dlt.extract.pipe_iterator import ManagedPipeIterator


def patch__init__(self: Any, *args: Any, **kwargs: Any) -> None:
    raise PipelineIsRunning(self, args, kwargs)


def import_script_module(
    module_path: str, script_relative_path: str, ignore_missing_imports: bool = False
) -> ModuleType:
    """Loads a module in `script_relative_path` by splitting it into a script module (file part) and package (folders).  `module_path` is added to sys.path
    Optionally, missing imports will be ignored by importing a dummy module instead.
    """
    if os.path.isabs(script_relative_path):
        raise ValueError(script_relative_path, f"Not relative path to `{module_path}`")

    module, _ = os.path.splitext(script_relative_path)
    module = ".".join(Path(module).parts)

    # add path to module search
    sys_path: str = None
    if module_path not in sys.path:
        sys_path = module_path
        # path must be first so we always load our module of
        sys.path.insert(0, sys_path)
    try:
        logger.info(f"Importing pipeline script from module `{module}` with path `{module_path}`")
        if ignore_missing_imports:
            return import_module_with_missing(module)
        else:
            return import_module(module)

    finally:
        # remove script module path
        if sys_path:
            sys.path.remove(sys_path)


def import_pipeline_script(
    module_path: str, script_relative_path: str, ignore_missing_imports: bool = False
) -> ModuleType:
    # patch entry points to pipeline, sources and resources to prevent pipeline from running
    with (
        patch.object(Pipeline, "__init__", patch__init__),
        patch.object(DltSource, "__init__", patch__init__),
        patch.object(ManagedPipeIterator, "__init__", patch__init__),
    ):
        return import_script_module(
            module_path, script_relative_path, ignore_missing_imports=ignore_missing_imports
        )


class PipelineIsRunning(DltException):
    def __init__(self, obj: object, args: Tuple[str, ...], kwargs: DictStrAny) -> None:
        super().__init__(
            "The pipeline script instantiates the pipeline on import. Did you forget to use if"
            f" __name__ == 'main':? in {obj.__class__.__name__}",
            obj,
            args,
            kwargs,
        )
