import os
import sys
import builtins
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, Tuple, List, Mapping, Sequence
from unittest.mock import patch
from importlib import import_module

from dlt.common import logger
from dlt.common.exceptions import DltException, MissingDependencyException
from dlt.common.typing import DictStrAny

from dlt.pipeline import Pipeline
from dlt.extract import DltSource
from dlt.extract.pipe import ManagedPipeIterator


def patch__init__(self: Any, *args: Any, **kwargs: Any) -> None:
    raise PipelineIsRunning(self, args, kwargs)


class DummyModule(ModuleType):
    """A dummy module from which you can import anything"""

    def __getattr__(self, key: str) -> Any:
        if key[0].isupper():
            # if imported name is capitalized, import type
            return SimpleNamespace
        else:
            # otherwise import instance
            return SimpleNamespace()

    __all__: List[Any] = []  # support wildcard imports


def _import_module(name: str, missing_modules: Tuple[str, ...] = ()) -> ModuleType:
    """Module importer that ignores missing modules by importing a dummy module"""

    def _try_import(
        name: str,
        _globals: Mapping[str, Any] = None,
        _locals: Mapping[str, Any] = None,
        fromlist: Sequence[str] = (),
        level: int = 0,
    ) -> ModuleType:
        """This function works as follows: on ImportError it raises. This import error is then next caught in the main function body and the name is added to exceptions.
        Next time if the name is on exception list or name is a package on exception list we return DummyModule and do not reraise
        This excepts only the modules that bubble up ImportError up until our code so any handled import errors are not excepted
        """
        try:
            return real_import(name, _globals, _locals, fromlist, level)
        except ImportError:
            # print(f"_import_module {name} {missing_modules} {fromlist} {level} {ex}")
            # return a dummy when: (1) name is on exception list (2) name is package path (dot separated) that start with exception from the list
            if any(name == m or name.startswith(m + ".") for m in missing_modules):
                return DummyModule(name)
            else:
                raise

    try:
        # patch built in import
        real_import, builtins.__import__ = builtins.__import__, _try_import  # type: ignore
        # discover missing modules and repeat until all are patched by dummies
        while True:
            try:
                return import_module(name)
            except ImportError as ie:
                if ie.name is None:
                    raise
                # print(f"ADD {ie.name} {ie.path} vs {name} vs {str(ie)}")
                if ie.name in missing_modules:
                    raise
                missing_modules += (ie.name,)
            except MissingDependencyException as me:
                if isinstance(me.__context__, ImportError):
                    if me.__context__.name is None:
                        raise
                    if me.__context__.name in missing_modules:
                        # print(f"{me.__context__.name} IN :/")
                        raise
                    # print(f"ADD {me.__context__.name}")
                    missing_modules += (me.__context__.name,)
                else:
                    raise
    finally:
        builtins.__import__ = real_import


def load_script_module(
    module_path: str, script_relative_path: str, ignore_missing_imports: bool = False
) -> ModuleType:
    """Loads a module in `script_relative_path` by splitting it into a script module (file part) and package (folders).  `module_path` is added to sys.path
    Optionally, missing imports will be ignored by importing a dummy module instead.
    """
    if os.path.isabs(script_relative_path):
        raise ValueError(script_relative_path, f"Not relative path to {module_path}")
    # script_path = os.path.join(module_path, script_relative_path)
    # if not os.path.isfile(script_path) and not os.path:
    #     raise FileNotFoundError(script_path)

    module, _ = os.path.splitext(script_relative_path)
    module = ".".join(Path(module).parts)

    # add path to module search
    sys_path: str = None
    if module_path not in sys.path:
        sys_path = module_path
        # path must be first so we always load our module of
        sys.path.insert(0, sys_path)
    try:
        logger.info(f"Importing pipeline script from path {module_path} and module: {module}")
        if ignore_missing_imports:
            return _import_module(f"{module}")
        else:
            return import_module(f"{module}")

    finally:
        # remove script module path
        if sys_path:
            sys.path.remove(sys_path)


def inspect_pipeline_script(
    module_path: str, script_relative_path: str, ignore_missing_imports: bool = False
) -> ModuleType:
    # patch entry points to pipeline, sources and resources to prevent pipeline from running
    with patch.object(Pipeline, "__init__", patch__init__), patch.object(
        DltSource, "__init__", patch__init__
    ), patch.object(ManagedPipeIterator, "__init__", patch__init__):
        return load_script_module(
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
