from types import SimpleNamespace
import pytest

from dlt.reflection.script_inspector import load_script_module, inspect_pipeline_script, DummyModule, PipelineIsRunning

from tests.utils import unload_modules

MODULE_CASES = "./tests/reflection/module_cases"

def test_import_init_module() -> None:
    with pytest.raises(ModuleNotFoundError):
        load_script_module("./tests/reflection/", "module_cases", ignore_missing_imports=False)
    m = load_script_module("./tests/reflection/", "module_cases", ignore_missing_imports=True)
    assert isinstance(m.xxx, DummyModule)
    assert isinstance(m.a1, SimpleNamespace)


def test_import_module() -> None:
    load_script_module(MODULE_CASES, "all_imports", ignore_missing_imports=False)
    # the module below raises
    with pytest.raises(NotImplementedError):
        load_script_module(MODULE_CASES, "raises", ignore_missing_imports=True)
    # the module below has syntax error
    with pytest.raises(SyntaxError):
        load_script_module(MODULE_CASES, "syntax_error", ignore_missing_imports=True)
    # the module has invalid import structure
    with pytest.raises(ImportError):
        load_script_module(MODULE_CASES, "no_pkg", ignore_missing_imports=True)
    # but with package name in module name it will work
    m = load_script_module("./tests/reflection/", "module_cases.no_pkg", ignore_missing_imports=True)
    # uniq_id got imported
    assert isinstance(m.uniq_id(), str)


def test_import_module_with_missing_dep_exc() -> None:
    # will ignore MissingDependencyException
    m = load_script_module(MODULE_CASES, "dlt_import_exception", ignore_missing_imports=True)
    assert isinstance(m.e, SimpleNamespace)


def test_import_wrong_pipeline_script() -> None:
    with pytest.raises(PipelineIsRunning):
        inspect_pipeline_script(MODULE_CASES, "executes_resource", ignore_missing_imports=False)
