from types import SimpleNamespace
import pytest

from dlt.reflection.script_inspector import (
    import_script_module,
    import_pipeline_script,
    DummyModule,
    PipelineIsRunning,
)

from tests.utils import unload_modules

MODULE_CASES = "./tests/reflection/module_cases"


def test_import_init_module() -> None:
    with pytest.raises(ModuleNotFoundError):
        import_script_module("./tests/reflection/", "module_cases", ignore_missing_imports=False)
    m = import_script_module("./tests/reflection/", "module_cases", ignore_missing_imports=True)
    assert isinstance(m.xxx, DummyModule)
    assert isinstance(m.a1, SimpleNamespace)


def test_import_module() -> None:
    import_script_module(MODULE_CASES, "all_imports", ignore_missing_imports=False)
    # the module below raises
    with pytest.raises(NotImplementedError):
        import_script_module(MODULE_CASES, "raises", ignore_missing_imports=True)
    # the module below has syntax error
    with pytest.raises(SyntaxError):
        import_script_module(MODULE_CASES, "syntax_error", ignore_missing_imports=True)
    # the module has invalid import structure
    with pytest.raises(ImportError):
        import_script_module(MODULE_CASES, "no_pkg", ignore_missing_imports=True)
    # but with package name in module name it will work
    m = import_script_module(
        "./tests/reflection/", "module_cases.no_pkg", ignore_missing_imports=True
    )
    # uniq_id got imported
    assert isinstance(m.uniq_id(), str)


def test_import_module_with_missing_dep_exc() -> None:
    # will ignore MissingDependencyException
    m = import_script_module(MODULE_CASES, "dlt_import_exception", ignore_missing_imports=True)
    assert isinstance(m.e, SimpleNamespace)


def test_import_module_capitalized_as_type() -> None:
    # capitalized names are imported as types
    m = import_script_module(MODULE_CASES, "import_as_type", ignore_missing_imports=True)
    assert issubclass(m.Tx, SimpleNamespace)
    assert isinstance(m.tx, SimpleNamespace)


def test_import_wrong_pipeline_script() -> None:
    with pytest.raises(PipelineIsRunning):
        import_pipeline_script(MODULE_CASES, "executes_resource", ignore_missing_imports=False)


def test_package_dummy_clash() -> None:
    # a case where the whole `stripe_analytics` import raises MissingImport. we patched `stripe` import already
    # so if do not recognize package names with following condition (mind the dot):
    # if any(name == m or name.startswith(m + ".") for m in missing_modules):
    # we would return dummy for the whole module
    m = import_script_module(MODULE_CASES, "stripe_analytics_pipeline", ignore_missing_imports=True)
    # and those would fails
    assert m.VALUE == 1
    assert m.HELPERS_VALUE == 3
