import importlib
import importlib.util
import inspect
import sys
import shutil
from typing import Iterator

import pytest

from dlt.common.reflection.ref import object_from_ref, callable_typechecker
from dlt.extract.reference import SourceFactory, SourceReference
from tests.utils import unload_modules

MODULE_SOURCE_PATH = "tests/common/reflection/cases/modules"


@pytest.fixture(autouse=True)
def set_syspath() -> Iterator[None]:
    sys.path.append(MODULE_SOURCE_PATH)
    try:
        yield
    finally:
        sys.path.pop()


def test_ref_import_with_missing_deps() -> None:
    with pytest.raises(ImportError):
        import missing_dep  # type: ignore[import-not-found]

    # missing_dep contains missing types and dependencies
    func_, trace = object_from_ref(
        "missing_dep.f",
        callable_typechecker,
        raise_exec_errors=True,
        import_missing_modules=True,
    )
    assert func_.__name__ == "f"
    assert trace is None
    class_, trace = object_from_ref(
        "missing_dep.One",
        lambda f_: f_ if issubclass(f_, object) else None,
        raise_exec_errors=True,
        import_missing_modules=True,
    )
    assert class_.__name__ == "One"
    assert trace is None


def test_ref_import_with_missing_package_deps() -> None:
    # find_spec imports package first which fails because it uses regular importer
    with pytest.raises(ImportError):
        object_from_ref(
            "pkg_missing_dep.mod_in_pkg_missing_dep.f",
            callable_typechecker,
            raise_exec_errors=True,
            import_missing_modules=True,
        )

    # here we prefer to get trace
    func_, trace = object_from_ref(
        "pkg_missing_dep.mod_in_pkg_missing_dep.f",
        callable_typechecker,
        raise_exec_errors=False,
        import_missing_modules=True,
    )
    assert func_ is None
    assert trace.reason == "ImportSpecError"
    assert isinstance(trace.exc, ModuleNotFoundError)


def test_import_deep_packages() -> None:
    # import from deeply nested packages and modules
    func_, _ = object_from_ref(
        "pkg_1.mod_2.pkg_3.mod_4.f",
        callable_typechecker,
    )
    assert callable(func_)
    # one of packages on the way does not exist
    func_, trace = object_from_ref(
        "pkg_1.mod_2.pkg_X.mod_4.f",
        callable_typechecker,
    )
    assert func_ is None
    assert trace.reason == "ModuleSpecNotFound"
    # on of package on the way has an error in its code
    func_, trace = object_from_ref(
        "pkg_1.mod_2.mod_bkn.mod_4.add_n_to_x",
        callable_typechecker,
    )
    assert func_ is None
    assert trace.reason == "ImportSpecError"
    assert isinstance(trace.exc, ModuleNotFoundError) and trace.exc.name == "n"
    # make it raise
    with pytest.raises(ModuleNotFoundError) as mod_ex:
        object_from_ref(
            "pkg_1.mod_2.mod_bkn.mod_4.add_n_to_x",
            callable_typechecker,
            raise_exec_errors=True,
        )
    assert mod_ex.value.name == "n"
    # our importer that ignores missing deps does not work for broken packages in the middle
    with pytest.raises(ModuleNotFoundError) as mod_ex:
        object_from_ref(
            "pkg_1.mod_2.mod_bkn.mod_4.add_n_to_x",
            callable_typechecker,
            raise_exec_errors=True,
            import_missing_modules=True,
        )


def test_ref_import() -> None:
    # import source
    s_, trace = object_from_ref("regular_mod.s", SourceReference._factory_typechecker)
    assert trace is None
    assert s_

    # module spec not found
    func_, trace = object_from_ref(
        "unknown_mod.f",
        callable_typechecker,
    )
    assert func_ is None
    assert trace.ref == "unknown_mod.f"
    assert trace.module == "unknown_mod"
    assert trace.attr_name == "f"
    assert trace.reason == "ModuleSpecNotFound"

    # NOTE: we test an error when executing package (not the final module) code in test_import_deep_packages

    # module code exec error
    func_, trace = object_from_ref("broken_mod.f", callable_typechecker)
    assert func_ is None
    assert trace.ref == "broken_mod.f"
    assert trace.reason == "ImportError"
    assert isinstance(trace.exc, NameError)
    # we can rise code execution errors
    with pytest.raises(NameError):
        object_from_ref("broken_mod.f", callable_typechecker, raise_exec_errors=True)

    # attr not found
    s_, trace = object_from_ref("regular_mod.not_s", SourceReference._factory_typechecker)
    assert s_ is None
    assert trace.attr_name == "not_s"
    assert trace.reason == "AttrNotFound"
    assert isinstance(trace.exc, AttributeError)

    # typecheck raises
    s_, trace = object_from_ref("regular_mod.f", SourceReference._factory_typechecker)
    assert s_ is None
    assert trace.attr_name == "f"
    assert trace.reason == "TypeCheck"
    assert isinstance(trace.exc, TypeError)

    # standalone resource is a regular function
    r_as_f, _ = object_from_ref("regular_mod.r", callable_typechecker)
    assert callable(r_as_f)
    assert not isinstance(r_as_f, SourceFactory)
    # typechecker may modify attr. source typechecker can extract factory from standalone function
    r_, _ = object_from_ref("regular_mod.r", SourceReference._factory_typechecker)
    assert isinstance(r_, SourceFactory)


def test_ref_is_properly_reloaded(tmp_path) -> None:
    mod_v1 = """\
import dlt

@dlt.source(name="foo_s")
def s():
    return []

@dlt.resource(name="foo_r", standalone=True)
def r():
    yield [1, 2, 3]

def foo():
    pass
"""


    mod_v2 = """\
import dlt

@dlt.source(name="bar_s")
def s():
    return []

@dlt.resource(name="bar_r", standalone=True)
def r():
    yield [1, 2, 3]

def bar():
    pass
"""
    module_name = "mod_to_reload"
    source_ref = f"{module_name}.s"
    resource_ref = f"{module_name}.r"
    module_path = tmp_path / f"{module_name}.py"
    sys.path.append(str(tmp_path))
    
    # write module v1
    with module_path.open("w") as f:
        f.write(mod_v1)

    # load module v1
    s_1, _ = object_from_ref(
        source_ref,
        SourceReference._factory_typechecker,
    )
    # calling object_from_ref loads the python module
    mod_to_reload = __import__(module_name)
    source_code_v1 = inspect.getsource(mod_to_reload)
    assert "foo_s" in source_code_v1
    assert "foo_r" in source_code_v1

    r_1, _ = object_from_ref(
        resource_ref,
        SourceReference._factory_typechecker,
    )

    assert s_1.name == "foo_s"
    assert r_1.name == "foo_r"


    # write module v2
    with module_path.open("w") as f:
        f.write(mod_v2)

    # load module v2
    s_2, _ = object_from_ref(
        source_ref,
        SourceReference._factory_typechecker,
    )

    # calling object_from_ref should reload the module
    source_code_v2 = inspect.getsource(mod_to_reload)
    assert "foo_s" not in source_code_v2
    assert "foo_r" not in source_code_v2
    assert "bar_s" in source_code_v2
    assert "bar_r" in source_code_v2

    assert s_2.name == "bar_s"

    r_2, _ = object_from_ref(
        resource_ref,
        SourceReference._factory_typechecker,
    )
    assert r_2.name == "bar_r"

    sys.path.pop()