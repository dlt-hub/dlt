import os
from pathlib import Path
from typing import Iterator
import pytest
import sys

from dlt.common.reflection.ref import callable_typechecker, import_folder_module, object_from_ref
from dlt.common.storages import FileStorage
from dlt.extract.reference import SourceFactory, SourceReference
from tests.utils import auto_unload_modules, test_storage


@pytest.fixture(autouse=True)
def set_syspath() -> Iterator[None]:
    sys.path.append("tests/common/reflection/cases/modules")
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


def test_import_folder_module(test_storage: FileStorage) -> None:
    # the folder is named like an installed package: it imports under the given package instead
    test_storage.create_folder("json")
    folder = Path(test_storage.make_full_path("json"))
    (folder / "helpers.py").write_text("VALUE = 1\n")
    (folder / "agent.py").write_text("from .helpers import VALUE\nCALLS = [VALUE]\n")
    package = "_test_folder_pkg"
    try:
        module = import_folder_module(str(folder), "agent", package)
        assert module.CALLS == [1]
        assert module.__name__ == f"{package}.agent"
        assert sys.modules[f"{package}.helpers"].VALUE == 1

        # changed code: served from the cache unless reloaded
        (folder / "helpers.py").write_text("VALUE = 2\n")
        # same size, maybe same mtime: move the mtime so the cached bytecode is not reused
        os.utime(folder / "helpers.py", (0, 0))
        assert import_folder_module(str(folder), "agent", package) is module
        reloaded = import_folder_module(str(folder), "agent", package, reload=True)
        assert reloaded.CALLS == [2]

        # a module that fails to run is not left behind
        (folder / "broken.py").write_text("raise RuntimeError('boom')\n")
        with pytest.raises(RuntimeError):
            import_folder_module(str(folder), "broken", package, reload=True)
        assert f"{package}.broken" not in sys.modules
    finally:
        for name in [n for n in sys.modules if n.startswith(package)]:
            del sys.modules[name]
