import builtins
import importlib.util
from importlib import import_module
from types import ModuleType, SimpleNamespace
from typing import Any, Callable, Literal, NamedTuple, Tuple, Mapping, List, Sequence

from dlt.common.exceptions import MissingDependencyException, TypeErrorWithKnownTypes
from dlt.common.typing import TAny


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


def import_module_with_missing(name: str, missing_modules: Tuple[str, ...] = ()) -> ModuleType:
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


class ImportTrace(NamedTuple):
    ref: str
    module: str
    attr_name: str
    reason: Literal[
        "MissingDependencyException",
        "ModuleSpecNotFound",
        "ImportSpecError",
        "ImportError",
        "AttrNotFound",
        "TypeCheck",
    ]
    exc: Exception


def callable_typechecker(o: TAny) -> TAny:
    if callable(o):
        return o  # type: ignore[no-any-return]
    raise TypeErrorWithKnownTypes("attr", o, ["Callable"])


def object_from_ref(
    ref: str,
    typechecker: Callable[[Any], Any],
    raise_exec_errors: bool = False,
    import_missing_modules: bool = False,
) -> Tuple[Any, ImportTrace]:
    """Splits ref on module.attr and import module, then gets attr and runs typechecker on it.
    If `import_missing_modules` is True, will use custom importer that replaces missing modules with dummy
    from which you can import anything.
    NOTE: if your import got patched it will not work. Use this only to import refs ie. to verify if they exist, not in production code
    if `raise_exec_errors`, all errors that happen when code of the imported module is executed are raised, otherwise corresponding trace is returned

    Returns: a tuple (typechecked attr, trace if error)

    """
    if "." not in ref:
        raise ValueError("`ref` format is `module.attr` and must contain at least one `.`")
    module_path, attr_name = ref.rsplit(".", 1)
    try:
        spec = importlib.util.find_spec(module_path)
        if spec is None:
            return None, ImportTrace(ref, module_path, attr_name, "ModuleSpecNotFound", None)
    except Exception as exc:
        # one of modules on path could not be imported
        if isinstance(exc, ModuleNotFoundError) and module_path.startswith(exc.name):
            return None, ImportTrace(ref, module_path, attr_name, "ModuleSpecNotFound", exc)
        if raise_exec_errors:
            raise
        # spec may raise when importing parent packages of the module_path
        return None, ImportTrace(ref, module_path, attr_name, "ImportSpecError", exc)

    _importer = import_module_with_missing if import_missing_modules else import_module

    try:
        dest_module = _importer(module_path)
    except MissingDependencyException as missing_ex:
        if raise_exec_errors:
            raise
        return None, ImportTrace(
            ref, module_path, attr_name, "MissingDependencyException", missing_ex
        )
    except Exception as mod_ex:
        if raise_exec_errors:
            raise
        return None, ImportTrace(ref, module_path, attr_name, "ImportError", mod_ex)

    try:
        factory = getattr(dest_module, attr_name)
    except AttributeError as attr_ex:
        return None, ImportTrace(ref, module_path, attr_name, "AttrNotFound", attr_ex)

    # check type
    try:
        return typechecker(factory), None
    except Exception as type_exc:
        return None, ImportTrace(ref, module_path, attr_name, "TypeCheck", type_exc)
