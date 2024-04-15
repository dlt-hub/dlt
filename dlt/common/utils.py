import os
from pathlib import Path
import sys
import base64
import hashlib
import secrets
from contextlib import contextmanager
from functools import wraps
from os import environ
from types import ModuleType
import traceback
import zlib

from typing import (
    Any,
    ContextManager,
    Dict,
    Iterator,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    Mapping,
    List,
    Union,
    Counter,
    Iterable,
)
from collections.abc import Mapping as C_Mapping

from dlt.common.exceptions import DltException, ExceptionTrace, TerminalException
from dlt.common.typing import AnyFun, StrAny, DictStrAny, StrStr, TAny, TFun


T = TypeVar("T")
TDict = TypeVar("TDict", bound=DictStrAny)

TKey = TypeVar("TKey")
TValue = TypeVar("TValue")

# row counts
RowCounts = Dict[str, int]


def chunks(iterable: Iterable[T], n: int) -> Iterator[Sequence[T]]:
    it = iter(iterable)
    while True:
        chunk = list()
        try:
            for _ in range(n):
                chunk.append(next(it))
        except StopIteration:
            if chunk:
                yield chunk
            break
        yield chunk


def uniq_id(len_: int = 16) -> str:
    """Returns a hex encoded crypto-grade string of random bytes with desired len_"""
    return secrets.token_hex(len_)


def uniq_id_base64(len_: int = 16) -> str:
    """Returns a base64 encoded crypto-grade string of random bytes with desired len_"""
    return base64.b64encode(secrets.token_bytes(len_)).decode("ascii").rstrip("=")


def many_uniq_ids_base64(n_ids: int, len_: int = 16) -> List[str]:
    """Generate `n_ids` base64 encoded crypto-grade strings of random bytes with desired len_.
    This is more performant than calling `uniq_id_base64` multiple times.
    """
    random_bytes = secrets.token_bytes(n_ids * len_)
    encode = base64.b64encode
    return [
        encode(random_bytes[i : i + len_]).decode("ascii").rstrip("=")
        for i in range(0, n_ids * len_, len_)
    ]


def digest128(v: str, len_: int = 15) -> str:
    """Returns a base64 encoded shake128 hash of str `v` with digest of length `len_` (default: 15 bytes = 20 characters length)"""
    return (
        base64.b64encode(hashlib.shake_128(v.encode("utf-8")).digest(len_))
        .decode("ascii")
        .rstrip("=")
    )


def digest128b(v: bytes, len_: int = 15) -> str:
    """Returns a base64 encoded shake128 hash of bytes `v` with digest of length `len_` (default: 15 bytes = 20 characters length)"""
    enc_v = base64.b64encode(hashlib.shake_128(v).digest(len_)).decode("ascii")
    return enc_v.rstrip("=")


def digest256(v: str) -> str:
    digest = hashlib.sha3_256(v.encode("utf-8")).digest()
    return base64.b64encode(digest).decode("ascii")


def str2bool(v: str) -> bool:
    if isinstance(v, bool):
        return v
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise ValueError("Boolean value expected.")


# def flatten_list_of_dicts(dicts: Sequence[StrAny]) -> StrAny:
#     """
#     Transforms a list of objects [{K: {...}}, {L: {....}}, ...] -> {K: {...}, L: {...}...}
#     """
#     o: DictStrAny = {}
#     for d in dicts:
#         for k,v in d.items():
#             if k in o:
#                 raise KeyError(f"Cannot flatten with duplicate key {k}")
#             o[k] = v
#     return o


def flatten_list_of_str_or_dicts(seq: Sequence[Union[StrAny, str]]) -> DictStrAny:
    """
    Transforms a list of objects or strings [{K: {...}}, L, ...] -> {K: {...}, L: None, ...}
    """
    o: DictStrAny = {}
    for e in seq:
        if isinstance(e, dict):
            for k, v in e.items():
                if k in o:
                    raise KeyError(f"Cannot flatten with duplicate key {k}")
                o[k] = v
        else:
            key = str(e)
            if key in o:
                raise KeyError(f"Cannot flatten with duplicate key {k}")
            o[key] = None
    return o


# def flatten_dicts_of_dicts(dicts: Mapping[str, Any]) -> Sequence[Any]:
#     """
#     Transform and object {K: {...}, L: {...}...} -> [{key:K, ....}, {key: L, ...}, ...]
#     """
#     o: List[Any] = []
#     for k, v in dicts.items():
#         if isinstance(v, list):
#             # if v is a list then add "key" to each list element
#             for lv in v:
#                 lv["key"] = k
#         else:
#             # add as "key" to dict
#             v["key"] = k

#         o.append(v)
#     return o


# def tuplify_list_of_dicts(dicts: Sequence[DictStrAny]) -> Sequence[DictStrAny]:
#     """
#     Transform list of dictionaries with single key into single dictionary of {"key": orig_key, "value": orig_value}
#     """
#     for d in dicts:
#         if len(d) > 1:
#             raise ValueError(f"Tuplify requires one key dicts {d}")
#         if len(d) == 1:
#             key = next(iter(d))
#             # delete key first to avoid name clashes
#             value = d[key]
#             del d[key]
#             d["key"] = key
#             d["value"] = value

#     return dicts


def flatten_list_or_items(_iter: Union[Iterable[TAny], Iterable[List[TAny]]]) -> Iterator[TAny]:
    for items in _iter:
        if isinstance(items, List):
            yield from items
        else:
            yield items


def concat_strings_with_limit(strings: List[str], separator: str, limit: int) -> Iterator[str]:
    """
    Generator function to concatenate strings.

    The function takes a list of strings and concatenates them into a single string such that the length of each
    concatenated string does not exceed a specified limit. It yields each concatenated string as it is created.
    The strings are separated by a specified separator.

    Args:
        strings (List[str]): The list of strings to be concatenated.
        separator (str): The separator to use between strings. Defaults to a single space.
        limit (int): The maximum length for each concatenated string.

    Yields:
        Generator[str, None, None]: A generator that yields each concatenated string.
    """

    if not strings:
        return

    current_length = len(strings[0])
    start = 0
    sep_len = len(separator)

    for i in range(1, len(strings)):
        if (
            current_length + len(strings[i]) + sep_len > limit
        ):  # accounts for the length of separator
            yield separator.join(strings[start:i])
            start = i
            current_length = len(strings[i])
        else:
            current_length += len(strings[i]) + sep_len  # accounts for the length of separator

    yield separator.join(strings[start:])


def graph_edges_to_nodes(
    edges: Sequence[Tuple[TAny, TAny]], directed: bool = True
) -> Dict[TAny, Set[TAny]]:
    """Converts a directed graph represented as a sequence of edges to a graph represented as a mapping from nodes a set of connected nodes.

    Isolated nodes are represented as edges to itself. If `directed` is `False`, each edge is duplicated but going in opposite direction.
    """
    graph: Dict[TAny, Set[TAny]] = {}
    for u, v in edges:
        if u not in graph:
            graph[u] = set()
        if v not in graph:
            graph[v] = set()
        if v != u:
            graph[u].add(v)
            if not directed:
                graph[v].add(u)

    return graph


def graph_find_scc_nodes(undag: Dict[TAny, Set[TAny]]) -> List[Set[TAny]]:
    """Finds and returns a list of sets of nodes in strongly connected components of a `undag` which is undirected

    To obtain undirected graph from edges use `graph_edges_to_nodes` function with `directed` argument `False`.
    """
    visited: Set[TAny] = set()
    components: List[Set[TAny]] = []

    def dfs(node: TAny, current_component: Set[TAny]) -> None:
        if node not in visited:
            visited.add(node)
            current_component.add(node)
            for neighbor in undag[node]:
                dfs(neighbor, current_component)

    for node in undag:
        if node not in visited:
            component: Set[TAny] = set()
            dfs(node, component)
            components.append(component)

    return components


def filter_env_vars(envs: List[str]) -> StrStr:
    return {k.lower(): environ[k] for k in envs if k in environ}


def update_dict_with_prune(dest: DictStrAny, update: StrAny) -> None:
    """Updates values that are both in `dest` and `update` and deletes `dest` values that are None in `update`"""
    for k, v in update.items():
        if v is not None:
            dest[k] = v
        elif k in dest:
            del dest[k]


def update_dict_nested(dst: TDict, src: StrAny, keep_dst_values: bool = False) -> TDict:
    """Merges `src` into `dst` key wise. Does not recur into lists. Values in `src` overwrite `dst` if both keys exit.
    Optionally (`keep_dst_values`) you can keep the `dst` value on conflict
    """
    # based on https://github.com/clarketm/mergedeep/blob/master/mergedeep/mergedeep.py

    def _is_recursive_merge(a: StrAny, b: StrAny) -> bool:
        both_mapping = isinstance(a, C_Mapping) and isinstance(b, C_Mapping)
        both_counter = isinstance(a, Counter) and isinstance(b, Counter)
        return both_mapping and not both_counter

    for key in src:
        if key in dst:
            if _is_recursive_merge(dst[key], src[key]):
                # If the key for both `dst` and `src` are both Mapping types (e.g. dict), then recurse.
                update_dict_nested(dst[key], src[key], keep_dst_values=keep_dst_values)
            elif dst[key] is src[key]:
                # If a key exists in both objects and the values are `same`, the value from the `dst` object will be used.
                pass
            else:
                if not keep_dst_values:
                    # if not keep then overwrite
                    dst[key] = src[key]
        else:
            # If the key exists only in `src`, the value from the `src` object will be used.
            dst[key] = src[key]
    return dst


def map_nested_in_place(func: AnyFun, _complex: TAny) -> TAny:
    """Applies `func` to all elements in `_dict` recursively, replacing elements in nested dictionaries and lists in place."""
    if isinstance(_complex, tuple):
        if hasattr(_complex, "_asdict"):
            _complex = _complex._asdict()
        else:
            _complex = list(_complex)  # type: ignore

    if isinstance(_complex, dict):
        for k, v in _complex.items():
            if isinstance(v, (dict, list, tuple)):
                _complex[k] = map_nested_in_place(func, v)
            else:
                _complex[k] = func(v)
    elif isinstance(_complex, list):
        for idx, _l in enumerate(_complex):
            if isinstance(_l, (dict, list, tuple)):
                _complex[idx] = map_nested_in_place(func, _l)
            else:
                _complex[idx] = func(_l)
    else:
        raise ValueError(_complex, "Not a complex type")
    return _complex


def is_interactive() -> bool:
    """
    Determine if the current environment is interactive.

    Returns:
        bool: True if interactive (e.g., REPL, IPython, Jupyter Notebook), False if running as a script.
    """
    import __main__ as main

    # When running as a script, the __main__ module has a __file__ attribute.
    # In an interactive environment, the __file__ attribute is absent.
    return not hasattr(main, "__file__")


def dict_remove_nones_in_place(d: Dict[Any, Any]) -> Dict[Any, Any]:
    for k in list(d.keys()):
        if d[k] is None:
            del d[k]
    return d


@contextmanager
def custom_environ(env: StrStr) -> Iterator[None]:
    """Temporarily set environment variables inside the context manager and
    fully restore previous environment afterwards
    """
    original_env = {key: os.getenv(key) for key in env}
    os.environ.update(env)
    try:
        yield
    finally:
        for key, value in original_env.items():
            if value is None:
                del os.environ[key]
            else:
                os.environ[key] = value


def with_custom_environ(f: TFun) -> TFun:
    @wraps(f)
    def _wrap(*args: Any, **kwargs: Any) -> Any:
        saved_environ = os.environ.copy()
        try:
            return f(*args, **kwargs)
        finally:
            os.environ.clear()
            os.environ.update(saved_environ)

    return _wrap  # type: ignore


def encoding_for_mode(mode: str) -> Optional[str]:
    if "b" in mode:
        return None
    else:
        return "utf-8"


def main_module_file_path() -> str:
    if len(sys.argv) > 0 and os.path.isfile(sys.argv[0]):
        return str(Path(sys.argv[0]))
    return None


@contextmanager
def set_working_dir(path: str) -> Iterator[str]:
    curr_dir = os.path.abspath(os.getcwd())
    try:
        if path:
            os.chdir(path)
        yield path
    finally:
        os.chdir(curr_dir)


@contextmanager
def multi_context_manager(managers: Sequence[ContextManager[Any]]) -> Iterator[Any]:
    """A context manager holding several other context managers. Enters and exists all of them. Yields from the last in the list"""
    try:
        rv: Any = None
        for manager in managers:
            rv = manager.__enter__()
        yield rv
    except Exception as ex:
        # release context manager
        for manager in managers:
            if isinstance(ex, StopIteration):
                manager.__exit__(None, None, None)
            else:
                manager.__exit__(type(ex), ex, None)
        raise
    else:
        for manager in managers:
            manager.__exit__(None, None, None)


def get_callable_name(f: AnyFun, name_attr: str = "__name__") -> Optional[str]:
    # check first if __name__ is present directly (function), if not then look for type name
    name: str = getattr(f, name_attr, None)
    if not name:
        name = getattr(f.__class__, name_attr, None)
    return name


def is_inner_callable(f: AnyFun) -> bool:
    """Checks if f is defined within other function"""
    # inner functions have full nesting path in their qualname
    return "<locals>" in get_callable_name(f, name_attr="__qualname__")


def obfuscate_pseudo_secret(pseudo_secret: str, pseudo_key: bytes) -> str:
    return base64.b64encode(
        bytes([_a ^ _b for _a, _b in zip(pseudo_secret.encode("utf-8"), pseudo_key * 250)])
    ).decode()


def reveal_pseudo_secret(obfuscated_secret: str, pseudo_key: bytes) -> str:
    return bytes(
        [
            _a ^ _b
            for _a, _b in zip(
                base64.b64decode(obfuscated_secret.encode("ascii"), validate=True), pseudo_key * 250
            )
        ]
    ).decode("utf-8")


def get_module_name(m: ModuleType) -> str:
    """Gets module name from module with a fallback for executing module __main__"""
    if m.__name__ == "__main__" and hasattr(m, "__file__"):
        module_file = os.path.basename(m.__file__)
        module_name, _ = os.path.splitext(module_file)
        return module_name
    return m.__name__.split(".")[-1]


def derives_from_class_of_name(o: object, name: str) -> bool:
    """Checks if object o has class of name in its derivation tree"""
    mro = type.mro(type(o))
    return any(t.__name__ == name for t in mro)


def compressed_b64encode(value: bytes) -> str:
    """Compress and b64 encode the given bytestring"""
    return base64.b64encode(zlib.compress(value, level=9)).decode("ascii")


def compressed_b64decode(value: str) -> bytes:
    """Decode a bytestring encoded with `compressed_b64encode`"""
    value_bytes = base64.b64decode(value, validate=True)
    return zlib.decompress(value_bytes)


def identity(x: TAny) -> TAny:
    return x


def increase_row_count(row_counts: RowCounts, counter_name: str, count: int) -> None:
    row_counts[counter_name] = row_counts.get(counter_name, 0) + count


def merge_row_counts(row_counts_1: RowCounts, row_counts_2: RowCounts) -> None:
    """merges row counts_2 into row_counts_1"""
    # only keys present in row_counts_2 are modifed
    for counter_name in row_counts_2.keys():
        row_counts_1[counter_name] = row_counts_1.get(counter_name, 0) + row_counts_2[counter_name]


def extend_list_deduplicated(original_list: List[Any], extending_list: Iterable[Any]) -> List[Any]:
    """extends the first list by the second, but does not add duplicates"""
    list_keys = set(original_list)
    for item in extending_list:
        if item not in list_keys:
            original_list.append(item)
    return original_list


@contextmanager
def maybe_context(manager: ContextManager[TAny]) -> Iterator[TAny]:
    """Allows context manager `manager` to be None by creating dummy context. Otherwise `manager` is used"""
    if manager is None:
        yield None
    else:
        with manager as ctx:
            yield ctx


def without_none(d: Mapping[TKey, Optional[TValue]]) -> Mapping[TKey, TValue]:
    """Return a new dict with all `None` values removed"""
    return {k: v for k, v in d.items() if v is not None}


def get_full_class_name(obj: Any) -> str:
    cls = obj.__class__
    module = cls.__module__
    # exclude 'builtins' for built-in types.
    if module is None or module == "builtins":
        return cls.__name__  #  type: ignore[no-any-return]
    return module + "." + cls.__name__  #  type: ignore[no-any-return]


def get_exception_trace(exc: BaseException) -> ExceptionTrace:
    """Get exception trace and additional information for DltException(s)"""
    trace: ExceptionTrace = {"message": str(exc), "exception_type": get_full_class_name(exc)}
    if exc.__traceback__:
        tb_extract = traceback.extract_tb(exc.__traceback__)
        trace["stack_trace"] = traceback.format_list(tb_extract)
    trace["is_terminal"] = isinstance(exc, TerminalException)

    # get attrs and other props
    if isinstance(exc, DltException):
        if exc.__doc__:
            trace["docstring"] = exc.__doc__
        attrs = exc.attrs()
        str_attrs = {}
        for k, v in attrs.items():
            if v is None:
                continue
            try:
                from dlt.common.json import json

                # must be json serializable, other attrs are skipped
                if not isinstance(v, str):
                    json.dumps(v)
                str_attrs[k] = v
            except Exception:
                continue
            # extract special attrs
            if k in ["load_id", "pipeline_name", "source_name", "resource_name", "job_id"]:
                trace[k] = v  # type: ignore[literal-required]

        trace["exception_attrs"] = str_attrs
    return trace


def get_exception_trace_chain(
    exc: BaseException, traces: List[ExceptionTrace] = None, seen: Set[int] = None
) -> List[ExceptionTrace]:
    """Get traces for exception chain. The function will recursively visit all __cause__ and __context__ exceptions. The top level
    exception trace is first on the list
    """
    traces = traces or []
    seen = seen or set()
    # prevent cycles
    if id(exc) in seen:
        return traces
    seen.add(id(exc))
    traces.append(get_exception_trace(exc))
    if exc.__cause__:
        return get_exception_trace_chain(exc.__cause__, traces, seen)
    elif exc.__context__:
        return get_exception_trace_chain(exc.__context__, traces, seen)
    return traces


def order_deduped(lst: List[Any]) -> List[Any]:
    """Returns deduplicated list preserving order of input elements.

    Only works for lists with hashable elements.
    """
    return list(dict.fromkeys(lst))
