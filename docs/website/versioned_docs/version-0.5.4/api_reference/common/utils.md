---
sidebar_label: utils
title: common.utils
---

## uniq\_id

```python
def uniq_id(len_: int = 16) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L67)

Returns a hex encoded crypto-grade string of random bytes with desired len_

## uniq\_id\_base64

```python
def uniq_id_base64(len_: int = 16) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L72)

Returns a base64 encoded crypto-grade string of random bytes with desired len_

## many\_uniq\_ids\_base64

```python
def many_uniq_ids_base64(n_ids: int, len_: int = 16) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L77)

Generate `n_ids` base64 encoded crypto-grade strings of random bytes with desired len_.
This is more performant than calling `uniq_id_base64` multiple times.

## digest128

```python
def digest128(v: str, len_: int = 15) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L89)

Returns a base64 encoded shake128 hash of str `v` with digest of length `len_` (default: 15 bytes = 20 characters length)

## digest128b

```python
def digest128b(v: bytes, len_: int = 15) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L98)

Returns a base64 encoded shake128 hash of bytes `v` with digest of length `len_` (default: 15 bytes = 20 characters length)

## flatten\_list\_of\_str\_or\_dicts

```python
def flatten_list_of_str_or_dicts(
        seq: Sequence[Union[StrAny, str]]) -> DictStrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L133)

Transforms a list of objects or strings [{K: {...}}, L, ...] -> {K: {...}, L: None, ...}

## concat\_strings\_with\_limit

```python
def concat_strings_with_limit(strings: List[str], separator: str,
                              limit: int) -> Iterator[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L160)

Generator function to concatenate strings.

The function takes a list of strings and concatenates them into a single string such that the length of each
concatenated string does not exceed a specified limit. It yields each concatenated string as it is created.
The strings are separated by a specified separator.

**Arguments**:

- `strings` _List[str]_ - The list of strings to be concatenated.
- `separator` _str_ - The separator to use between strings. Defaults to a single space.
- `limit` _int_ - The maximum length for each concatenated string.
  

**Yields**:

  Generator[str, None, None]: A generator that yields each concatenated string.

## graph\_edges\_to\_nodes

```python
def graph_edges_to_nodes(edges: Sequence[Tuple[TAny, TAny]],
                         directed: bool = True) -> Dict[TAny, Set[TAny]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L197)

Converts a directed graph represented as a sequence of edges to a graph represented as a mapping from nodes a set of connected nodes.

Isolated nodes are represented as edges to itself. If `directed` is `False`, each edge is duplicated but going in opposite direction.

## graph\_find\_scc\_nodes

```python
def graph_find_scc_nodes(undag: Dict[TAny, Set[TAny]]) -> List[Set[TAny]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L218)

Finds and returns a list of sets of nodes in strongly connected components of a `undag` which is undirected

To obtain undirected graph from edges use `graph_edges_to_nodes` function with `directed` argument `False`.

## update\_dict\_with\_prune

```python
def update_dict_with_prune(dest: DictStrAny, update: StrAny) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L246)

Updates values that are both in `dest` and `update` and deletes `dest` values that are None in `update`

## update\_dict\_nested

```python
def update_dict_nested(dst: TDict,
                       src: TDict,
                       copy_src_dicts: bool = False) -> TDict
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L255)

Merges `src` into `dst` key wise. Does not recur into lists. Values in `src` overwrite `dst` if both keys exit.
Only `dict` and its subclasses are updated recursively. With `copy_src_dicts`, dict key:values will be deep copied,
otherwise, both dst and src will keep the same references.

## clone\_dict\_nested

```python
def clone_dict_nested(src: TDict) -> TDict
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L278)

Clones `src` structure descending into nested dicts. Does not descend into mappings that are not dicts ie. specs instances.
Compared to `deepcopy` does not clone any other objects. Uses `update_dict_nested` internally

## map\_nested\_in\_place

```python
def map_nested_in_place(func: AnyFun, _complex: TAny) -> TAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L285)

Applies `func` to all elements in `_dict` recursively, replacing elements in nested dictionaries and lists in place.

## is\_interactive

```python
def is_interactive() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L310)

Determine if the current environment is interactive.

**Returns**:

- `bool` - True if interactive (e.g., REPL, IPython, Jupyter Notebook), False if running as a script.

## custom\_environ

```python
@contextmanager
def custom_environ(env: StrStr) -> Iterator[None]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L332)

Temporarily set environment variables inside the context manager and
fully restore previous environment afterwards

## multi\_context\_manager

```python
@contextmanager
def multi_context_manager(
        managers: Sequence[ContextManager[Any]]) -> Iterator[Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L386)

A context manager holding several other context managers. Enters and exists all of them. Yields from the last in the list

## is\_inner\_callable

```python
def is_inner_callable(f: AnyFun) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L414)

Checks if f is defined within other function

## get\_module\_name

```python
def get_module_name(m: ModuleType) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L437)

Gets module name from module with a fallback for executing module __main__

## derives\_from\_class\_of\_name

```python
def derives_from_class_of_name(o: object, name: str) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L446)

Checks if object o has class of name in its derivation tree

## compressed\_b64encode

```python
def compressed_b64encode(value: bytes) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L452)

Compress and b64 encode the given bytestring

## compressed\_b64decode

```python
def compressed_b64decode(value: str) -> bytes
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L457)

Decode a bytestring encoded with `compressed_b64encode`

## merge\_row\_counts

```python
def merge_row_counts(row_counts_1: RowCounts, row_counts_2: RowCounts) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L471)

merges row counts_2 into row_counts_1

## extend\_list\_deduplicated

```python
def extend_list_deduplicated(
        original_list: List[Any],
        extending_list: Iterable[Any],
        normalize_f: Callable[[str], str] = str.__call__) -> List[Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L478)

extends the first list by the second, but does not add duplicates

## maybe\_context

```python
@contextmanager
def maybe_context(manager: ContextManager[TAny]) -> Iterator[TAny]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L492)

Allows context manager `manager` to be None by creating dummy context. Otherwise `manager` is used

## without\_none

```python
def without_none(d: Mapping[TKey, Optional[TValue]]) -> Mapping[TKey, TValue]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L501)

Return a new dict with all `None` values removed

## get\_exception\_trace

```python
def get_exception_trace(exc: BaseException) -> ExceptionTrace
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L515)

Get exception trace and additional information for DltException(s)

## get\_exception\_trace\_chain

```python
def get_exception_trace_chain(exc: BaseException,
                              traces: List[ExceptionTrace] = None,
                              seen: Set[int] = None) -> List[ExceptionTrace]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L549)

Get traces for exception chain. The function will recursively visit all __cause__ and __context__ exceptions. The top level
exception trace is first on the list

## order\_deduped

```python
def order_deduped(lst: List[Any]) -> List[Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/utils.py#L569)

Returns deduplicated list preserving order of input elements.

Only works for lists with hashable elements.

