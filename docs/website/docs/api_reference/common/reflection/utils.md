---
sidebar_label: utils
title: common.reflection.utils
---

#### get\_literal\_defaults

```python
def get_literal_defaults(node: ast.FunctionDef) -> Dict[str, str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/reflection/utils.py#L9)

Extract defaults from function definition node literally, as pieces of source code

#### get\_func\_def\_node

```python
def get_func_def_node(f: AnyFun) -> ast.FunctionDef
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/reflection/utils.py#L33)

Finds the function definition node for function f by parsing the source code of the f's module

#### find\_outer\_func\_def

```python
def find_outer_func_def(node: ast.AST) -> Optional[ast.FunctionDef]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/reflection/utils.py#L49)

Finds the outer function definition node in which the 'node' is contained. Returns None if 'node' is toplevel.

#### set\_ast\_parents

```python
def set_ast_parents(tree: ast.AST) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/reflection/utils.py#L60)

Walks AST tree and sets the `parent` attr in each node to the node's parent. Toplevel nodes (parent is a `tree`) have the `parent` attr set to None.

#### creates\_func\_def\_name\_node

```python
def creates_func_def_name_node(func_def: ast.FunctionDef,
                               source_lines: Sequence[str]) -> ast.Name
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/reflection/utils.py#L67)

Recreate function name as a ast.Name with known source code location

#### rewrite\_python\_script

```python
def rewrite_python_script(
        source_script_lines: List[str],
        transformed_nodes: List[Tuple[ast.AST, ast.AST]]) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/reflection/utils.py#L76)

Replaces all the nodes present in `transformed_nodes` in the `script_lines`. The `transformed_nodes` is a tuple where the first element
is must be a node with full location information created out of `script_lines`

