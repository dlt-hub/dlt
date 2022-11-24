import re
import ast
import inspect
import astunparse
from typing import Dict, List, Optional

from dlt.common.typing import AnyFun


def get_literal_defaults(node: ast.FunctionDef) -> Dict[str, str]:
    defaults: List[ast.expr] = []
    if node.args.defaults:
        defaults.extend(node.args.defaults)
    if node.args.kw_defaults:
        defaults.extend(node.args.kw_defaults)
    args: List[ast.arg] = []
    if node.args.posonlyargs:
        args.extend(node.args.posonlyargs)
    if node.args.args:
        args.extend(node.args.args)
    if node.args.kwonlyargs:
        args.extend(node.args.kwonlyargs)

    # zip args and defaults
    literal_defaults: Dict[str, str] = {}
    for arg, default in zip(reversed(args), reversed(defaults)):
        if default:
            literal_defaults[str(arg.arg)] = astunparse.unparse(default).strip()

    return literal_defaults


def get_func_def_node(f: AnyFun) -> ast.FunctionDef:
    # this will be slow
    source, lineno = inspect.findsource(inspect.unwrap(f))

    for node in ast.walk(ast.parse("".join(source))):
        if isinstance(node, ast.FunctionDef):
            f_lineno = node.lineno - 1
            # get line number of first decorator
            if node.decorator_list:
                f_lineno = node.decorator_list[0].lineno - 1
            # line number and function name must match
            if f_lineno == lineno and node.name == f.__name__:
                return node
    return None


def get_outer_func_def(node: ast.AST) -> Optional[ast.FunctionDef]:
    if not hasattr(node, "parent"):
        raise ValueError("No parent information in node, not enabled in visitor", node)
    while not isinstance(node.parent, ast.FunctionDef):  # type: ignore
        if node.parent is None:  # type: ignore
            return None
        node = node.parent  # type: ignore
    return node  # type: ignore


def set_ast_parents(tree: ast.AST) -> None:
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            child.parent = node if node is not tree else None  # type: ignore
