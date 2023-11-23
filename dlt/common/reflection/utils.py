import ast
import inspect
import astunparse
from typing import Any, Dict, List, Optional, Sequence, Tuple

from dlt.common.typing import AnyFun


def get_literal_defaults(node: ast.FunctionDef) -> Dict[str, str]:
    """Extract defaults from function definition node literally, as pieces of source code"""
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
    """Finds the function definition node for function f by parsing the source code of the f's module"""
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


def find_outer_func_def(node: ast.AST) -> Optional[ast.FunctionDef]:
    """Finds the outer function definition node in which the 'node' is contained. Returns None if 'node' is toplevel."""
    if not hasattr(node, "parent"):
        raise ValueError("No parent information in node, not enabled in visitor", node)
    while not isinstance(node.parent, ast.FunctionDef):
        if node.parent is None:
            return None
        node = node.parent
    return node  # type: ignore


def set_ast_parents(tree: ast.AST) -> None:
    """Walks AST tree and sets the `parent` attr in each node to the node's parent. Toplevel nodes (parent is a `tree`) have the `parent` attr set to None."""
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            child.parent = node if node is not tree else None  # type: ignore


def creates_func_def_name_node(func_def: ast.FunctionDef, source_lines: Sequence[str]) -> ast.Name:
    """Recreate function name as a ast.Name with known source code location"""
    func_name = ast.Name(func_def.name)
    func_name.lineno = func_name.end_lineno = func_def.lineno
    func_name.col_offset = source_lines[func_name.lineno - 1].index(
        func_def.name
    )  # find where function name starts
    func_name.end_col_offset = func_name.col_offset + len(func_def.name)
    return func_name


def rewrite_python_script(
    source_script_lines: List[str], transformed_nodes: List[Tuple[ast.AST, ast.AST]]
) -> List[str]:
    """Replaces all the nodes present in `transformed_nodes` in the `script_lines`. The `transformed_nodes` is a tuple where the first element
    is must be a node with full location information created out of `script_lines`"""
    script_lines: List[str] = []
    last_line = -1
    last_offset = -1
    # sort transformed nodes by line and offset
    for node, t_value in sorted(transformed_nodes, key=lambda n: (n[0].lineno, n[0].col_offset)):
        # do we have a line changed
        if last_line != node.lineno - 1:
            # add remainder from the previous line
            if last_offset >= 0:
                script_lines.append(source_script_lines[last_line][last_offset:])
            # add all new lines from previous line to current
            script_lines.extend(source_script_lines[last_line + 1 : node.lineno - 1])
            # add trailing characters until node in current line starts
            script_lines.append(source_script_lines[node.lineno - 1][: node.col_offset])
        elif last_offset >= 0:
            # no line change, add the characters from the end of previous node to the current
            script_lines.append(source_script_lines[last_line][last_offset : node.col_offset])

        # replace node value
        script_lines.append(astunparse.unparse(t_value).strip())
        last_line = node.end_lineno - 1
        last_offset = node.end_col_offset

    # add all that was missing
    if last_offset >= 0:
        script_lines.append(source_script_lines[last_line][last_offset:])
    script_lines.extend(source_script_lines[last_line + 1 :])
    return script_lines


def evaluate_node_literal(node: ast.AST) -> Any:
    try:
        return ast.literal_eval(node)
    except ValueError:
        return None


def get_module_docstring(module_script: str) -> str:
    module = ast.parse(module_script)
    docstring = ast.get_docstring(module)
    if docstring is None:
        docstring = ""
    return docstring
