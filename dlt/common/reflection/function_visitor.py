import ast
from ast import NodeVisitor
from typing import Any

class FunctionVisitor(NodeVisitor):
    def __init__(self, source: str):
        self.source = source
        self.top_func: ast.FunctionDef = None

    def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
        if not self.top_func:
            self.top_func = node
        super().generic_visit(node)
