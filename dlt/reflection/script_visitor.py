import inspect
import ast
from ast import NodeVisitor, unparse
from typing import Any, Dict, List

from dlt.common.reflection.utils import find_outer_func_def

import dlt.reflection.names as n


class PipelineScriptVisitor(NodeVisitor):
    def __init__(self, source: str):
        self.source = source
        self.source_lines: List[str] = ast._splitlines_no_ff(source)  # type: ignore

        self.mod_aliases: Dict[str, str] = {}
        self.func_aliases: Dict[str, str] = {}
        # self.source_aliases: Dict[str, str] = {}
        self.is_destination_imported: bool = False
        self.known_calls: Dict[str, List[inspect.BoundArguments]] = {}
        self.known_sources: Dict[str, ast.FunctionDef] = {}
        self.known_source_calls: Dict[str, List[ast.Call]] = {}
        self.known_resources: Dict[str, ast.FunctionDef] = {}
        self.known_resource_calls: Dict[str, List[ast.Call]] = {}
        # updated in post visit
        self.known_sources_resources: Dict[str, ast.FunctionDef] = {}
        self.known_sources_resources_calls: Dict[str, List[ast.Call]] = {}

    def visit_passes(self, tree: ast.AST) -> None:
        self._curr_pass = 1
        self.visit(tree)
        self._curr_pass = 2
        self.visit(tree)
        self._post_visit()

    def visit_Import(self, node: ast.Import) -> Any:
        if self._curr_pass == 1:
            # reflect on imported modules
            for alias in node.names:
                # detect dlt import
                if alias.name == n.DLT:
                    eff_name = alias.asname or alias.name
                    self.mod_aliases[eff_name] = alias.name
                    self._add_f_aliases(eff_name)
                if alias.name.startswith(f"{n.DLT}.") and alias.asname is None:
                    # this also imports dlt
                    self.mod_aliases[alias.name] = alias.name
                    self._add_f_aliases(alias.name)
                if alias.name.startswith(f"{n.DESTINATIONS}."):
                    self.is_destination_imported = True
        super().generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> Any:
        if self._curr_pass == 1:
            # reflect on pipeline functions and decorators
            if node.module == n.DLT:
                for alias in node.names:
                    if alias.name in n.DETECTED_FUNCTIONS:
                        self.func_aliases[alias.asname or alias.name] = alias.name
            if node.module == n.DESTINATIONS:
                self.is_destination_imported = True
        super().generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
        if self._curr_pass == 1:
            # find all sources and resources by inspecting decorators
            for deco in node.decorator_list:
                # decorators can be function calls, attributes or names
                if isinstance(deco, (ast.Name, ast.Attribute)):
                    alias_name = ast.unparse(deco).strip()
                elif isinstance(deco, ast.Call):
                    alias_name = ast.unparse(deco.func).strip()
                else:
                    raise ValueError(
                        self.source_segment(deco), type(deco), "Unknown decorator form"
                    )
                fn = self.func_aliases.get(alias_name)
                if fn == n.SOURCE:
                    self.known_sources[str(node.name)] = node
                elif fn == n.RESOURCE:
                    self.known_resources[str(node.name)] = node
                elif fn == n.TRANSFORMER:
                    self.known_resources[str(node.name)] = node
        super().generic_visit(node)

    def visit_Call(self, node: ast.Call) -> Any:
        if self._curr_pass == 2:
            # check if this is a call to any of known functions
            alias_name = ast.unparse(node.func).strip()
            fn = self.func_aliases.get(alias_name)
            if not fn:
                # try a fallback to "run" function that may be called on pipeline or source
                if isinstance(node.func, ast.Attribute) and node.func.attr == n.RUN:
                    fn = n.RUN
            if fn:
                # set parent to the outer function
                node.parent = find_outer_func_def(node)  # type: ignore
                sig = n.SIGNATURES[fn]
                try:
                    # bind the signature where the argument values are the corresponding ast nodes
                    bound_args = sig.bind(
                        *node.args, **{str(kwd.arg): kwd.value for kwd in node.keywords}
                    )
                    bound_args.apply_defaults()
                    # print(f"ALIAS: {alias_name} of {self.func_aliases.get(alias_name)} with {bound_args}")
                    fun_calls = self.known_calls.setdefault(fn, [])
                    fun_calls.append(bound_args)
                except TypeError:
                    # skip the signature
                    pass
            else:
                # check if this is a call to any known source
                if alias_name in self.known_sources or alias_name in self.known_resources:
                    # set parent to the outer function
                    node.parent = find_outer_func_def(node)  # type: ignore
                    if alias_name in self.known_sources:
                        decorated_calls = self.known_source_calls.setdefault(alias_name, [])
                    else:
                        decorated_calls = self.known_resource_calls.setdefault(alias_name, [])
                    decorated_calls.append(node)
        # visit the children
        super().generic_visit(node)

    def _post_visit(self) -> None:
        self.known_sources_resources = self.known_sources.copy()
        self.known_sources_resources.update(self.known_resources)
        self.known_sources_resources_calls = self.known_source_calls.copy()
        self.known_sources_resources_calls.update(self.known_resource_calls)

    def source_segment(self, node: ast.AST) -> str:
        # TODO: this must cache parsed source. right now the full source is tokenized on every call
        return ast.get_source_segment(self.source, node)

    def _add_f_aliases(self, module_name: str) -> None:
        for fn in n.DETECTED_FUNCTIONS:
            self.func_aliases[f"{module_name}.{fn}"] = fn
