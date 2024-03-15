"""This module provides a utility to patch source code of pipeline scripts
to remove all `pipeline.run` calls to avoid any undesired side effects when
running the pipeline with given resources and sources
"""
import typing as t

from dlt.reflection import names as rn
from dlt.reflection.script_visitor import PipelineScriptVisitor


class SourcePatcher:
    def __init__(self, visitor: PipelineScriptVisitor) -> None:
        self.visitor = visitor

    def patch(self) -> str:
        """Removes all pipeline.run nodes and return patched source code"""
        # Extract pipeline.run nodes
        pipeline_runs = self.visitor.known_calls_with_nodes.get(rn.RUN)
        run_nodes = [run_node for _args, run_node in pipeline_runs]

        # Copy original source
        script_lines: t.List[str] = self.visitor.source_lines[:]
        stub = '""" run method replaced """'

        for node in run_nodes:
            # if it is a one liner
            if node.lineno == node.end_lineno:
                script_lines[node.lineno] = None
            else:
                line_of_code = script_lines[node.lineno - 1]
                script_lines[node.lineno - 1] = self.restore_indent(line_of_code) + stub
                start = node.lineno + 1
                while start <= node.end_lineno:
                    script_lines[start - 1] = None
                    start += 1

        result = [line.rstrip() for line in script_lines if line]
        return "\n".join(result)

    def restore_indent(self, line: str) -> str:
        indent = ""
        for ch in line:
            if ch != " ":
                break

            indent += " "
        return indent
