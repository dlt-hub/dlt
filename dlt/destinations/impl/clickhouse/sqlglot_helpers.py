import sqlglot
from sqlglot import exp
from typing import List


def _add_properties(properties: sqlglot.expressions.Properties, new_properties: List[sqlglot.exp.Expression]) -> sqlglot.expressions.Properties:
    if properties is None:
        properties = exp.Properties()
        properties.set("expressions", new_properties)
    else:
        properties.expressions.extend(new_properties)
    return properties


# def _add_global(node: exp.Expression) -> exp.Expression:
#     if isinstance(node, exp.Join) and not node.args.get("global"):
#         node.set("global", True)
#     return node


def _has_expression(properties: sqlglot.expressions.Properties, expression_type: sqlglot.expressions.Expression) -> bool:
    if properties is not None and properties.expressions is not None and len(properties.expressions) > 0:
        for i, prop in enumerate(properties.expressions):
            if isinstance(prop, expression_type):
                print(f"Found {expression_type.__name__} at index {i}: {prop}")
                return True
    return False
