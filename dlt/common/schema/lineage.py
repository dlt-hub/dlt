# NOTE: this should live in libs/sqlglot

from typing import List, Dict, Optional, TYPE_CHECKING, Tuple, Union, Any

import sqlglot
from sqlglot import Schema, maybe_parse
from sqlglot.optimizer import build_scope, qualify
from sqlglot.lineage import lineage, Scope


def _build_scope(sql: str, dialect: str, schema: Dict | Schema, **kwargs) -> Scope:
    expression = maybe_parse(sql, dialect=dialect)

    expression = qualify.qualify(
        expression,
        dialect=dialect,
        schema=schema,
        **{"validate_qualify_columns": False, "identify": False, **kwargs},  # type: ignore
    )

    return build_scope(expression)


def get_result_column_names_from_select(
    sql: str,
    schema: Optional[Union[Dict[str, Any], Schema]] = None,
    dialect: Optional[str] = None,
    scope: Optional[Scope] = None,
    **kwargs
) -> List[str]:
    """given an sql select statement, and a schema, return the list of column names in the result"""
    """NOTE: code mostly taken from lineage.py in sqlglot"""

    scope = _build_scope(sql, dialect, schema, **kwargs) if scope is None else scope

    if not scope:
        raise Exception("Expression does not seem to be a valid select statement")
    selected_columns = [select.alias_or_name for select in scope.expression.selects]
    return selected_columns


def get_column_origin(
    column_name: str, sql: str, schema: Dict | Schema, dialect: str = None
) -> Tuple[str, str]:
    """given a column name, an sql select statement, and a schema, return the origin of the column as a tuple of (table_name, column_name)"""
    """ returns None, None if original can't be found"""

    lineage_graph = lineage(column=column_name, sql=sql, schema=schema, dialect=dialect)

    origin_table_name = None
    origin_column_name = None

    for node in lineage_graph.walk():
        print(type(node.expression))
        if type(node.expression) == sqlglot.expressions.Table:
            origin_table_name = node.expression.name

        if type(node.expression) == sqlglot.expressions.Alias:
            # search identifier in the expression chain
            identifier = node.expression.this
            while type(identifier) != sqlglot.expressions.Identifier and identifier is not None:
                identifier = identifier.this
            origin_column_name = identifier.name if identifier else None

    if not origin_table_name:
        return None, None
    return origin_table_name, origin_column_name


def get_result_origins(
    sql: str, schema: Union[Dict, Schema] = None, dialect: str = None
) -> List[Tuple[str, Tuple[str, str]]]:
    """given a schema and a sql select statement, return a list of tuples of (column_name, (origin_table_name, origin_column_name)) for each column name in the result"""
    scope = _build_scope(sql, dialect, schema)
    selected_columns = get_result_column_names_from_select(sql, schema, dialect, scope)

    # build result
    result: List[Tuple[str, Tuple[str, str]]] = []

    # star select without schema is not possible, raise?
    if not schema and selected_columns[0] == "*":
        return []

    # get origins
    for column_name in selected_columns:
        result.append((column_name, get_column_origin(column_name, sql, schema, dialect)))
    return result
