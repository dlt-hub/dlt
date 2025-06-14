# from typing import Any, Optional, Sequence, Set, Union

# from sqlglot import exp as sge

# from dlt.common.exceptions import DltException
# from dlt.common.schema.typing import C_DLT_LOAD_ID

# # TODO this constant should probably live elsewhere
# LOAD_ID_COL_ON_LOADS_TABLE = "load_id"


# def filter_by_status(query: sge.Select, *, status: Union[int, Sequence[int], None]) -> sge.Select:
#     """Add `status` filter to a SGLGlot Select expression."""
#     if isinstance(status, int):
#         query = query.where(sge.to_identifier("status").eq(status))
#     elif isinstance(status, Sequence):
#         if len(status) < 1:
#             raise DltException(
#                 f"Invalid argument `status`. Reason: `len(status) < 1`. Received `{status}`"
#             )
#         elif not all(isinstance(s, int) for s in status):
#             raise DltException(
#                 f"Invalid argument `status`. Reason: not all values are `int`. Received `{status}`"
#             )
#         query = query.where(sge.to_identifier("status").isin(*status))
#     elif status is not None:
#         raise DltException(
#             f"The `status` argument should be `int`, `Sequence[int]` or `None`. Received `{status}`"
#         )
#     return query


# # TODO use @overload to type hint valid signatures
# def filter_min_max(
#     query: sge.Select,
#     *,
#     lt: Optional[Any] = None,
#     le: Optional[Any] = None,
#     gt: Optional[Any] = None,
#     ge: Optional[Any] = None,
# ) -> sge.Select:
#     # TODO add exception handling to catch invalid user input (e.g., `le < ge`)
#     if ge is not None and gt is not None:
#         raise ValueError()

#     if le is not None and lt is not None:
#         raise ValueError()

#     load_id_col = sge.to_identifier(LOAD_ID_COL_ON_LOADS_TABLE)
#     conditions = []

#     # between is a special condition: (ge, le)
#     if ge is not None and le is not None:
#         cond = load_id_col.between(low=ge, high=le)
#         conditions.append(cond)
#     else:
#         # 8 - 1 combinations: (gt,), (ge,), (lt,), (le,), (gt, lt), (gt, le), (ge, lt); and between() == (ge, le)
#         if ge is not None or gt is not None:
#             cond = load_id_col >= ge if ge is not None else load_id_col > gt
#             conditions.append(cond)

#         if le is not None or lt is not None:
#             cond = load_id_col <= le if le is not None else load_id_col < lt
#             conditions.append(cond)

#     query = query.where(*conditions)
#     return query


# def set_limit(query: sge.Select, *, limit: Optional[int]) -> sge.Select:
#     if isinstance(limit, int):
#         if limit < 1:
#             raise DltException(f"Invalid argument `limit`. Reason: `limit < 1`. Received `{limit}`")
#         query = query.limit(limit)
#     elif limit is not None:
#         raise DltException(f"The `limit` argument should be `int` or `None`. Received `{limit}`")
#     return query


# # TODO use @overload to document valid signature
# # for example, passing min, max, and limit is ambiguous and shouldn't be possible
# def list_load_ids_expr(
#     loads_table_name: str = "_dlt_loads",
#     lt: Optional[Any] = None,
#     le: Optional[Any] = None,
#     gt: Optional[Any] = None,
#     ge: Optional[Any] = None,
#     status: Union[int, Sequence[int], None] = None,
#     limit: Optional[int] = None,
# ) -> sge.Select:
#     query = (
#         sge.select(sge.to_identifier(LOAD_ID_COL_ON_LOADS_TABLE))
#         .from_(loads_table_name)
#         .order_by(f"{LOAD_ID_COL_ON_LOADS_TABLE} DESC")
#     )
#     query = filter_by_status(query, status=status)
#     query = filter_min_max(query, lt=lt, le=le, gt=gt, ge=ge)
#     query = set_limit(query, limit=limit)
#     return query


# def latest_load_id_expr(
#     loads_table_name: str = "_dlt_loads", status: Union[int, Sequence[int], None] = None
# ) -> sge.Select:
#     query = sge.select(
#         sge.func("max", sge.to_identifier(LOAD_ID_COL_ON_LOADS_TABLE)).as_(
#             LOAD_ID_COL_ON_LOADS_TABLE
#         )
#     ).from_(loads_table_name)
#     query = filter_by_status(query, status=status)
#     return query


# def filter_root_table_expr(
#     table_name: str,
#     load_ids: Union[Sequence[str], Set[str]],
# ) -> sge.Select:
#     query = sge.select("*").from_(sge.to_identifier(table_name))

#     # load_ids is false-y, match no rows and return an empty set
#     if not load_ids:
#         query = query.where("1 = 0")
#     else:
#         query = query.where(sge.to_identifier(C_DLT_LOAD_ID).isin(*load_ids))

#     return query


# def filter_child_table_expr(
#     table_name: str,
#     table_root_key: str,
#     root_table_name: str,
#     root_table_row_key: str,
#     load_ids: Union[Sequence[str], Set[str]],
# ) -> sge.Select:
#     root_table_expr = filter_root_table_expr(table_name=root_table_name, load_ids=load_ids)
#     root_alias = f"{root_table_name}_filtered"
#     join_condition = f"{table_name}.{table_root_key} = {root_alias}.{root_table_row_key}"
#     query = (
#         sge.select(f"{table_name}.*")
#         .from_(sge.to_identifier(table_name))
#         .join(
#             root_table_expr,
#             on=join_condition,
#             join_type="inner",
#             join_alias=root_alias,
#         )
#     )
#     return query
