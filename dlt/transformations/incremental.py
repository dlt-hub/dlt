# from typing import Union

# from sqlglot import expressions as sge

# import dlt
# from dlt.common.exceptions import SourceSectionNotAvailable
# from dlt.extract.incremental import Incremental


# def _filter_using_incremental(query: sge.Select, incremental: Incremental) -> sge.Select:
#     """

#     This function only works from the context of a transformation.
#     """
#     try:
#         state = dlt.current.state()
#         transformation_name = dlt.current.resource_name()
#     except SourceSectionNotAvailable:
#         raise

#     cursor = state["resources"][transformation_name]["incremental"][incremental.cursor_path]
#     initial_value = cursor["initial_value"]
#     last_value = cursor["last_value"]
#     # TODO do some check on the cursor_path format
#     column_name = incremental.cursor_path

#     conditions = []

#     if incremental.range_start == "closed":
#         start_condition = sge.column(column_name) > initial_value
#     else:
#         start_condition = sge.column(column_name) >= initial_value
#     conditions.append(start_condition)

#     if last_value != "":
#         if incremental.range_end == "closed":
#             end_condition = sge.column(column_name) < last_value
#         else:
#             end_condition = sge.column(column_name) <= last_value
#         conditions.append(end_condition)

#     return query.where(*conditions)


# """NOTES
# # Ideal dataset interface
# 1. Dataset is defined by a dlt Schema and connects to destination
# 2. Can convert the dlt Schema to a SQLGlot schema
# 3. Transformation function returns a SQLGlot query
# 4.


# # Incremental
# 1. Use dlt.sources.incremental mechanism
# 2. builds an SQLGlot expression to filter a specific table
# 3. the incremental expression is added as a CTE


# """
