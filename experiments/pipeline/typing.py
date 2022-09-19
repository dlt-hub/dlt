from typing import Literal


TPipelineStep = Literal["extract", "normalize", "load"]


# class TTableSchema(TTableSchema, total=False):
#     name: Optional[str]
#     description: Optional[str]
#     write_disposition: Optional[TWriteDisposition]
#     table_sealed: Optional[bool]
#     parent: Optional[str]
#     filters: Optional[TRowFilters]
#     columns: TTableSchemaColumns