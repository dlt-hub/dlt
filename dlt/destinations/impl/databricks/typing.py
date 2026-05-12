from typing import Dict, List, Literal, Optional, Union

from dlt.common.schema.typing import TColumnHint, TColumnSchema

TDatabricksInsertApi = Literal["copy_into", "zerobus"]
TDatabricksColumnHint = Union[TColumnHint, Literal["foreign_key"]]


class TDatabricksColumnSchema(TColumnSchema, total=False):
    column_comment: Optional[str]
    column_tags: Optional[List[Union[str, Dict[str, str]]]]


TDatabricksTableSchemaColumns = Dict[str, TDatabricksColumnSchema]
