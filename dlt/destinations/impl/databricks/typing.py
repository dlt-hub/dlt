from typing import Optional, List, Dict, Union, Literal
from dlt.common.schema.typing import TColumnSchema, TColumnHint

TDatabricksColumnHint = Union[TColumnHint, Literal["foreign_key"]]


class TDatabricksColumnSchema(TColumnSchema, total=False):
    column_comment: Optional[str]
    column_tags: Optional[List[Union[str, Dict[str, str]]]]


TDatabricksTableSchemaColumns = Dict[str, TDatabricksColumnSchema]
