from typing import Optional

from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.libs.pyarrow import (
    get_column_type_from_py_arrow,
    get_py_arrow_datatype,
    pyarrow as pa,
)

from dlt.common.schema.typing import TColumnSchema, TColumnType
from dlt.destinations.type_mapping import TypeMapperImpl


# TODO: TypeMapperImpl must be a Generic where pa.DataType will be a concrete class
class LanceTypeMapper(TypeMapperImpl):
    def to_destination_type(self, column: TColumnSchema, table: PreparedTableSchema) -> pa.DataType:
        return get_py_arrow_datatype(column, self.capabilities, "UTC")

    def from_destination_type(
        self,
        db_type: pa.DataType,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
    ) -> TColumnType:
        return get_column_type_from_py_arrow(db_type)
