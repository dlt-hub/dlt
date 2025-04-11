from typing import Optional

import sqlglot
import sqlglot.expressions as sge
from sqlglot.expressions import DataType as dt, DATA_TYPE
from sqlglot.schema import Schema as SQLGlotSchema, ensure_schema
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify

from dlt.common.schema.typing import TTableSchemaColumns, TColumnType
from dlt.destinations.type_mapping import TypeMapperImpl
from dlt.destinations.dataset import ReadableDBAPIDataset


# TODO maybe we don't need a full on type mapper
class SQLGlotTypeMapper(TypeMapperImpl):
    sct_to_unbound_dbt = {
        "json": "JSON",
        "text": "TEXT",
        "double": "DOUBLE",
        "bool": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMPTZ",
        "bigint": "BIGINT",
        "binary": "BINARY",
        "time": "TIME",  # should we use TIMETZ ?
    }

    # what is that for?
    sct_to_dbt = {}

    dbt_to_sct = {
        dt.Type.BINARY: "binary",
        dt.Type.BOOLEAN: "bool",
        **{typ: "json" for typ in dt.NESTED_TYPES},  # array and struct
        **{typ: "text" for typ in dt.TEXT_TYPES},
        **{typ: "decimal" for typ in dt.REAL_TYPES},
        **{typ: "double" for typ in dt.FLOAT_TYPES},
        **{typ: "bigint" for typ in dt.INTEGER_TYPES},  # signed and unsigned
        **{
            typ: "date" for typ in dt.TEMPORAL_TYPES
            if typ in [
                dt.Type.DATE,
                dt.Type.DATE32,
                dt.Type.DATETIME,
                dt.Type.DATETIME2,
                dt.Type.DATETIME64,
                dt.Type.SMALLDATETIME,
            ]
        },
        **{
            typ: "timestamp" for typ in dt.TEMPORAL_TYPES
            if typ in [
                dt.Type.TIMESTAMP,
                dt.Type.TIMESTAMPNTZ,
                dt.Type.TIMESTAMPLTZ,
                dt.Type.TIMESTAMPTZ,
                dt.Type.TIMESTAMP_MS,
                dt.Type.TIMESTAMP_NS,
                dt.Type.TIMESTAMP_S,
            ]
        },
        **{typ: "time" for typ in [dt.Type.TIME, dt.Type.TIMETZ]},
    }  # type: ignore
    # NOTE not rquired at the moment
    # def to_db_integer_type(self, column: TColumnSchema, table: PreparedTableSchema = None) -> str:

    # def to_db_datetime_type(self, column: TColumnSchema, table: PreparedTableSchema = None) -> str:

    def from_destination_type(
        self, db_type: DATA_TYPE, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        db_type = dt.build(db_type)
        # NOTE can retrieve nullable from `db_type.arg_types["nullable"]`
        return super().from_destination_type(db_type.this, precision, scale)

    
def create_sqlglot_schema(dataset: ReadableDBAPIDataset) -> SQLGlotSchema:
    type_mapper = dataset._destination.capabilities().get_type_mapper()

    mapping_schema = {}
    for table_name, table in dataset.schema.tables.items():
        if mapping_schema.get(table_name) is None:
            mapping_schema[table_name] = {}

        for column_name, column in table["columns"].items():  # type: ignore
            mapping_schema[table_name][column_name] = type_mapper.to_destination_type(column, table)

    # NOTE we can use either: {table: {col: type}} or {db: ...} or {catalog: {db: ...}}
    # catalog_name = dataset.sql_client.catalog_name
    # dataset_name = dataset.dataset_name
    return ensure_schema(mapping_schema)


def get_sqlglot_dialect(dataset: ReadableDBAPIDataset) -> str:
    return dataset._destination.capabilities().sqlglot_dialect


def compute_columns_schema(
    sql_query: str,
    sqlglot_schema: SQLGlotSchema,
    dialect: str,
    type_mapper: TypeMapperImpl,
) -> TTableSchemaColumns:
    expression = sqlglot.maybe_parse(sql_query, dialect=dialect)
    assert isinstance(expression, sge.Select)

    expression = qualify(expression, schema=sqlglot_schema, dialect=dialect)
    expression = annotate_types(expression, schema=sqlglot_schema, dialect=dialect)

    dlt_columns_schema: TTableSchemaColumns = {
        str(column.output_name): type_mapper.from_destination_type(column.type, None, None)
        for column in expression.selects
    } # type: ignore

    return dlt_columns_schema

if __name__ == "__main__":
    import dlt
    from dlt.sources._single_file_templates.fruitshop_pipeline import fruitshop as fruitshop_source

    # typical user code
    pipeline = dlt.pipeline(pipeline_name="test_fruitshop", destination="duckdb", dev_mode=True)
    pipeline.run(fruitshop_source())
    dataset = pipeline.dataset(dataset_type="default")
    # sql_query = dataset.customers.select("name", "city").query()
    sql_query = "SELECT AVG(id) as mean_id, name, CAST(LEN(name) as DOUBLE) FROM customers"


    dialect = get_sqlglot_dialect(dataset)
    sqlglot_schema = create_sqlglot_schema(dataset)
    dlt_types = compute_columns_schema(
        sql_query,
        sqlglot_schema,
        dialect,
        SQLGlotTypeMapper(dataset._destination.capabilities())
    )
    print()