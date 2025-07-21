import dataclasses
import datetime
from typing import Type, TypeVar

from polyfactory.factories import DataclassFactory

import dlt
from dlt.common.data_types.typing import TDataType
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TTableSchema


T = TypeVar("T")

DLT_TO_PY_TYPE_MAPPING: dict[TDataType, type] = {
    "text": str,
    "double": float,
    "bool": bool,
    "timestamp": datetime.datetime,
    "bigint": int,
    "binary": bytes,
    "json": dict,
    "decimal": float,  # TODO add Decimal support
    "wei": float,  # TODO add Decimal support
    "date": datetime.date,
    "time": datetime.time,
}


# TODO make this generic to allow to swap TypeMapper
# TODO this could generate typeddict instead; depends on the use case
# TODO allow user to manually specify fields and factory
def table_schema_to_dataclass(table_schema: TTableSchema) -> type:
    # we create a special name to avoid name collisions
    cls_name: str = "_" + table_schema["name"].capitalize()
    fields: list[tuple[str, type]] = []
    for column_name, column in table_schema["columns"].items():
        py_type = DLT_TO_PY_TYPE_MAPPING[column["data_type"]]
        # if column.get("nullable") is True:
        #    py_type = Optional[py_type]

        attribute = (column_name, py_type)#, field)
        fields.append(attribute)
    
    return dataclasses.make_dataclass(cls_name=cls_name, fields=fields, kw_only=True, slots=True)


# TODO fix typing parameterization; DataclassFactory[T] is not really a thing
# in reality, we get a subclass of DataclassFactory 
def _create_factory(dataclass: Type[T]) -> DataclassFactory[T]:
    factory = DataclassFactory.create_factory(
        dataclass
    )
    # override the default behavior which excludes fields with `_` prefix (e.g., `_dlt_id`)
    factory.should_set_field_value = lambda *args, **kwargs: True
    return factory


def generate_data(schema: Schema, table_name: str, size: int = 5) -> list[object]:
    dataclass = table_schema_to_dataclass(schema.tables[table_name])
    factory = _create_factory(dataclass)
    instances = factory.batch(size)
    return instances


def populate_dataset(dataset: dlt.Dataset, table_name: str, size: int = 10) -> dlt.Dataset:
    pipeline = dlt.pipeline(
        pipeline_name="data-generation-test",
        destination=dataset._destination,
        dataset_name=dataset._name,
    )

    table_schema = dataset.schema.tables.get(table_name)
    if table_schema is None:
        raise KeyError(
            f"Table `{table_name}` not found in schema."
            f" Available tables: `{list(dataset.schema.tables.keys())}`"
        )

    dataclass = table_schema_to_dataclass(table_schema)
    factory = _create_factory(dataclass)
    instances = factory.batch(size)
    pipeline.run(instances, table_name=table_name, loader_file_format="parquet")

    return dataset
