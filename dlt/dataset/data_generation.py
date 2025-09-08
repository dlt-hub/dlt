from polyfactory.factories import DataclassFactory

import dlt
from dlt.common.schema.models import table_schema_to_dataclass



# TODO fix typing parameterization; DataclassFactory[T] is not really a thing
# in reality, we get a subclass of DataclassFactory 
def _create_factory(dataclass: Type[T]) -> DataclassFactory[T]:
    factory = DataclassFactory.create_factory(dataclass)
    # override the default behavior which excludes fields with `_` prefix (e.g., `_dlt_id`)
    factory.should_set_field_value = lambda *args, **kwargs: True
    return factory


# TODO use conditional generation, where parent -> child relationships generate
# consistent join keys (_dlt_id, _dlt_parent_id, _dlt_root_key). To do this,
# we need to generate tables from root to child and draw possible values from existing
# parent values. In other words, it shouldn't produce orphan rows.
# TODO respect normalization behavior; ensure valid `_dlt_list_idx` and coherent normalized
# data generation
def generate_data(schema: dlt.Schema, table_name: str, size: int = 5) -> list[object]:
    dataclass = table_schema_to_dataclass(schema.tables[table_name])
    factory = _create_factory(dataclass)
    instances = factory.batch(size)
    return instances
    
    
# TODO implement "noisy" data generation that generates data with quality issues
# It could be implemented in 2 passes: 1. generate "perfect" data; 2. perturbate perfect data
# into invalid data

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

