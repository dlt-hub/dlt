from typing import Union

from polyfactory.factories import DataclassFactory

import dlt
from dlt.common.schema.models import table_schema_to_dataclass
from dlt.common.exceptions import TypeErrorWithKnownTypes


# TODO fix typing parameterization; DataclassFactory[T] is not really a thing
# in reality, we get a subclass of DataclassFactory 
def _create_factory(dataclass) -> DataclassFactory:
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
def generate_data(obj: Union[dlt.Schema, dlt.Dataset, dlt.Pipeline], table_name: str, *, n_records: int = 5) -> list[object]:
    if isinstance(obj, dlt.Schema):
        schema = obj
    elif isinstance(obj, dlt.Dataset):
        schema = obj.schema
    elif isinstance(obj, dlt.Pipeline):
        schema = obj.default_schema
    else:
        raise TypeErrorWithKnownTypes("obj", obj, ["dlt.Schema", "dlt.Dataset", "dlt.Pipeline"])

    dataclass = table_schema_to_dataclass(schema.tables[table_name])
    factory = _create_factory(dataclass)
    instances = factory.batch(n_records)
    return instances
    
    
# TODO implement "noisy" data generation that generates data with quality issues
# It could be implemented in 2 passes: 1. generate "perfect" data; 2. perturbate perfect data
# into invalid data
