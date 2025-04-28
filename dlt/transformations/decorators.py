from typing import Callable, Any, overload, Optional, Type

from dlt.common.utils import get_callable_name
from dlt.common.destination.dataset import SupportsReadableDataset
from dlt.common.typing import AnyFun, TColumnNames
from dlt.extract.incremental import TIncrementalConfig

from dlt.common.schema.typing import (
    TWriteDisposition,
    TTableSchemaColumns,
    TSchemaContract,
    TTableFormat,
    TTableReferenceParam,
)

from dlt.transformations.typing import (
    TTransformationType,
    TTransformationFunParams,
    TLineageMode,
)
from dlt.transformations.transform import make_transform_resource, DltTransformResource
from dlt.transformations.configuration import TransformConfiguration


# NOTE: can we just return a resource directly with some additional hints here?
@overload
def transformation(
    func: None = ...,
    /,
    name: str = None,
    table_name: str = None,
    write_disposition: TWriteDisposition = None,
    columns: TTableSchemaColumns = None,
    primary_key: TColumnNames = None,
    merge_key: TColumnNames = None,
    schema_contract: TSchemaContract = None,
    table_format: TTableFormat = None,
    references: TTableReferenceParam = None,
    selected: bool = True,
    spec: Type[TransformConfiguration] = None,
    parallelized: bool = False,
    incremental: Optional[TIncrementalConfig] = None,
    standalone: bool = False,
    # transformation specific
    transformation_type: TTransformationType = None,
    chunk_size: int = None,
    lineage_mode: TLineageMode = None,
) -> Callable[[Callable[TTransformationFunParams, Any]], DltTransformResource,]: ...


@overload
def transformation(
    func: Callable[TTransformationFunParams, Any] = None,
    /,
    name: str = None,
    table_name: str = None,
    write_disposition: TWriteDisposition = None,
    columns: TTableSchemaColumns = None,
    primary_key: TColumnNames = None,
    merge_key: TColumnNames = None,
    schema_contract: TSchemaContract = None,
    table_format: TTableFormat = None,
    references: TTableReferenceParam = None,
    selected: bool = True,
    spec: Type[TransformConfiguration] = None,
    parallelized: bool = False,
    incremental: Optional[TIncrementalConfig] = None,
    standalone: bool = False,
    # transformation specific
    transformation_type: TTransformationType = None,
    chunk_size: int = None,
    lineage_mode: TLineageMode = None,
) -> Callable[[Callable[TTransformationFunParams, Any]], DltTransformResource,]: ...


def transformation(
    func: Optional[AnyFun] = None,
    /,
    name: str = None,
    table_name: str = None,
    write_disposition: TWriteDisposition = None,
    columns: TTableSchemaColumns = None,
    primary_key: TColumnNames = None,
    merge_key: TColumnNames = None,
    schema_contract: TSchemaContract = None,
    table_format: TTableFormat = None,
    references: TTableReferenceParam = None,
    selected: bool = True,
    spec: Type[TransformConfiguration] = None,
    parallelized: bool = False,
    incremental: Optional[TIncrementalConfig] = None,
    standalone: bool = False,
    # transformation specific
    transformation_type: TTransformationType = None,
    chunk_size: int = None,
    lineage_mode: TLineageMode = None,
) -> Any:
    """
    Decorator to mark a function as a transformation. Returns a DltTransformation object.
    """

    def decorator(
        f: Callable[TTransformationFunParams, Any],
    ) -> DltTransformResource:
        nonlocal name, write_disposition

        name = name or get_callable_name(f)
        return make_transform_resource(
            f,
            name=name,
            table_name=table_name,
            write_disposition=write_disposition,
            columns=columns,
            primary_key=primary_key,
            merge_key=merge_key,
            schema_contract=schema_contract,
            table_format=table_format,
            references=references,
            selected=selected,
            spec=spec,
            parallelized=parallelized,
            incremental=incremental,
            standalone=standalone,
            transformation_type=transformation_type,
            chunk_size=chunk_size,
            lineage_mode=lineage_mode,
        )

    if func is None:
        return decorator

    name = name or get_callable_name(func)
    return decorator(func)
