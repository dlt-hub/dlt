from typing import Callable, Any, overload, Optional, Type

from dlt.common.utils import get_callable_name
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
    TTransformationFunParams,
)
from dlt.transformations.transformation import (
    make_transformation_resource,
    DltTransformationResource,
)
from dlt.transformations.configuration import TransformationConfiguration


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
    spec: Type[TransformationConfiguration] = None,
    parallelized: bool = False,
    section: Optional[str] = None,
) -> Callable[[Callable[TTransformationFunParams, Any]], DltTransformationResource,]: ...


@overload
def transformation(
    func: Callable[TTransformationFunParams, Any],
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
    spec: Type[TransformationConfiguration] = None,
    parallelized: bool = False,
    section: Optional[str] = None,
) -> DltTransformationResource: ...


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
    spec: Type[TransformationConfiguration] = None,
    parallelized: bool = False,
    section: Optional[str] = None,
) -> Any:
    """
    Decorator to mark a function as a transformation. Returns a DltTransformation object.
    """

    def decorator(
        f: Callable[TTransformationFunParams, Any],
    ) -> DltTransformationResource:
        nonlocal name, write_disposition

        name = name or get_callable_name(f)
        return make_transformation_resource(
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
            section=section,
        )

    if func is None:
        return decorator

    return decorator(func)
