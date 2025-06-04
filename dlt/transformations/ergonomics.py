from typing import Callable, Any, Optional, Type, Iterator, List
from functools import wraps

import dlt
from dlt.common.typing import AnyFun, TDataItems, TTableHintTemplate
from dlt.extract.hints import SqlModel
from dlt.transformations.typing import TTransformationFunParams
from dlt.destinations.dataset import ReadableDBAPIDataset
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.extract.hints import make_hints
from dlt.transformations.configuration import TransformationConfiguration
from dlt.common.schema.typing import (
    TWriteDisposition,
    TColumnNames,
    TSchemaContract,
    TTableFormat,
    TTableReferenceParam,
)
from dlt.transformations.transformation import DltTransformationResource
from dlt.transformations import lineage


def transformation(
    func: Optional[AnyFun] = None,
    /,
    name: TTableHintTemplate[str] = None,
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
    section: Optional[TTableHintTemplate[str]] = None,
) -> Any:
    """
    Decorator to mark a function as a transformation. Returns a DltTransformation object.
    """

    def decorator(
        f: Callable[TTransformationFunParams, Any],
    ) -> DltTransformationResource:
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


def make_transformation_resource(
    transformation_func: Callable[TTransformationFunParams, Any],
    name: TTableHintTemplate[str],
    table_name: str,
    write_disposition: TWriteDisposition,
    columns: TTableSchemaColumns,
    primary_key: TColumnNames,
    merge_key: TColumnNames,
    schema_contract: TSchemaContract,
    table_format: TTableFormat,
    references: TTableReferenceParam,
    selected: bool,
    spec: Type[TransformationConfiguration],
    parallelized: bool,
    section: Optional[TTableHintTemplate[str]],
) -> DltTransformationResource:
    @wraps(transformation_func)
    def transformation_function(*args: Any, **kwargs: Any) -> Iterator[TDataItems]:
        all_arg_values = list(args) + list(kwargs.values())

        # collect all datasets and incrementals from args
        datasets: List[ReadableDBAPIDataset] = []
        for arg in all_arg_values:
            if isinstance(arg, ReadableDBAPIDataset):
                datasets.append(arg)

        # TODO determinate eager or lazy based on returned type
        # lazy: SQL string, sqlglot expression, ibis expression, polars lazyframe
        # eager: dictionary, pandas dataframe, polars dataframe, pyarrow table, list of dictionaries,
        transformation_definition: Any = transformation_func(*args, **kwargs)

        # TODO use functools.singledispatch to dispatch based on type
        # the abstract data backend approach allows to dispatch based on external libraries not installed
        # see ref: https://github.com/machow/databackend/tree/main
        if not isinstance(transformation_definition, str):
            raise NotImplementedError

        # if lazy:
        # NOTE since schema inference requires the full pipeline dataset, it shouldn't happen before `extract` phase
        yield dlt.mark.with_hints(
            SqlModel(
                transformation_definition,
                dialect=datasets[0].sql_client.capabilities.sqlglot_dialect,
            ),
            hints=make_hints(columns=columns),
        )
        # else:
        # yield an iterator

    return dlt.resource(  # type: ignore[return-value]
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
        # incremental=None,
        section=section,
        _impl_cls=DltTransformationResource,
        _base_spec=TransformationConfiguration,
    )(transformation_function)
