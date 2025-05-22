from functools import wraps
import inspect
from typing import Callable, Any, Optional, Type, Iterator, List


import dlt

from dlt.common.configuration.inject import _LAST_DLT_CONFIG, get_fun_last_config, get_fun_spec
from dlt.common.reflection.inspect import isgeneratorfunction
from dlt.common.typing import TDataItems
from dlt.common import logger

from dlt.extract.hints import SqlModel
from dlt.extract.incremental import Incremental

from dlt.transformations.typing import (
    TTransformationFunParams,
)
from dlt.transformations.exceptions import (
    TransformationException,
    UnknownColumnTypesException,
    TransformationInvalidReturnTypeException,
    IncompatibleDatasetsException,
)
from dlt.pipeline.exceptions import PipelineConfigMissing
from dlt.destinations.dataset import ReadableDBAPIDataset
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.extract.hints import make_hints
from dlt.common.destination.dataset import SupportsReadableRelation
from dlt.extract import DltResource
from dlt.transformations.configuration import TransformationConfiguration
from dlt.common.utils import get_callable_name
from dlt.common.schema.typing import (
    TWriteDisposition,
    TColumnNames,
    TSchemaContract,
    TTableFormat,
    TTableReferenceParam,
)
from dlt.extract.exceptions import (
    CurrentSourceNotAvailable,
)


class DltTransformationResource(DltResource):
    def __init__(self, *args: Any, **kwds: Any) -> None:
        super().__init__(*args, **kwds)


def make_transformation_resource(
    func: Callable[TTransformationFunParams, Any],
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
) -> DltTransformationResource:
    resource_name = name if name and not callable(name) else get_callable_name(func)

    # check function type, for generators we assume a regular resource
    is_regular_resource = isgeneratorfunction(func)
    # check if spec derives from right base
    if spec:
        if not issubclass(spec, TransformationConfiguration):
            raise TransformationException(
                resource_name, "Please derive transformation spec from TransformationConfiguration"
            )

    @wraps(func)
    def transformation_function(*args: Any, **kwargs: Any) -> Iterator[TDataItems]:
        config: TransformationConfiguration = get_fun_last_config(func) or get_fun_spec(func)()  # type: ignore[assignment]

        all_arg_values = list(args) + list(kwargs.values())

        # collect all datasets and incrementals from args
        datasets: List[ReadableDBAPIDataset] = []
        for arg in all_arg_values:
            if isinstance(arg, ReadableDBAPIDataset):
                datasets.append(arg)

        # find incrementals in func signature
        for arg_name, arg in inspect.signature(func).parameters.items():
            if arg.annotation is Incremental or isinstance(arg.default, Incremental):
                logger.warning(
                    "Incremental arguments are not supported in transformation functions and will"
                    " have no effect. Found incremental argument: %s.",
                    arg_name,
                )

        # NOTE: there may be cases where some other dataset is used to get a starting
        # point and it will be on a different destination.
        if not datasets:
            raise IncompatibleDatasetsException(
                resource_name,
                "No datasets detected in transformation. Please supply all used datasets via"
                " transform function arguments.",
            )

        # Determine whether to materialize the model or return it to be materialized in the load stage
        # we need to supply the current schema name (=source name) to the dataset constructor
        should_materialize = False

        try:
            # TODO: convert to dlt.current.dataset()

            schema_name = dlt.current.source().name
            current_pipeline = dlt.current.pipeline()
            current_pipeline.destination_client()  # this line will raise PipelineConfigMissing if destination not configured

            should_materialize = not datasets[0].is_same_physical_destination(
                dlt.current.pipeline().dataset(schema=schema_name)
            )
        # if we cannot reach the destination, or a running outside of a pipeline, we extract frames
        except (PipelineConfigMissing, CurrentSourceNotAvailable):
            logger.info(
                "Cannot reach destination, defaulting to model extraction for transformation %s",
                resource_name,
            )
            # if destination is not configured when extracting we have a power user scenario
            # assume that pipeline is correctly set up so if the user is returning model assume it can be materialized later
            should_materialize = False

        # extract query from transform function
        select_query: str = None
        transformation_result: Any = func(*args, **kwargs)
        if isinstance(transformation_result, str):
            # use first dataset to convert query into expression
            select_query = transformation_result
            transformation_result = datasets[0](select_query)
        if not isinstance(transformation_result, SupportsReadableRelation):
            raise TransformationInvalidReturnTypeException(
                resource_name,
                "Sql Transformation %s returned an invalid type: %s. Please either return a valid"
                " sql string or Ibis / data frame expression from a dataset. If you want to return"
                " data (data frames / arrow table), please yield those, not return."
                % (name, type(transformation_result)),
            )

        select_query = transformation_result.query(qualified=True)
        # compute lineage
        computed_columns: TTableSchemaColumns = {}
        all_columns: TTableSchemaColumns = columns or {}
        # strict lineage!
        computed_columns = transformation_result.compute_columns_schema(
            infer_sqlglot_schema=False,
            allow_anonymous_columns=False,
            allow_partial=False,
        )
        all_columns = {**computed_columns, **(columns or {})}

        # for sql transformations all column types must be known
        if not should_materialize:
            # search all columns and see if there are some unknown ones
            unknown_column_types = [
                name for name, c in all_columns.items() if c.get("data_type") is None
            ]

            if unknown_column_types:
                raise UnknownColumnTypesException(
                    resource_name,
                    "For sql transformations all data_types of columns must be known. "
                    + "Please run with strict lineage or provide data_type hints "
                    + f"for following columns: {unknown_column_types}",
                )

            yield dlt.mark.with_hints(SqlModel(select_query), hints=make_hints(columns=all_columns))
        else:
            # NOTE: dataset will not execute query over unknown tables or columns
            for chunk in datasets[0](select_query).iter_arrow(chunk_size=config.buffer_max_items):
                yield dlt.mark.with_hints(chunk, hints=make_hints(columns=all_columns))

    return dlt.resource(
        func if is_regular_resource else transformation_function,
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
    )
