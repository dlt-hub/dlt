import inspect
from typing import Callable, Any, Type, Optional, cast, Iterator, List
from contextlib import contextmanager


import dlt

from dlt.common.typing import TDataItems
from dlt.common import logger

from dlt.extract.incremental import TIncrementalConfig

from dlt.transformations.typing import (
    TTransformationFunParams,
)
from dlt.transformations.exceptions import (
    UnknownColumnTypesException,
    TransformationInvalidReturnTypeException,
    IncompatibleDatasetsException,
)
from dlt.pipeline.exceptions import PipelineConfigMissing
from dlt.destinations.dataset.ibis_relation import ReadableIbisRelation
from dlt.destinations.dataset import ReadableDBAPIDataset
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.extract.hints import make_hints
from dlt.common.destination.dataset import SupportsReadableRelation
from dlt.extract import DltResource
from dlt.transformations.configuration import TransformConfiguration
from dlt.common.utils import get_callable_name
from dlt.common.schema.typing import (
    TWriteDisposition,
    TColumnNames,
    TSchemaContract,
    TTableFormat,
    TTableReferenceParam,
)
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import known_sections
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
    spec: Type[TransformConfiguration] = None,
    parallelized: bool = False,
    incremental: Optional[TIncrementalConfig] = None,
) -> DltTransformationResource:
    # resolve defaults etc

    resource_name = name if name and not callable(name) else get_callable_name(func)

    # check function type, for generators we assume a regular resource
    is_regular_resource = inspect.isgeneratorfunction(inspect.unwrap(func))

    # build transformation function
    def transformation_function(*args: Any, **kwargs: Any) -> Iterator[TDataItems]:
        # NOTE: this is most likely not correct
        config = resolve_configuration(
            TransformConfiguration(),
            sections=(known_sections.EXTRACT, resource_name),
            accept_partial=False,
        )

        # collect all datasets from args
        datasets: List[ReadableDBAPIDataset] = []
        for arg in args:
            if isinstance(arg, ReadableDBAPIDataset):
                datasets.append(arg)

        # NOTE: there may be cases where some other dataset is used to get a starting
        # point and it will be on a different destination.
        if datasets:
            for d in datasets:
                if not d.is_same_physical_destination(datasets[0]):
                    raise IncompatibleDatasetsException(
                        resource_name,
                        "All datasets used in transformation must be on the"
                        + " same physical destination.",
                    )
        else:
            raise IncompatibleDatasetsException(
                resource_name,
                "No datasets detected in transformation. Please supply all used datasets via"
                " transform function arguments.",
            )

        # Determine wether we use sql (model) or python (arrow_iterator) transformation
        # we need to supply the curent schema name (=source name)to the dataset constructor

        resolved_transformation_type = "python"
        try:
            schema_name = dlt.current.source().name
            current_pipeline = dlt.current.pipeline()
            current_pipeline.destination_client()  # this line will raise PipelineConfigMissing if destination not configured

            if datasets[0].is_same_physical_destination(
                dlt.current.pipeline().dataset(schema=schema_name)
            ):
                resolved_transformation_type = "model"
        # if we cannot reach the destination, or a running outside of a pipeline, we extract frames
        except (PipelineConfigMissing, CurrentSourceNotAvailable):
            logger.info(
                "Cannot reach destination, defaulting to model extraction for transformation %s",
                resource_name,
            )
            resolved_transformation_type = "model"

        # extract query from transform function
        select_query: str = None
        transformation_result: Any = func(*args, **kwargs)
        computed_columns: TTableSchemaColumns = {}
        if isinstance(transformation_result, str):
            select_query = transformation_result
            resolved_transformation_type = "python"
        elif isinstance(transformation_result, SupportsReadableRelation):
            computed_columns = transformation_result.compute_columns_schema(
                allow_unknown_columns=False,
                allow_anonymous_columns=False,
                allow_fail=False,
            )

            # Context manager to temporarily disable SELECT * emission
            # Forces the compiler to expand all column names explicitly
            @contextmanager
            def no_star() -> Iterator[None]:
                Select = rewrites.Select
                original = Select.is_star_selection
                Select.is_star_selection = lambda self: False
                try:
                    yield
                finally:
                    Select.is_star_selection = original

            if (
                isinstance(transformation_result, ReadableIbisRelation)
                and resolved_transformation_type == "model"
            ):
                import ibis.backends.sql.rewrites as rewrites

                with no_star():
                    select_query = transformation_result[list(computed_columns.keys())].query()
            else:
                select_query = transformation_result.query()
        else:
            raise TransformationInvalidReturnTypeException(
                resource_name,
                (
                    "Sql Transformation %s returned an invalid type: %s. Please either "
                    + "return a valid sql string or a SupportsReadableRelation instance."
                )
                % (name, type(transformation_result)),
            )

        # compute lineage
        all_columns: TTableSchemaColumns = columns or {}
        if isinstance(transformation_result, SupportsReadableRelation):
            # strict lineage!
            all_columns = {**computed_columns, **(columns or {})}

            # for sql transfomrations all column types must be known
            if resolved_transformation_type == "model":
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

        # for sql transformations we yield an sql select query with column hints
        if resolved_transformation_type == "model":
            from dlt.extract.hints import SqlModel

            yield dlt.mark.with_hints(SqlModel(select_query), hints=make_hints(columns=all_columns))
        elif resolved_transformation_type == "python":
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
        incremental=incremental,
        _impl_cls=DltTransformationResource,
    )
