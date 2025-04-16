import inspect
from typing import Callable, Any, Type, Optional, cast, Iterator, List, Dict

from dlt.common.typing import TDataItems
from dlt import Schema

import dlt
from dlt.extract.incremental import TIncrementalConfig

from dlt.transformations.reference import (
    TTransformationType,
    TTransformationFunParams,
    TLineageMode,
)
from dlt.transformations.exceptions import (
    TransformationTypeMismatch,
    LineageFailedException,
    UnknownColumnTypesException,
    IncompatibleDatasetsException,
)
from dlt.destinations.dataset import ReadableDBAPIDataset
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.extract.hints import make_hints
from dlt.common.destination.dataset import SupportsReadableDataset, SupportsReadableRelation
from dlt.extract import DltResource
from dlt.common.typing import Concatenate
from dlt.transformations.configuration import TransformConfiguration

from dlt.common.schema.typing import (
    TWriteDisposition,
    TColumnNames,
    TSchemaContract,
    TTableFormat,
    TTableReferenceParam,
)


DEFAULT_CHUNK_SIZE = 50000


class DltTransformResource(DltResource):
    def __init__(self, *args: Any, **kwds: Any) -> None:
        super().__init__(*args, **kwds)
        self.transformation_type: TTransformationType = None


def make_transform_resource(
    func: Callable[Concatenate[SupportsReadableDataset[Any], TTransformationFunParams], Any],
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
) -> DltTransformResource:
    # resolve defaults etc
    chunk_size = chunk_size or DEFAULT_CHUNK_SIZE
    lineage_mode = lineage_mode or "best_effort"
    # resolve type of transformation
    # TODO: react to capabilities of the destination, maybe inside the transform function
    transformation_function_type = "python" if inspect.isgeneratorfunction(func) else "sql"
    resolved_transformation_type = transformation_type
    if not resolved_transformation_type:
        resolved_transformation_type = "sql" if transformation_function_type == "sql" else "python"

    # some sanity checks
    if resolved_transformation_type == "sql" and transformation_function_type == "python":
        raise TransformationTypeMismatch(
            "Sql transformation must return single sql query string or dlt "
            + "readablerelation and not be a generator function"
        )

    if lineage_mode == "strict" and transformation_function_type == "python":
        raise LineageFailedException(
            "Lineage mode 'strict' is only supported for sql transformations"
        )

    # build transform function
    def transform_function(*args: Any, **kwargs: Any) -> Iterator[TDataItems]:
        datasets: List[ReadableDBAPIDataset[Any]] = []

        for arg in args:
            if isinstance(arg, ReadableDBAPIDataset):
                datasets.append(arg)

        # NOTE: there may be cases where some other dataset is used to get a starting
        # point and it will be on a different destination.
        # if datasets:
        #     for d in datasets:
        #         if not d.is_same_phyiscal_destination(datasets[0]):
        #             raise IncompatibleDatasetsException(
        #                 "All datasets used in transformation must be on the"
        #                 + " same physical destination."
        #             )

        # Check that if sql transformation is used, destination dataset is on
        # same physical destination
        # if resolved_transformation_type == "sql" and not datasets[0].is_same_phyiscal_destination(
        #     dlt.current.pipeline().dataset()
        # ):
        #     raise IncompatibleDatasetsException(
        #         "When using sql transformations, the destination dataset"
        #         + " must be on the same physical destination."
        #     )

        # schemas: Dict[str, Schema] = {dataset.dataset_name: dataset.schema for dataset in datasets}

        # extract query from transform function
        select_query: str = None
        transformation_result: Any = func(*args, **kwargs)
        if isinstance(transformation_result, str):
            select_query = transformation_result
        elif hasattr(transformation_result, "query"):
            if isinstance(transformation_result, SupportsReadableRelation):
                select_query = transformation_result.query()
        else:
            raise ValueError(
                (
                    "Sql Transformation %s returned an invalid type: %s. Please either "
                    + "return a valid sql string or a dlt readable relation."
                )
                % (name, type(transformation_result))
            )

        # compute lineage
        computed_columns: TTableSchemaColumns = {}
        all_columns: TTableSchemaColumns = columns or {}
        if lineage_mode in ["strict", "best_effort"]:
            if not isinstance(transformation_result, SupportsReadableRelation):
                raise ValueError(
                    "Lineage only supported for classes that implement SupportsReadableRelation"
                )
            computed_columns = transformation_result.compute_columns_schema()

            # compute_resulting_dlt_table_schema(
            #     select_query,
            #     schemas,
            #     # dialect=dataset.dialect, # TODO: provide dialect
            #     allow_unknown_origins=lineage_mode == "best_effort",
            # )

            all_columns = {**computed_columns, **(columns or {})}

            # for sql transfomrations all column types must be known
            if resolved_transformation_type == "sql":
                # from dlt.transformations.lineage_helpers import (
                #     compute_resulting_column_names_from_select,
                # )

                # column_names = compute_resulting_column_names_from_select(
                #     select_query,
                #     schemas,
                #     # dialect=dataset.dialect, # TODO: provide dialect?
                # )

                # search all columns and see if there are some unknown ones
                unknown_column_types = [
                    name for name, c in all_columns.items() if c.get("data_type") is None
                ]

                if unknown_column_types:
                    raise UnknownColumnTypesException(
                        "For sql transformations all column types must be known. "
                        + "Please run with strict lineage or provide data type hints "
                        + f"for following columns: {unknown_column_types}"
                    )

        # for sql transformations we yield an sql select query with column hints
        if resolved_transformation_type == "sql":
            from dlt.extract.hints import SqlModel

            yield dlt.mark.with_hints(SqlModel(select_query), hints=make_hints(columns=all_columns))
        elif resolved_transformation_type == "python":
            for chunk in datasets[0](select_query).iter_arrow(chunk_size=chunk_size):
                yield dlt.mark.with_hints(chunk, hints=make_hints(columns=all_columns))

    resource = cast(
        DltTransformResource,
        dlt.resource(
            transform_function if transformation_function_type == "sql" else func,  # type: ignore
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
            _impl_cls=DltTransformResource,
        ),
    )
    # set some extra attributes
    resource.transformation_type = resolved_transformation_type
    return resource
