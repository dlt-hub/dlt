from functools import wraps
import inspect
from typing import Callable, Any, Optional, Type, Iterator, List

import dlt

from dlt.common.configuration.inject import get_fun_last_config, get_fun_spec
from dlt.common.reflection.inspect import isgeneratorfunction
from dlt.common.typing import TDataItems, TTableHintTemplate
from dlt.common import logger

from dlt.destinations.dataset.relation import BaseReadableDBAPIRelation
from dlt.extract.hints import SqlModel, make_hints, WithComputableHints, TResourceHints
from dlt.extract.incremental import Incremental
from dlt.extract import DltResource
from dlt.transformations.typing import TTransformationFunParams
from dlt.transformations.exceptions import (
    TransformationException,
    TransformationInvalidReturnTypeException,
    IncompatibleDatasetsException,
)
from dlt.pipeline.exceptions import PipelineConfigMissing
from dlt.destinations.dataset import ReadableDBAPIDataset
from dlt.common.schema.typing import (
    TTableSchemaColumns,
    TWriteDisposition,
    TColumnNames,
    TSchemaContract,
    TTableFormat,
    TTableReferenceParam,
)
from dlt.transformations.configuration import TransformationConfiguration
from dlt.common.utils import get_callable_name
from dlt.extract.exceptions import CurrentSourceNotAvailable
from dlt.common.schema.typing import TPartialTableSchema
from dlt.extract.pipe_iterator import DataItemWithMeta


class MaterializableSqlModel(SqlModel, WithComputableHints):
    # NOTE: we could forward all data access methods to this class
    __slots__ = ("relation",)

    def __init__(
        self,
        relation: Optional[BaseReadableDBAPIRelation] = None,
    ) -> None:
        super().__init__(relation.query(), relation.query_dialect())
        self.relation = relation

    @classmethod
    def from_relation(cls, relation: BaseReadableDBAPIRelation) -> "MaterializableSqlModel":
        return cls(relation=relation)

    def compute_hints(self) -> TResourceHints:
        computed_columns, _ = self.relation._compute_columns_schema(
            infer_sqlglot_schema=True,
            allow_anonymous_columns=True,
            allow_partial=True,
        )
        return make_hints(columns=computed_columns)


class DltTransformationResource(DltResource):
    def __init__(self, *args: Any, **kwds: Any) -> None:
        super().__init__(*args, **kwds)


def make_transformation_resource(
    func: Callable[TTransformationFunParams, Any],
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
    resource_name = name if name and not callable(name) else get_callable_name(func)
    is_regular_resource = isgeneratorfunction(func)

    if spec and not issubclass(spec, TransformationConfiguration):
        raise TransformationException(
            resource_name,
            "Please derive transformation spec from `TransformationConfiguration`",
        )

    @wraps(func)
    def transformation_function(*args: Any, **kwargs: Any) -> Iterator[TDataItems]:
        config: TransformationConfiguration = (
            get_fun_last_config(func) or get_fun_spec(func)()  # type: ignore[assignment]
        )

        # Collect all datasets from args and kwargs
        all_arg_values = list(args) + list(kwargs.values())
        datasets: List[ReadableDBAPIDataset] = [
            arg for arg in all_arg_values if isinstance(arg, ReadableDBAPIDataset)
        ]

        # Warn if Incremental arguments are present
        for arg_name, param in inspect.signature(func).parameters.items():
            if param.annotation is Incremental or isinstance(param.default, Incremental):
                logger.warning(
                    "Incremental arguments are not supported in transformation functions and will"
                    " have no effect. Found incremental argument: %s.",
                    arg_name,
                )

        if not datasets:
            raise IncompatibleDatasetsException(
                resource_name,
                "No datasets detected in transformation. Please supply all used datasets via"
                " transform function arguments.",
            )

        # Determine whether to materialize the model or return it to be materialized in the load stage
        should_materialize = False
        try:
            schema_name = dlt.current.source().name
            current_pipeline = dlt.current.pipeline()
            current_pipeline.destination_client()  # raises if destination not configured

            should_materialize = not datasets[0].is_same_physical_destination(
                current_pipeline.dataset(schema=schema_name)
            )
        except (PipelineConfigMissing, CurrentSourceNotAvailable):
            logger.info(
                "Cannot reach destination, defaulting to model extraction for transformation %s",
                resource_name,
            )
            should_materialize = False

        # respect always materialize config
        should_materialize = should_materialize or config.always_materialize

        # Call the transformation function
        transformation_result: Any = func(*args, **kwargs)

        # unwrap meta
        meta = None
        if isinstance(transformation_result, DataItemWithMeta):
            meta = transformation_result.meta
            transformation_result = transformation_result.data

        # If a string is returned, construct relation from first dataset from it
        if isinstance(transformation_result, BaseReadableDBAPIRelation):
            relation = transformation_result
        elif isinstance(transformation_result, str):
            relation = datasets[0](transformation_result)
        else:
            raise TransformationInvalidReturnTypeException(
                resource_name,
                "Sql Transformation %s returned an invalid type: %s. Please either return a valid"
                " sql string or Ibis / data frame expression from a dataset. If you want to return"
                " data (data frames / arrow table), please yield those, not return."
                % (name, type(transformation_result)),
            )

        sql_model = MaterializableSqlModel.from_relation(relation)

        if not should_materialize:
            if meta:
                yield DataItemWithMeta(meta, sql_model)
            else:
                yield sql_model
        else:
            for chunk in relation.iter_arrow(chunk_size=config.buffer_max_items):
                yield dlt.mark.with_hints(chunk, hints=sql_model.compute_hints())

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
        section=section,
        _impl_cls=DltTransformationResource,
        _base_spec=TransformationConfiguration,
    )(
        func if is_regular_resource else transformation_function  # type: ignore[arg-type]
    )
