from functools import wraps
import inspect
from typing import Callable, Any, Optional, Type, Iterator, List

import dlt
import sqlglot

from dlt.common.configuration.inject import get_fun_last_config, get_fun_spec
from dlt.common.typing import TDataItems, TTableHintTemplate
from dlt.common import logger

from dlt.destinations.dataset.relation import BaseReadableDBAPIRelation
from dlt.extract.incremental import Incremental
from dlt.extract import DltResource
from dlt.transformations.typing import TTransformationFunParams
from dlt.transformations.exceptions import (
    TransformationException,
    IncompatibleDatasetsException,
)

from dlt.common.exceptions import MissingDependencyException
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
from dlt.extract.pipe_iterator import DataItemWithMeta

try:
    from dlt.helpers.ibis import Expr as IbisExpr
    from dlt.helpers.ibis import compile_ibis_to_sqlglot
except (ImportError, MissingDependencyException):
    IbisExpr = None


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

    if spec and not issubclass(spec, TransformationConfiguration):
        raise TransformationException(
            resource_name,
            "Please derive transformation spec from `TransformationConfiguration`",
        )

    @wraps(func)
    def transformation_function(*args: Any, **kwargs: Any) -> Iterator[TDataItems]:
        # Collect all datasets from args and kwargs
        all_arg_values = list(args) + list(kwargs.values())
        datasets: List[ReadableDBAPIDataset] = [
            arg for arg in all_arg_values if isinstance(arg, ReadableDBAPIDataset)
        ]

        # get first item from gen and see what we're dealing with
        gen = func(*args, **kwargs)
        original_first_item = next(gen)

        # unwrap if needed
        meta = None
        unwrapped_item = original_first_item
        relation = None
        if isinstance(original_first_item, DataItemWithMeta):
            meta = original_first_item.meta
            unwrapped_item = original_first_item.data

        # catch the two cases where we get a relation from the transformation function
        # NOTE: we only process the first item, all other things that are still in the generator are ignored
        if isinstance(unwrapped_item, BaseReadableDBAPIRelation):
            relation = unwrapped_item
        # we see if the string is a valid sql query, if so we need a dataset
        elif isinstance(unwrapped_item, str):
            try:
                sqlglot.parse_one(unwrapped_item)
                if len(datasets) == 0:
                    raise IncompatibleDatasetsException(
                        resource_name,
                        "No datasets found in transformation function arguments. Please supply all"
                        " used datasets via transform function arguments.",
                    )
                else:
                    relation = datasets[0](unwrapped_item)
            except sqlglot.errors.ParseError:
                pass
        # TODO: after merge of sqlglot based readble relation, we want to nativly support ibis expressions in the constructor of the relation
        elif IbisExpr and isinstance(unwrapped_item, IbisExpr):
            sql_query = compile_ibis_to_sqlglot(unwrapped_item, datasets[0].sqlglot_dialect)
            relation = datasets[0](sql_query.sql(datasets[0].sqlglot_dialect))

        # we have something else, so fall back to regular resource behavior
        if not relation:
            yield original_first_item
            yield from gen
            return

        config: TransformationConfiguration = (
            get_fun_last_config(func) or get_fun_spec(func)()  # type: ignore[assignment]
        )

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

        if not should_materialize:
            if meta:
                yield DataItemWithMeta(meta, relation)
            else:
                yield relation
        else:
            for chunk in relation.iter_arrow(chunk_size=config.buffer_max_items):
                yield dlt.mark.with_hints(chunk, hints=relation.compute_hints())

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
    )(transformation_function)
