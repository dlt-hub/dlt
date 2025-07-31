from functools import wraps
from typing import Callable, Any, Optional, Type, Iterator, List, cast

import dlt
import sqlglot

from dlt.common.configuration.inject import get_fun_last_config, get_fun_spec
from dlt.common.typing import TDataItems, TTableHintTemplate
from dlt.common import logger, json

from dlt.extract import DltResource
from dlt.transformations.typing import TTransformationFunParams
from dlt.transformations.exceptions import (
    TransformationException,
    IncompatibleDatasetsException,
)
from dlt.destinations.dataset.dataset import ReadableDBAPIDataset
from dlt.common.typing import TDataItem
from dlt.common.schema.typing import TTableSchema

from dlt.common.exceptions import MissingDependencyException
from dlt.pipeline.exceptions import PipelineConfigMissing
from dlt.common.schema.typing import (
    TTableSchemaColumns,
    TWriteDisposition,
    TColumnNames,
    TSchemaContract,
    TTableFormat,
    TTableReferenceParam,
)
from dlt.transformations.configuration import TransformationConfiguration
from dlt.common.utils import get_callable_name, simple_repr, without_none
from dlt.extract.exceptions import CurrentSourceNotAvailable
from dlt.extract.pipe_iterator import DataItemWithMeta
from dlt.extract.hints import DLT_HINTS_METADATA_KEY, make_hints
from dlt.destinations.dataset.relation import ReadableDBAPIRelation

try:
    from dlt.helpers.ibis import Expr as IbisExpr
except (ImportError, MissingDependencyException):
    IbisExpr = None

try:
    from dlt.common.libs.pyarrow import pyarrow
except (ImportError, MissingDependencyException):
    pyarrow = None


class DltTransformationResource(DltResource):
    def __init__(self, *args: Any, **kwds: Any) -> None:
        super().__init__(*args, **kwds)

    @property
    def has_dynamic_table_name(self) -> bool:
        return True

    @property
    def has_other_dynamic_hints(self) -> bool:
        return True

    def compute_table_schema(self, item: TDataItem = None, meta: Any = None) -> TTableSchema:
        # if we detect any hints on the item directly, merge them with the existing hints
        schema: TTableSchema = {}
        original_hints = self._hints
        if isinstance(item, ReadableDBAPIRelation):
            schema = item.schema

        # extract resource hints from arrow metadata if available
        if (
            pyarrow
            and isinstance(item, (pyarrow.Table, pyarrow.RecordBatch))
            and item.schema
            and item.schema.metadata
        ):
            _h = item.schema.metadata.get(DLT_HINTS_METADATA_KEY.encode("utf-8"))
            if _h:
                schema = json.loads(_h.decode("utf-8"))

        if schema:
            # TODO: helper function that does this properly
            # convert schema to hints
            hints = make_hints(columns=schema["columns"])

            # NOTE: by merging in the original hints again, we ensure that the item hints are the lowest priority
            self.merge_hints(hints)
            self.merge_hints(original_hints)

        return super().compute_table_schema(item, meta)

    def __repr__(self) -> str:
        kwargs = {
            "name": self.name,
            #  "section": self.section,  should this be explicitly passed?
            "table_name": self._hints.get("table_name"),
            "primary_key": self._hints.get("primary_key"),
            "merge_key": self._hints.get("merge_key"),
            "columns": "{...}" if self._hints.get("columns") else None,
            "parent_table_name": self._hints.get("parent_table_name"),
            "references": "{...}" if self._hints.get("references") else None,
            "nested_hints": "{...}" if self._hints.get("nested_hints") else None,
            "max_table_nesting": self._hints.get("max_table_nesting"),
            "write_disposition": self._hints.get("write_disposition"),
            "table_format": self._hints.get("table_format"),
            "file_format": self._hints.get("file_format"),
            "schema_contract": "{...}" if self._hints.get("schema_contract") else None,
            "incremental": self.incremental,
            "validator": self.validator,
        }
        return simple_repr("@dlt.transformation", **without_none(kwargs))


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

        # resolve config
        config: TransformationConfiguration = (
            get_fun_last_config(func) or get_fun_spec(func)()  # type: ignore[assignment]
        )

        # Determine whether to materialize the model or return it to be executed as sql in the load stage
        should_materialize = False
        if len(datasets) > 0:
            try:
                schema_name = dlt.current.source().name
                current_pipeline = dlt.current.pipeline()
                current_pipeline.destination_client()  # raises if destination not configured
                pipeline_dataset = cast(
                    ReadableDBAPIDataset, current_pipeline.dataset(schema=schema_name)
                )
                should_materialize = not datasets[0].is_same_physical_destination(pipeline_dataset)
            except (PipelineConfigMissing, CurrentSourceNotAvailable):
                logger.info(
                    "Cannot reach destination, defaulting to model extraction for"
                    " transformation %s",
                    resource_name,
                )
                should_materialize = False
        # respect config setting
        should_materialize = should_materialize or config.always_materialize

        def _process_item(item: TDataItems) -> Iterator[TDataItems]:
            # catch the cases where we get a relation from the transformation function
            if isinstance(item, ReadableDBAPIRelation):
                relation = item
            # we see if the string is a valid sql query, if so we need a dataset
            elif isinstance(item, str):
                try:
                    sqlglot.parse_one(item)
                    if len(datasets) == 0:
                        raise IncompatibleDatasetsException(
                            resource_name,
                            "No datasets found in transformation function arguments. Please supply"
                            " all used datasets via transform function arguments.",
                        )
                    else:
                        relation = cast(ReadableDBAPIRelation, datasets[0](item))
                except sqlglot.errors.ParseError as e:
                    raise TransformationException(
                        resource_name,
                        "Invalid SQL query in transformation function. Please supply a valid SQL"
                        " query via transform function arguments.",
                    ) from e
            elif IbisExpr and isinstance(item, IbisExpr):
                relation = cast(ReadableDBAPIRelation, datasets[0](item))
            else:
                # no transformation, just yield this item
                yield item
                return

            if not should_materialize:
                yield relation
            else:
                from dlt.common.libs.pyarrow import add_arrow_metadata

                serialized_hints = json.dumps(relation.schema)
                for chunk in relation.iter_arrow(chunk_size=config.buffer_max_items):
                    yield add_arrow_metadata(chunk, {DLT_HINTS_METADATA_KEY: serialized_hints})

        # support both generator and function
        gen_or_item = func(*args, **kwargs)
        iterable_items = gen_or_item if isinstance(gen_or_item, Iterator) else [gen_or_item]

        for item in iterable_items:
            # unwrap if needed
            meta = None
            if isinstance(item, DataItemWithMeta):
                meta = item.meta
                item = item.data

            for processed_item in _process_item(item):
                yield (DataItemWithMeta(meta, processed_item) if meta else processed_item)

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
