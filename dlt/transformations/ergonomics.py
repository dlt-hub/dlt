from typing import Callable, Any, Optional, Type, Iterator, List
from functools import wraps

# TODO I have a solution for isinstance checks without importing
# external dependencies see ref: https://github.com/machow/databackend/tree/main
import sqlglot
from ibis import ir
import ibis.backends.sql.compilers as sc
import narwhals as nw
import pandas as pd

import dlt
import dlt.helpers.ibis
from dlt.common.destination import TDestinationReferenceArg, Destination
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

        # TODO position-based inference of "what's the destination" is a weak assumption
        # for now, we can assume there's only 1 destination
        # (i.e., cross-dataset is allowed on the same destination)
        # collect all datasets and incrementals from args
        datasets: List[ReadableDBAPIDataset] = []
        for arg in all_arg_values:
            if isinstance(arg, ReadableDBAPIDataset):
                datasets.append(arg)

        dialect = None if not datasets else datasets[0].sql_client.capabilities.sqlglot_dialect

        # TODO determinate eager or lazy based on returned type
        # lazy: SQL string, sqlglot expression, ibis expression, polars lazyframe
        # eager: dictionary, pandas dataframe, polars dataframe, pyarrow table, list of dictionaries,
        transformation_definition: Any = transformation_func(*args, **kwargs)

        # TODO guardrails for one "type" of definition per generator
        # i.e., it shouldn't `yield from (pd.DataFrame, "SELECT *", ibis.Table(...))`
        for definition in transformation_definition:
            # TODO use functools.singledispatch to dispatch based on type
            # the abstract data backend approach allows to dispatch based on external libraries not installed
            # see ref: https://github.com/machow/databackend/tree/main
            if isinstance(definition, str):
                lazy_transform = sqlglot.maybe_parse(definition)
            elif isinstance(definition, ir.Table):
                destination_compiler = get_ibis_to_sqlglot_compiler(datasets[0]._destination)
                lazy_transform = destination_compiler.to_sqlglot(definition)
            elif isinstance(definition, nw.LazyFrame):
                destination_compiler = get_ibis_to_sqlglot_compiler(datasets[0]._destination)
                lazy_transform = destination_compiler.to_sqlglot(nw.to_native(definition))
            elif isinstance(definition, pd.DataFrame):
                eager_transform = definition
            elif isinstance(definition, nw.DataFrame):
                eager_transform = nw.to_native(definition)
            else:
                raise NotImplementedError

            # if lazy:
            # NOTE since schema inference requires the full pipeline dataset, it shouldn't happen before `extract` phase
            yield dlt.mark.with_hints(
                SqlModel(
                    lazy_transform,
                    dialect=dialect,
                ),
                hints=make_hints(columns=columns),
            )

    # TODO question which arguments make sense for `@dlt.transformation`
    # This currently wraps the function twice; we could do that directly
    # @transformation
    #  @resource
    #    transformation_func 
    #
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


# TODO move this code to the right place
def _get_sqlalchemy_compiler(destination: Destination):
    """
    reference: https://docs.sqlalchemy.org/en/20/dialects/
    """
    # Dialects included in SQLAlchemy
    # SQLite via SQLAlchemy is fully-tested with dlt
    if ... == "sqlite":
        compiler = sc.SQLiteCompiler()
    # MySQL via SQLAlchemy is fully-tested with dlt
    elif ... == "mysql":
        compiler = sc.MySQLCompiler()

    elif ... == "oracle":
        compiler = sc.OracleCompiler()

    elif ... == "mariadb":
        raise NotImplementedError

    elif ... == "postgres":
        compiler = sc.PostgresCompiler()

    elif ... == "mssql":
        compiler = sc.MSSQLCompiler()

    # SQLAlchemy extension dialects
    elif ... == "druid":
        compiler = sc.DruidCompiler()

    elif ... == "hive":
        compiler = sc.TrinoCompiler()

    elif ... == "clickhouse":
        compiler = sc.ClickHouseCompiler()

    elif ... == "databricks":
        compiler = sc.DatabricksCompiler()

    elif ... == "bigquery":
        compiler = sc.BigQueryCompiler()

    elif ... == "impala":
        compiler = sc.ImpalaCompiler()

    elif ... == "snowflake":
        compiler = sc.SnowflakeCompiler()

    else:
        raise NotImplementedError

    return compiler



def get_ibis_to_sqlglot_compiler(destination: TDestinationReferenceArg): 
    # ensure destination is a Destination instance
    if not isinstance(destination, Destination):
        destination = Destination.from_reference(destination)
    assert isinstance(destination, Destination)

    if destination.destination_name == "duckdb":
        compiler = sc.DuckDBCompiler()

    elif destination.destination_name == "filesystem":
        compiler = sc.DuckDBCompiler()

    elif destination.destination_name == "motherduck":
        compiler = sc.DuckDBCompiler()

    elif destination.destination_name == "postgres":
        compiler = sc.PostgresCompiler()

    elif destination.destination_name == "clickhouse":
        compiler = sc.ClickHouseCompiler()

    elif destination.destination_name == "snowflake":
        compiler = sc.SnowflakeCompiler()

    elif destination.destination_name == "databricks":
        compiler = sc.DatabricksCompiler()

    elif destination.destination_name == "mssql":
        compiler = sc.MSSQLCompiler()

    # NOTE synapse might differ from MSSQL
    elif destination.destination_name == "synapse":
        compiler = sc.MSSQLCompiler()

    elif destination.destination_name == "bigquery":
        compiler = sc.BigQueryCompiler()

    elif destination.destination_name == "athena":
        compiler = sc.AthenaCompiler()

    # NOTE Dremio uses a presto/trino-based language
    elif destination.destination_name == "dremio":
        compiler = sc.TrinoCompiler()

    # TODO parse the SQLAlchemy dialect
    elif destination.destination_name == "sqlalchemy":
        compiler = _get_sqlalchemy_compiler(destination)

    else:
        raise NotImplementedError(
            f"Destination of type {Destination.from_reference(destination).destination_type} not"
            " supported by ibis."
        )

    return compiler