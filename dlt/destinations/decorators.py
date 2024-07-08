import functools

from typing import Any, Type, Optional, Callable, Union
from typing_extensions import Concatenate
from dlt.common.typing import AnyFun

from functools import wraps

from dlt.common import logger
from dlt.common.destination import TLoaderFileFormat
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema
from dlt.common.destination.capabilities import TLoaderParallelismStrategy

from dlt.destinations.impl.destination.factory import destination as _destination
from dlt.destinations.impl.destination.configuration import (
    TDestinationCallableParams,
    CustomDestinationClientConfiguration,
)


def destination(
    func: Optional[AnyFun] = None,
    /,
    loader_file_format: TLoaderFileFormat = None,
    batch_size: int = 10,
    name: str = None,
    naming_convention: str = "direct",
    skip_dlt_columns_and_tables: bool = True,
    max_table_nesting: int = 0,
    spec: Type[CustomDestinationClientConfiguration] = None,
    max_parallel_load_jobs: Optional[int] = None,
    loader_parallelism_strategy: Optional[TLoaderParallelismStrategy] = None,
) -> Callable[
    [Callable[Concatenate[Union[TDataItems, str], TTableSchema, TDestinationCallableParams], Any]],
    Callable[TDestinationCallableParams, _destination],
]:
    """A decorator that transforms a function that takes two positional arguments "table" and "items" and any number of keyword arguments with defaults
    into a callable that will create a custom destination. The function does not return anything, the keyword arguments can be configuration and secrets values.

    #### Example Usage with Configuration and Secrets:

    >>> @dlt.destination(batch_size=100, loader_file_format="parquet")
    >>> def my_destination(items, table, api_url: str = dlt.config.value, api_secret = dlt.secrets.value):
    >>>     print(table["name"])
    >>>     print(items)
    >>>
    >>> p = dlt.pipeline("chess_pipeline", destination=my_destination)

    Here all incoming data will be sent to the destination function with the items in the requested format and the dlt table schema.
    The config and secret values will be resolved from the path destination.my_destination.api_url and destination.my_destination.api_secret.

    #### Args:
        batch_size: defines how many items per function call are batched together and sent as an array. If you set a batch-size of 0, instead of passing in actual dataitems, you will receive one call per load job with the path of the file as the items argument. You can then open and process that file in any way you like.
        loader_file_format: defines in which format files are stored in the load package before being sent to the destination function, this can be puae-jsonl or parquet.
        name: defines the name of the destination that get's created by the destination decorator, defaults to the name of the function
        naming_convention: defines the name of the destination that gets created by the destination decorator. This controls how table and column names are normalized. The default is direct which will keep all names the same.
        max_nesting_level: defines how deep the normalizer will go to normalize complex fields on your data to create subtables. This overwrites any settings on your source and is set to zero to not create any nested tables by default.
        skip_dlt_columns_and_tables: defines wether internal tables and columns will be fed into the custom destination function. This is set to True by default.
        spec: defines a configuration spec that will be used to to inject arguments into the decorated functions. Argument not in spec will not be injected
        max_parallel_load_jobs: how many load jobs at most will be running during the load
        loader_parallelism_strategy: Can be "sequential" which equals max_parallel_load_jobs=1, "table-sequential" where each table will have at most one loadjob at any given time and "parallel"
    Returns:
        A callable that can be used to create a dlt custom destination instance
    """

    def decorator(
        destination_callable: Callable[
            Concatenate[Union[TDataItems, str], TTableSchema, TDestinationCallableParams], Any
        ]
    ) -> Callable[TDestinationCallableParams, _destination]:
        @wraps(destination_callable)
        def wrapper(
            *args: TDestinationCallableParams.args, **kwargs: TDestinationCallableParams.kwargs
        ) -> _destination:
            if args:
                logger.warning(
                    "Ignoring positional arguments for destination callable %s",
                    destination_callable,
                )
            return _destination(
                spec=spec,
                destination_callable=destination_callable,
                loader_file_format=loader_file_format,
                batch_size=batch_size,
                destination_name=name,
                naming_convention=naming_convention,
                skip_dlt_columns_and_tables=skip_dlt_columns_and_tables,
                max_table_nesting=max_table_nesting,
                max_parallel_load_jobs=max_parallel_load_jobs,
                loader_parallelism_strategy=loader_parallelism_strategy,
                **kwargs,  # type: ignore
            )

        return wrapper

    if func is None:
        # we're called with parens.
        return decorator

    # we're called as @source without parens.
    return decorator(func)  # type: ignore
