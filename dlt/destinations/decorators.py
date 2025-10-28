import inspect
from typing import Any, Type, Optional, Callable, Union, overload
from typing_extensions import Concatenate
from dlt.common.destination.reference import DestinationReference, AnyDestination, Destination
from dlt.common.reflection.spec import get_spec_name_from_f
from dlt.common.typing import AnyFun

from functools import wraps

from dlt.common import logger
from dlt.common.destination import TLoaderFileFormat
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema
from dlt.common.destination.capabilities import TLoaderParallelismStrategy

from dlt.common.utils import get_callable_name, get_full_callable_name, is_inner_callable
from dlt.destinations.impl.destination.factory import destination as _destination
from dlt.destinations.impl.destination.configuration import (
    TDestinationCallableParams,
    CustomDestinationClientConfiguration,
)


@overload
def destination(
    func: None = ...,
    /,
    *,
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
    """A decorator that transforms a function into a custom destination with configuration parameters.

    This overload handles parameterized decorator usage where configuration is provided
    in the decorator call, and the function to be decorated is applied later.

    #### Example Usage:

    >>> @dlt.destination(batch_size=100, loader_file_format="parquet")
    >>> def my_destination(items, table, api_url: str = dlt.config.value, api_secret = dlt.secrets.value):
    >>>     print(table["name"])
    >>>     print(items)
    >>>
    >>> p = dlt.pipeline("chess_pipeline", destination=my_destination)

    Args:
        func (None): Must be None for this overload. The actual function will be provided
            when the decorator is applied.

        loader_file_format (TLoaderFileFormat, optional): Defines the format in which files are stored
            in the load package before being sent to the destination function. Defaults to None.

        batch_size (int, optional): Defines how many items per function call are batched together
            and sent as an array. If set to 0, instead of passing actual data items, you will receive
            one call per load job with the path of the file as the "items" argument. Defaults to 10.

        name (str, optional): Defines the name of the destination. If not provided, defaults to
            the name of the decorated function.

        naming_convention (str, optional): Controls how table and column names are normalized.
            The default value, "direct", keeps all names unchanged.

        skip_dlt_columns_and_tables (bool, optional): Defines whether internal dlt tables and columns
            are included in the custom destination function. Defaults to True.

        max_table_nesting (int, optional): Defines how deep the normalizer will go to flatten nested
            fields in your data to create subtables. This overrides any source settings and defaults
            to 0, meaning no nested tables are created.

        spec (Type[CustomDestinationClientConfiguration], optional): Defines a configuration spec used
            to inject arguments into the decorated function. Arguments not included in the spec will
            not be injected. Defaults to None.

        max_parallel_load_jobs (Optional[int], optional): Defines the maximum number of load jobs
            that can run concurrently during the load process. Defaults to None.

        loader_parallelism_strategy (Optional[TLoaderParallelismStrategy], optional): Determines the
            load job parallelism strategy. Can be "sequential" (equivalent to max_parallel_load_jobs=1),
            "table-sequential" (one load job per table at a time), or "parallel". Defaults to None.

    Returns:
        Callable[[Callable[Concatenate[Union[TDataItems, str], TTableSchema, TDestinationCallableParams], Any]], Callable[TDestinationCallableParams, _destination]]:
            A decorator function that accepts the destination function and returns a callable that can
            be used to create a custom dlt destination instance.
    """
    ...


@overload
def destination(
    func: Callable[
        Concatenate[Union[TDataItems, str], TTableSchema, TDestinationCallableParams], Any
    ],
    /,
    *,
    loader_file_format: TLoaderFileFormat = None,
    batch_size: int = 10,
    name: str = None,
    naming_convention: str = "direct",
    skip_dlt_columns_and_tables: bool = True,
    max_table_nesting: int = 0,
    spec: Type[CustomDestinationClientConfiguration] = None,
    max_parallel_load_jobs: Optional[int] = None,
    loader_parallelism_strategy: Optional[TLoaderParallelismStrategy] = None,
) -> Callable[TDestinationCallableParams, _destination]:
    """Creates a destination factory from a function by directly passing it as an argument.

    This overload handles direct function calls where the destination function is passed
    as the first argument, rather than using decorator syntax.

    #### Example Usage:

    >>> def my_destination(items, table, api_url: str = dlt.config.value, api_secret = dlt.secrets.value):
    >>>     print(table["name"])
    >>>     print(items)
    >>>
    >>> dest = dlt.destination(my_destination, batch_size=100, loader_file_format="parquet")
    >>> p = dlt.pipeline("chess_pipeline", destination=dest)

    Args:
        func (Callable[Concatenate[Union[TDataItems, str], TTableSchema, TDestinationCallableParams], Any]):
            A callable that takes two positional arguments "items" and "table",
            followed by any number of keyword arguments with default values. This function
            will process the incoming data and does not need to return anything. The keyword
            arguments can represent configuration or secret values.

        loader_file_format (TLoaderFileFormat): Defines the format in which files are stored in the load package
            before being sent to the destination function.

        batch_size (int): Defines how many items per function call are batched together and sent as an array.
            If set to 0, instead of passing actual data items, you will receive one call per load job
            with the path of the file as the "items" argument. You can then open and process that file as needed.

        name (str): Defines the name of the destination created by the destination decorator.
            Defaults to the name of the function.

        naming_convention (str): Controls how table and column names are normalized.
            The default value, "direct", keeps all names unchanged.

        skip_dlt_columns_and_tables (bool): Defines whether internal dlt tables and columns
            are included in the custom destination function. Defaults to True.

        max_table_nesting (int): Defines how deep the normalizer will go to flatten nested fields
            in your data to create subtables. This overrides any source settings and defaults to 0,
            meaning no nested tables are created.

        spec (Type[CustomDestinationClientConfiguration]): Defines a configuration spec used
            to inject arguments into the decorated function. Arguments not included in the spec will not be injected.

        max_parallel_load_jobs (Optional[int]): Defines the maximum number of load jobs
            that can run concurrently during the load process.

        loader_parallelism_strategy (Optional[TLoaderParallelismStrategy]): Determines the load job parallelism strategy.
            Can be "sequential" (equivalent to max_parallel_load_jobs=1), "table-sequential" (one load job per table at a time),
            or "parallel".

    Returns:
        Callable[TDestinationCallableParams, _destination]: A callable that can be used to create a custom dlt destination instance.
    """
    ...


@overload
def destination(
    destination_name: str,
    /,
    *,
    destination_type: Optional[str] = None,
    credentials: Optional[Any] = None,
    **kwargs: Any,
) -> AnyDestination:
    """Instantiates a destination from the provided destination name and type by retrieving the corresponding
    destination factory and initializing it with the given credentials and keyword arguments.

    Args:
        destination_name (str): The name of the destination instance to initialize.

        destination_type (Optional[str]): The type of the destination to instantiate.

        credentials (Optional[Any]): Credentials used to connect to the destination.
            May be an instance of a credential class supported by the respective destination.

        **kwargs (Any): Additional keyword arguments passed to the destination factory.

    Returns:
        AnyDestination: An initialized destination.
    """
    ...


def destination(
    func_or_name: Union[Optional[AnyFun], str] = None,
    /,
    *,
    loader_file_format: TLoaderFileFormat = None,
    batch_size: int = 10,
    name: str = None,
    naming_convention: str = "direct",
    skip_dlt_columns_and_tables: bool = True,
    max_table_nesting: int = 0,
    spec: Type[CustomDestinationClientConfiguration] = None,
    max_parallel_load_jobs: Optional[int] = None,
    loader_parallelism_strategy: Optional[TLoaderParallelismStrategy] = None,
    **kwargs: Any,
) -> Any:
    """When used as a decorator, transforms a function that takes two positional arguments, "items" and "table",
    along with any number of keyword arguments with default values, into a callable that will create a custom destination.
    The function itself does not return anything, the keyword arguments can represent configuration or secret values.

    When used as a function with the first argument being a string (the destination name),
    instantiates a destination from the provided destination name and type by retrieving the corresponding
    destination factory and initializing it with the given credentials and keyword arguments.
    """

    def decorator(
        destination_callable: Callable[
            Concatenate[Union[TDataItems, str], TTableSchema, TDestinationCallableParams], Any
        ]
    ) -> Callable[TDestinationCallableParams, _destination]:
        # resolve destination name
        destination_name = name or get_callable_name(destination_callable)

        # synthesize new Destination factory
        class _ConcreteDestinationBase(_destination):
            def __init__(self, **kwargs: Any):
                default_args = dict(
                    spec=spec,
                    destination_callable=destination_callable,
                    loader_file_format=loader_file_format,
                    batch_size=batch_size,
                    destination_name=destination_name,
                    naming_convention=naming_convention,
                    skip_dlt_columns_and_tables=skip_dlt_columns_and_tables,
                    max_table_nesting=max_table_nesting,
                    max_parallel_load_jobs=max_parallel_load_jobs,
                    loader_parallelism_strategy=loader_parallelism_strategy,
                )
                super().__init__(**{**default_args, **kwargs})

        cls_name = get_spec_name_from_f(destination_callable, kind="Destination")
        module = inspect.getmodule(destination_callable)
        # synthesize type
        D: Type[_destination] = type(
            cls_name,
            (_ConcreteDestinationBase,),
            {"__module__": module.__name__, "__orig_base__": _destination},
        )
        # add to the module
        setattr(module, cls_name, D)
        # register only standalone destinations, no inner
        if not is_inner_callable(destination_callable):
            DestinationReference.register(D, get_full_callable_name(destination_callable))

        @wraps(destination_callable)
        def wrapper(
            *args: TDestinationCallableParams.args, **kwargs: TDestinationCallableParams.kwargs
        ) -> _destination:
            if args:
                logger.warning(
                    "Ignoring positional arguments for destination callable %s",
                    destination_callable,
                )
            return D(**kwargs)  # type: ignore[arg-type]

        setattr(wrapper, "_factory", D)  # noqa
        return wrapper

    if func_or_name is None:
        # we're called with parens.
        return decorator
    elif not isinstance(func_or_name, str):
        # we're called as @dlt.destination without parens.
        return decorator(func_or_name)

    # Factory mode: create destination instance from given string
    destination_type = kwargs.pop("destination_type", None)
    if destination_type:
        destination = Destination.from_reference(
            ref=destination_type,
            destination_name=func_or_name,
            **kwargs,
        )
    elif kwargs.get("destination_callable"):
        destination = Destination.from_reference(
            ref="destination",
            destination_name=func_or_name,
            **kwargs,
        )
    else:
        destination = Destination.from_reference(
            ref=func_or_name,
            **kwargs,
        )

    return destination
