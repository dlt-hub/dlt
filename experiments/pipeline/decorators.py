import inspect
from types import ModuleType
from makefun import wraps
from typing import Any, Dict, NamedTuple, Optional, Type

from dlt.common.configuration import with_config, get_fun_spec
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.exceptions import ArgumentsOverloadException
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TTableSchemaColumns, TWriteDisposition
from dlt.common.source import TTableHintTemplate, TFunHintTemplate
from dlt.common.typing import AnyFun, TFun
from dlt.common.utils import is_inner_function
from dlt.extract.sources import DltResource, DltSource


class SourceInfo(NamedTuple):
    SPEC: Type[BaseConfiguration]
    f: AnyFun
    module: ModuleType


_SOURCES: Dict[str, SourceInfo] = {}


def source(func: Optional[AnyFun] = None, /, name: str = None, schema: Schema = None, spec: Type[BaseConfiguration] = None):

    if name and schema:
        raise ArgumentsOverloadException("Source name cannot be set if schema is present")

    def decorator(f: TFun) -> TFun:
        nonlocal schema, name

        # extract name
        if schema:
            name = schema.name
        else:
            name = name or f.__name__
            # create or load default schema
            # TODO: we need a convention to load ie. load the schema from file with name_schema.yaml
            schema = Schema(name)

        # wrap source extraction function in configuration with namespace
        conf_f = with_config(f, spec=spec, namespaces=("source", name))

        @wraps(conf_f, func_name=name)
        def _wrap(*args: Any, **kwargs: Any) -> DltSource:
            rv = conf_f(*args, **kwargs)
            # if generator, consume it immediately
            if inspect.isgenerator(rv):
                rv = list(rv)

            def check_rv_type(rv: Any) -> None:
                pass

            # check if return type is list or tuple
            if isinstance(rv, (list, tuple)):
                # check all returned elements
                for v in rv:
                    check_rv_type(v)
            else:
                check_rv_type(rv)

            # convert to source
            return DltSource.from_data(schema, rv)

        # get spec for wrapped function
        SPEC = get_fun_spec(conf_f)
        # store the source information
        _SOURCES[_wrap.__qualname__] = SourceInfo(SPEC, _wrap, inspect.getmodule(f))

        return _wrap

    if func is None:
        # we're called with parens.
        return decorator

    if not callable(func):
        raise ValueError("First parameter to the source must be callable ie. by using it as function decorator")

    # we're called as @source without parens.
    return decorator(func)


def resource(
    data: Optional[Any] = None,
    /,
    name: TTableHintTemplate[str] = None,
    table_name_fun: TFunHintTemplate[str] = None,
    write_disposition: TTableHintTemplate[TWriteDisposition] = None,
    columns: TTableHintTemplate[TTableSchemaColumns] = None,
    selected: bool = True,
    depends_on: DltResource = None,
    spec: Type[BaseConfiguration] = None):

    def make_resource(name, _data: Any) -> DltResource:
        table_template = DltResource.new_table_template(table_name_fun or name, write_disposition=write_disposition, columns=columns)
        return DltResource.from_data(_data, name, table_template, selected, depends_on)


    def decorator(f: TFun) -> TFun:
        resource_name = name or f.__name__

        # if f is not a generator (does not yield) raise Exception
        if not inspect.isgeneratorfunction(inspect.unwrap(f)):
            raise ResourceFunNotGenerator()

        # do not inject config values for inner functions, we assume that they are part of the source
        SPEC: Type[BaseConfiguration] = None
        if is_inner_function(f):
            conf_f = f
        else:
            print("USE SPEC -> GLOBAL")
            # wrap source extraction function in configuration with namespace
            conf_f = with_config(f, spec=spec, namespaces=("resource", resource_name))
            # get spec for wrapped function
            SPEC = get_fun_spec(conf_f)

        @wraps(conf_f, func_name=resource_name)
        def _wrap(*args: Any, **kwargs: Any) -> DltResource:
            return make_resource(resource_name, f(*args, **kwargs))

        # store the standalone resource information
        if SPEC:
            _SOURCES[_wrap.__qualname__] = SourceInfo(SPEC, _wrap, inspect.getmodule(f))

        return _wrap


    # if data is callable or none use decorator
    if data is None:
        # we're called with parens.
        return decorator

    if callable(data):
        return decorator(data)
    else:
        return make_resource(name, data)


def _get_source_for_inner_function(f: AnyFun) -> Optional[SourceInfo]:
    # find source function
    parts = f.__qualname__.split(".")
    parent_fun = ".".join(parts[:-2])
    return _SOURCES.get(parent_fun)
