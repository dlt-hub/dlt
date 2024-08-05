import ibis
from typing import cast
from typing import Iterator
from dlt import Pipeline
from contextlib import contextmanager
from ibis import BaseBackend
from importlib import import_module

IBIS_DESTINATION_MAP = {"synapse": "mssql", "redshift": "postgres"}


@contextmanager
def ibis_helper(p: Pipeline) -> Iterator[BaseBackend]:
    """This helpers wraps a pipeline to expose an ibis backend to the main"""

    destination_type = p.destination_client().config.destination_type

    # apply destination map
    destination_type = IBIS_DESTINATION_MAP.get(destination_type, destination_type)

    # get the right ibis module
    ibis_module = import_module(f"ibis.backends.{destination_type}")
    ibis_backend = cast(BaseBackend, ibis_module.Backend())

    with p.sql_client() as c:
        yield ibis_backend.from_connection(c)
