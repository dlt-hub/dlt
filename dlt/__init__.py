"""data load tool (dlt) — the open-source Python library for data loading

How to create a data loading pipeline with dlt in 3 seconds:

1. Write a pipeline script
>>> import dlt
>>> from dlt.sources.helpers import requests
>>> dlt.run(requests.get("https://pokeapi.co/api/v2/pokemon/").json()["results"], destination="duckdb", table_name="pokemon")

2. Run your pipeline script
  > $ python pokemon.py

3. See and query your data with autogenerated Streamlit app
  > $ dlt pipeline dlt_pokemon show

Or start with our pipeline template with sample PokeAPI (pokeapi.co) data loaded to bigquery

  > $ dlt init pokemon bigquery

For more detailed info, see https://dlthub.com/docs/getting-started
"""

from dlt.version import __version__
from dlt.common.configuration.accessors import config, secrets
from dlt.common.typing import TSecretValue as _TSecretValue, TSecretStrValue as _TSecretStrValue
from dlt.common.configuration.specs import CredentialsConfiguration as _CredentialsConfiguration
from dlt.common.pipeline import source_state as state
from dlt.common.schema import Schema

from dlt import sources
from dlt.extract.decorators import source, resource, transformer, defer
from dlt.destinations.decorators import destination

from dlt.pipeline import (
    pipeline as _pipeline,
    run,
    attach,
    Pipeline,
    dbt,
    current as _current,
    mark as _mark,
)
from dlt.pipeline import progress
from dlt import destinations

from dlt.destinations.transformations import transformation, transformation_group

pipeline = _pipeline
current = _current
mark = _mark

TSecretValue = _TSecretValue
"When typing source/resource function arguments it indicates that a given argument is a secret and should be taken from dlt.secrets."

TSecretStrValue = _TSecretStrValue
"When typing source/resource function arguments it indicates that a given argument is a secret STRING and should be taken from dlt.secrets."

TCredentials = _CredentialsConfiguration
"When typing source/resource function arguments it indicates that a given argument represents credentials and should be taken from dlt.secrets. Credentials may be a string, dictionary or any other type."

__all__ = [
    "__version__",
    "config",
    "secrets",
    "state",
    "Schema",
    "source",
    "resource",
    "transformer",
    "defer",
    "destination",
    "pipeline",
    "run",
    "attach",
    "Pipeline",
    "dbt",
    "progress",
    "current",
    "mark",
    "TSecretValue",
    "TCredentials",
    "sources",
    "destinations",
    "transformation",
    "transformation_group",
]

# verify that no injection context was created
from dlt.common.configuration.container import Container as _Container

assert (
    _Container._INSTANCE is None
), "Injection container should not be initialized during initial import"
# create injection container
_Container()
