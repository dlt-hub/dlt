"""
This source provides data extraction from an example source as a starting point for new pipelines.
Available resources: [berries, pokemon]
"""

from typing import Sequence, Iterable
import dlt
from dlt.common.typing import TDataItem
from dlt.extract.source import DltResource
from dlt.sources.helpers import requests


@dlt.resource(write_disposition="replace")
def berries(berry_url: str) -> Iterable[TDataItem]:
    """
    Returns a list of berries.
    Yields:
        dict: The berries data.
    """
    yield requests.get(berry_url).json()["results"]


@dlt.resource(write_disposition="replace")
def pokemon(pokemon_url: str) -> Iterable[TDataItem]:
    """
    Returns a list of pokemon.
    Yields:
        dict: The pokemon data.
    """
    yield requests.get(pokemon_url).json()["results"]


@dlt.source
def source(pokemon_url: str = dlt.config.value, berry_url: str = dlt.config.value) -> Sequence[DltResource]:
    """
    The source function that returns all availble resources.
    Returns:
        Sequence[DltResource]: A sequence of DltResource objects containing the fetched data.
    """
    return [berries(berry_url), pokemon(pokemon_url)]

pipeline = dlt.pipeline(
    pipeline_name="pokemon", destination="duckdb", dataset_name="pokemon_data"
)
load_info = pipeline.run(source())
print(load_info)