---
title: Enriching loaded data with transformers
description: Learn how to use dlt transformers and how to speed up your loads with parallelism
keywords: [transformers, parallelism, example]
---

import Header from '../_examples-header.md';

<Header 
    intro="In this tutorial you will learn how load a list of pokemone from the pokeapi and with the help of dlt transformers
    automatically query additional data per retrieved pokemon. You will also learn how to harness parallelism with futures."
    slug="transformer" 
    title="Enriching loaded data with transformers" />


## Use transformers
<!--@@@DLT_SNIPPET_START ./code/run-snippets.py::example-->
```py
from typing import Sequence, Iterable
import dlt
from dlt.common.typing import TDataItem
from dlt.extract.source import DltResource
from dlt.sources.helpers import requests

# constants
POKEMON_URL = "https://pokeapi.co/api/v2/pokemon"

# retrieve pokemon list
@dlt.resource(write_disposition="replace")
def pokemon_list() -> Iterable[TDataItem]:
    """
    Returns an iterator of pokemon
    Yields:
        dict: The pokemon list data.
    """
    yield from requests.get(POKEMON_URL).json()["results"]

# asynchronously retrieve details for each pokemon in the list
@dlt.transformer(data_from=pokemon_list)
async def pokemon(pokemon: TDataItem):
    """
    Returns an iterator of pokemon deatils
    Yields:
        dict: The pokemon full data.
    """
    # just return the results, if you yield, 
    # generator will be evaluated in main thread
    return requests.get(pokemon["url"]).json()


# asynchronously retrieve details for the species of each pokemon
@dlt.transformer(data_from=pokemon)
async def species(pokemon: TDataItem):
    """
    Returns an iterator of species details for each pokemon
    Yields:
        dict: The species full data.
    """
    # just return the results, if you yield, 
    # generator will be evaluated in main thread
    species_data = requests.get(pokemon["species"]["url"]).json()
    # optionally add pokemon_id to result json
    species_data["pokemon_id"] = pokemon["id"]
    return species_data


# build duck db pipeline
pipeline = dlt.pipeline(
    pipeline_name="pokemon", destination="duckdb", dataset_name="pokemon_data"
)

# the pokemon_list resource does not need to be loaded
load_info = pipeline.run([pokemon(), species()])
print(load_info)
```
<!--@@@DLT_SNIPPET_END ./code/run-snippets.py::example-->
