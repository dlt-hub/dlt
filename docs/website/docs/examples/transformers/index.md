---
title: Get Pokemon details in parallel using transformers
description: Learn how to use dlt transformers and how to speed up your loads with parallelism
keywords: [transformers, parallelism, example]
---

import Header from '../_examples-header.md';

<Header 
    intro="In this example you will learn how load a list of pokemone from the pokeapi and with the help of dlt transformers
    automatically query additional data per retrieved pokemon. You will also learn how to harness parallelism with futures."
    slug="transformer" />


## Using transformers with the pokemon api

For this example we will be loading pokemon data from the [Poke Api](https://pokeapi.co/) with the help of transformers to load
pokemon details in parallel

We'll learn how to:
- create 2 transformers and connect them to a resource with the pipe operator `|`
- load these transformers in parallel using the `@dlt.defer` decorator
- configure parallelism in the `config.toml` file
- deselect the main resource so it will not be loaded into the database.

### Loading code

<!--@@@DLT_SNIPPET_START ./code/pokemon-snippets.py::example-->
```py
from typing import Sequence, Iterable
import dlt
from dlt.common.typing import TDataItem
from dlt.extract.source import DltResource
from dlt.sources.helpers import requests

# constants
POKEMON_URL = "https://pokeapi.co/api/v2/pokemon"

# retrieve pokemon list
@dlt.resource(write_disposition="replace", selected=False)
def pokemon_list() -> Iterable[TDataItem]:
    """
    Returns an iterator of pokemon
    Yields:
        dict: The pokemon list data.
    """
    yield from requests.get(POKEMON_URL).json()["results"]

# asynchronously retrieve details for each pokemon in the list
@dlt.transformer()
@dlt.defer
def pokemon(pokemon: TDataItem):
    """
    Returns an iterator of pokemon deatils
    Yields:
        dict: The pokemon full data.
    """
    # just return the results, if you yield,
    # generator will be evaluated in main thread
    return requests.get(pokemon["url"]).json()


# asynchronously retrieve details for the species of each pokemon
@dlt.transformer()
@dlt.defer
def species(pokemon: TDataItem):
    """
    Returns an iterator of species details for each pokemon
    Yields:
        dict: The species full data.
    """
    # just return the results, if you yield,
    # generator will be evaluated in main thread
    species_data = requests.get(pokemon["species"]["url"]).json()
    # optionally add pokemon_id to result json, to later be able
    # to join tables
    species_data["pokemon_id"] = pokemon["id"]
    return species_data

@dlt.source
def source():
    return [pokemon_list | pokemon, pokemon_list | pokemon | species]

if __name__ == "__main__":
    # build duck db pipeline
    pipeline = dlt.pipeline(
        pipeline_name="pokemon", destination="duckdb", dataset_name="pokemon_data"
    )

    # the pokemon_list resource does not need to be loaded
    load_info = pipeline.run(source())
    print(load_info)
```
<!--@@@DLT_SNIPPET_END ./code/pokemon-snippets.py::example-->


### config.toml with examples how to configure parallelism
<!--@@@DLT_SNIPPET_START ./code/.dlt/config.toml::example-->
```toml
[runtime]
log_level="WARNING"

[extract]
# use 2 workers to extract sources in parallel
worker=2
# allow 10 async items to be processed in parallel
max_parallel_items=10

[normalize]
# use 3 worker processes to process 3 files in parallel
workers=3

[load]
# have 50 concurrent load jobs
workers=50
```
<!--@@@DLT_SNIPPET_END ./code/.dlt/config.toml::example-->
