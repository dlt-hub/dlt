---
title: Pokemon details in parallel using transformers
description: Learn how to use dlt transformers and how to speed up your loads with parallelism
keywords: [transformers, parallelism, example]
---

import Header from '../_examples-header.md';

<Header
    intro="In this example, you will learn how to load a list of Pokemon from the PokeAPI and with the help of dlt transformers
    automatically query additional data per retrieved Pokemon. You will also learn how to harness parallelism with a thread pool."
    slug="transformer"
    run_file="pokemon" />


## Using transformers with the Pokemon API

For this example, we will be loading Pokemon data from the [PokeAPI](https://pokeapi.co/) with the help of transformers to load
Pokemon details in parallel.

We'll learn how to:
- create 2 [transformers](../../general-usage/resource.md#feeding-data-from-one-resource-into-another) and connect them to a resource with the pipe operator `|`;
- [load these transformers in parallel](../../reference/performance.md#parallelism) using the `@dlt.defer` decorator;
- [configure parallelism](../../reference/performance.md#parallel-pipeline-config-example) in the `config.toml` file;
- deselect the main resource, so it will not be loaded into the database;
- importing and using a pre-configured `requests` library with automatic retries (`from dlt.sources.helpers import requests`).

### Loading code

<!--@@@DLT_SNIPPET_START ./code/pokemon-snippets.py::example-->
```py
import dlt
from dlt.sources.helpers import requests

@dlt.source(max_table_nesting=2)
def source(pokemon_api_url: str):
    """"""

    # note that we deselect `pokemon_list` - we do not want it to be loaded
    @dlt.resource(write_disposition="replace", selected=False)
    def pokemon_list():
        """Retrieve a first page of Pokemons and yield it. We do not retrieve all the pages in this example"""
        yield requests.get(pokemon_api_url).json()["results"]

    # transformer that retrieves a list of objects in parallel
    @dlt.transformer
    def pokemon(pokemons):
        """Yields details for a list of `pokemons`"""

        # @dlt.defer marks a function to be executed in parallel
        # in a thread pool
        @dlt.defer
        def _get_pokemon(_pokemon):
            return requests.get(_pokemon["url"]).json()

        # call and yield the function result normally, the @dlt.defer takes care of parallelism
        for _pokemon in pokemons:
            yield _get_pokemon(_pokemon)

    # a special case where just one item is retrieved in transformer
    # a whole transformer may be marked for parallel execution
    @dlt.transformer
    @dlt.defer
    def species(pokemon_details):
        """Yields species details for a pokemon"""
        species_data = requests.get(pokemon_details["species"]["url"]).json()
        # link back to pokemon so we have a relation in loaded data
        species_data["pokemon_id"] = pokemon_details["id"]
        # just return the results, if you yield,
        # generator will be evaluated in main thread
        return species_data

    # create two simple pipelines with | operator
    # 1. send list of pokemons into `pokemon` transformer to get pokemon details
    # 2. send pokemon details into `species` transformer to get species details
    # NOTE: dlt is smart enough to get data from pokemon_list and pokemon details once

    return (pokemon_list | pokemon, pokemon_list | pokemon | species)

if __name__ == "__main__":
    # build duck db pipeline
    pipeline = dlt.pipeline(
        pipeline_name="pokemon", destination="duckdb", dataset_name="pokemon_data"
    )

    # the pokemon_list resource does not need to be loaded
    load_info = pipeline.run(source("https://pokeapi.co/api/v2/pokemon"))
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
