---
title: Transformers and Parallelism
description: Learn how to use dlt transformers and how to speed up your loads with parallelism
keywords: [transformers, parallelism, example]
---

import Header from '../_examples-header.md';

<Header 
    intro="In this tutorial you will learn how load a list of pokemone from the pokeapi and with the help of dlt transformers
    automatically query additional data per retrieved pokemon. You will also learn how to harness parallelism with futures."
    slug="transformer-and-parallelism" 
    title="Transformers and Parallelism" />


## Use transformers

Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.

### Toml File
Labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.
<!--@@@DLT_SNIPPET_START ./code/.dlt/config.toml::toml-->
```toml
pokemon_url="https://pokeapi.co/api/v2/pokemon"
berry_url="https://pokeapi.co/api/v2/berry"
```
<!--@@@DLT_SNIPPET_END ./code/.dlt/config.toml::toml-->


### Python files
Do this to use transformers
<!--@@@DLT_SNIPPET_START ./code/run-snippets.py::snippet1-->
```py
@dlt.resource(write_disposition="replace")
def berries(berry_url: str) -> Iterable[TDataItem]:
    """
    Returns a list of berries.
    Yields:
        dict: The berries data.
    """
    yield requests.get(berry_url).json()["results"]
```
<!--@@@DLT_SNIPPET_END ./code/run-snippets.py::snippet1-->

Do this to enable async loading
<!--@@@DLT_SNIPPET_START ./code/run-snippets.py::snippet2-->
```py
@dlt.resource(write_disposition="replace")
def pokemon(pokemon_url: str) -> Iterable[TDataItem]:
    """
    Returns a list of pokemon.
    Yields:
        dict: The pokemon data.
    """
    yield requests.get(pokemon_url).json()["results"]
```
<!--@@@DLT_SNIPPET_END ./code/run-snippets.py::snippet2-->