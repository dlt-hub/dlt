def transformers_snippet() -> None:
    # @@@DLT_SNIPPET_START example
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

    __name__ = "__main__"  # @@@DLT_REMOVE
    if __name__ == "__main__":
        # build duck db pipeline
        pipeline = dlt.pipeline(
            pipeline_name="pokemon", destination="duckdb", dataset_name="pokemon_data"
        )

        # the pokemon_list resource does not need to be loaded
        load_info = pipeline.run(source("https://pokeapi.co/api/v2/pokemon"))
        print(load_info)
    # @@@DLT_SNIPPET_END example

    # test assertions
    row_counts = pipeline.last_trace.last_normalize_info.row_counts
    assert row_counts["pokemon"] == 20
    assert row_counts["species"] == 20
    assert "pokemon_list" not in row_counts
