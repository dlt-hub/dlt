from tests.utils import skipifgithubfork

__source_name__ = "mongodb"


@skipifgithubfork
def nested_data_snippet() -> None:
    # @@@DLT_SNIPPET_START example
    # @@@DLT_SNIPPET_START nested_data
    from itertools import islice
    from typing import Any, Dict, Iterator, Optional

    from bson.decimal128 import Decimal128
    from bson.objectid import ObjectId
    from pendulum import _datetime
    from pymongo import MongoClient

    import dlt
    from dlt.common.time import ensure_pendulum_datetime
    from dlt.common.typing import TDataItem
    from dlt.common.utils import map_nested_in_place

    CHUNK_SIZE = 10000

    # You can limit how deep dlt goes when generating child tables.
    # By default, the library will descend and generate child tables
    # for all nested lists, without a limit.
    # In this example, we specify that we only want to generate child tables up to level 2,
    # so there will be only one level of child tables within child tables.
    @dlt.source(max_table_nesting=2)
    def mongodb_collection(
        connection_url: str = dlt.secrets.value,
        database: Optional[str] = dlt.config.value,
        collection: str = dlt.config.value,
        incremental: Optional[dlt.sources.incremental] = None,  # type: ignore[type-arg]
        write_disposition: Optional[str] = dlt.config.value,
    ) -> Any:
        # set up mongo client
        client: Any = MongoClient(connection_url, uuidRepresentation="standard", tz_aware=True)
        mongo_database = client.get_default_database() if not database else client[database]
        collection_obj = mongo_database[collection]

        def collection_documents(
            client: Any,
            collection: Any,
            incremental: Optional[dlt.sources.incremental[Any]] = None,
        ) -> Iterator[TDataItem]:
            LoaderClass = CollectionLoader

            loader = LoaderClass(client, collection, incremental=incremental)
            yield from loader.load_documents()

        return dlt.resource(  # type: ignore
            collection_documents,
            name=collection_obj.name,
            primary_key="_id",
            write_disposition=write_disposition,
        )(client, collection_obj, incremental=incremental)

    # @@@DLT_SNIPPET_END nested_data

    class CollectionLoader:
        def __init__(
            self,
            client: Any,
            collection: Any,
            incremental: Optional[dlt.sources.incremental[Any]] = None,
        ) -> None:
            self.client = client
            self.collection = collection
            self.incremental = incremental
            if incremental:
                self.cursor_field = incremental.cursor_path
                self.last_value = incremental.last_value
            else:
                self.cursor_column = None
                self.last_value = None

        @property
        def _filter_op(self) -> Dict[str, Any]:
            if not self.incremental or not self.last_value:
                return {}
            if self.incremental.last_value_func is max:
                return {self.cursor_field: {"$gte": self.last_value}}
            elif self.incremental.last_value_func is min:
                return {self.cursor_field: {"$lt": self.last_value}}
            return {}

        def load_documents(self) -> Iterator[TDataItem]:
            cursor = self.collection.find(self._filter_op)
            while docs_slice := list(islice(cursor, CHUNK_SIZE)):
                yield map_nested_in_place(convert_mongo_objs, docs_slice)

    def convert_mongo_objs(value: Any) -> Any:
        if isinstance(value, (ObjectId, Decimal128)):
            return str(value)
        if isinstance(value, _datetime.datetime):
            return ensure_pendulum_datetime(value)
        return value

    # @@@DLT_SNIPPET_START nested_data_run

    __name__ = "__main__"  # @@@DLT_REMOVE
    if __name__ == "__main__":
        # When we created the source, we set max_table_nesting to 2.
        # This ensures that the generated tables do not have more than two
        # levels of nesting, even if the original data structure is more deeply nested.
        pipeline = dlt.pipeline(
            pipeline_name="mongodb_pipeline",
            destination="duckdb",
            dataset_name="unpacked_data",
        )
        source_data = mongodb_collection(collection="movies", write_disposition="replace")
        load_info = pipeline.run(source_data)
        print(load_info)
        tables = pipeline.last_trace.last_normalize_info.row_counts  # @@@DLT_REMOVE
        tables.pop("_dlt_pipeline_state")  # @@@DLT_REMOVE
        assert len(tables) == 7, pipeline.last_trace.last_normalize_info  # @@@DLT_REMOVE

        # The second method involves setting the max_table_nesting attribute directly
        # on the source data object.
        # This allows for dynamic control over the maximum nesting
        # level for a specific data source.
        # Here the nesting level is adjusted before running the pipeline.
        pipeline = dlt.pipeline(
            pipeline_name="mongodb_pipeline",
            destination="duckdb",
            dataset_name="not_unpacked_data",
        )
        source_data = mongodb_collection(collection="movies", write_disposition="replace")
        source_data.max_table_nesting = 0
        load_info = pipeline.run(source_data)
        print(load_info)
        tables = pipeline.last_trace.last_normalize_info.row_counts  # @@@DLT_REMOVE
        tables.pop("_dlt_pipeline_state")  # @@@DLT_REMOVE
        assert len(tables) == 1, pipeline.last_trace.last_normalize_info  # @@@DLT_REMOVE

        # The third method involves applying data type hints to specific columns in the data.
        # In this case, we tell dlt that column 'cast' (containing a list of actors)
        # in 'movies' table should have type complex which means
        # that it will be loaded as JSON/struct and not as child table.
        pipeline = dlt.pipeline(
            pipeline_name="mongodb_pipeline",
            destination="duckdb",
            dataset_name="unpacked_data_without_cast",
        )
        source_data = mongodb_collection(collection="movies", write_disposition="replace")
        source_data.movies.apply_hints(columns={"cast": {"data_type": "complex"}})
        load_info = pipeline.run(source_data)
        print(load_info)
        tables = pipeline.last_trace.last_normalize_info.row_counts  # @@@DLT_REMOVE
        tables.pop("_dlt_pipeline_state")  # @@@DLT_REMOVE
        assert len(tables) == 6, pipeline.last_trace.last_normalize_info  # @@@DLT_REMOVE

    # @@@DLT_SNIPPET_END nested_data_run
    # @@@DLT_SNIPPET_END example
