
def transformers_snippet() -> None:
    CHUNK_SIZE = 10000
    # @@@DLT_SNIPPET_START example
    # @@@DLT_SNIPPET_START nested_data
    from itertools import islice
    from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple

    import dlt
    from bson.decimal128 import Decimal128
    from bson.objectid import ObjectId
    from dlt.common.time import ensure_pendulum_datetime
    from dlt.common.typing import TDataItem
    from dlt.common.utils import map_nested_in_place
    from pendulum import _datetime
    from pymongo import ASCENDING, DESCENDING, MongoClient

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
        parallel: Optional[bool] = dlt.config.value,
    ) -> Any:
        # set up mongo client
        client = MongoClient(
            connection_url, uuidRepresentation="standard", tz_aware=True
        )
        mongo_database = client.get_default_database() if not database else client[database]
        collection_obj = mongo_database[collection]

        def collection_documents(
            client,
            collection,
            incremental: Optional[dlt.sources.incremental[Any]] = None,
        ) -> Iterator[TDataItem]:
            LoaderClass = CollectionLoader

            loader = LoaderClass(client, collection, incremental=incremental)
            for data in loader.load_documents():
                yield data

        return dlt.resource(  # type: ignore
            collection_documents,
            name=collection_obj.name,
            primary_key="_id",
            write_disposition=write_disposition,
        )(client, collection_obj, incremental=incremental, parallel=parallel)

    # @@@DLT_SNIPPET_END nested_data

    class CollectionLoader:
        def __init__(
            self,
            client,
            collection,
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

    __name__ = "__main__" # @@@DLT_REMOVE
    if __name__ == "__main__":
        # build duck db pipeline
        pipeline = dlt.pipeline(
            pipeline_name="mongodb_pipeline", destination="duckdb", dataset_name="pokemon_data"
        )
        source_data = mongodb_collection()
        source_data.max_table_nesting = 1

        load_info = pipeline.run(source_data)
        print(load_info)
    # @@@DLT_SNIPPET_END nested_data_run
    # @@@DLT_SNIPPET_END example

    # test assertions
    row_counts = pipeline.last_trace.last_normalize_info.row_counts
