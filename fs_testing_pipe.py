import dlt
import os

if __name__ == "__main__":
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "file://my_files"
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "TRUE"

    # resource with incremental for testing restoring of pipeline state 
    @dlt.resource(name="my_table")
    def my_resouce(id=dlt.sources.incremental("id")):
        yield from [
            {"id": 1},
            {"id": 2},
            {"id": 3},
            {"id": 4},
            {"id": 5}
            ]

    pipe = dlt.pipeline(pipeline_name="dave", destination="filesystem")
    pipe.run(my_resouce(), table_name="my_table") #, loader_file_format="parquet")
