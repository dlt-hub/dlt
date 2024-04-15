import dlt
import os
import shutil
import random

if __name__ == "__main__":

    # shutil.rmtree("./my_files", ignore_errors=True)
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
        
    pipeline_name = f"dave"

    pipe = dlt.pipeline(pipeline_name=pipeline_name, destination="filesystem")
    pipe.run(my_resouce(), table_name="my_table") #, loader_file_format="parquet")

    # resource with incremental for testing restoring of pipeline state 
    @dlt.resource(name="my_table")
    def updated_resouce(id=dlt.sources.incremental("id")):
        yield from [
            {"id": 1},
            {"id": 2},
            {"id": 3},
            {"id": 4},
            {"id": 5},
            {"id": 6}
            ]
    pipe.run(updated_resouce(), table_name="my_table") #, loader_file_format="parquet")
