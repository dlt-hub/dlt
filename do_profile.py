import dlt
import cProfile
import os
import pstats
from pstats import SortKey
import time

os.environ["BUCKET_URL"] = "s3://dlt-ci-test-bucket/"




def execute_pipeline():
    p = dlt.pipeline("test", destination="athena", dataset_name="test", dev_mode=True)
    p.run([{"id": 1, "name": "test"}], table_name="test")
    # with p.destination_client(p.default_schema_name) as client:
    #    client.drop_storage()

now = time.time()
cProfile.run("""
execute_pipeline()
""", filename="restats")
p = pstats.Stats('restats')
p.sort_stats(SortKey.TIME).print_stats(10)
print("time taken:")
print(time.time() - now)