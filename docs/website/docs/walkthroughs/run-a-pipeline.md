---
sidebar_position: 3
---

# Run a pipeline

## 1. Write and execute pipeline script
Follow the steps below to run your pipeline script, see your loaded data and tables, inspect pipeline state, trace and handle the most common problems.

Once you [created a new pipeline](create-a-pipeline) or [added an existing one](add-a-pipeline) you want to use it to load data. You need to write (or [customize](add-a-pipeline#3-customize-or-write-a-pipeline-script)) a pipeline script, like the one below loading the data from chess.com API.
```python
import dlt
from chess import chess

if __name__ == "__main__" :
    pipeline = dlt.pipeline(pipeline_name="chess_pipeline", destination='duckdb', dataset_name="games_data")
    # get data for a few famous players
    data = chess(['magnuscarlsen','vincentkeymer', 'dommarajugukesh', 'rpragchess'], start_month="2022/11", end_month="2022/12")
    load_info = pipeline.run(data)
    print(load_info)
```
The `run` method will [extract](../architecture.md) data from the twitter source, [normalize](../architecture.md) it into tables and then [load](../architecture.md) into `duckdb` destination in form of one or many load packages. The `run` method returns a `load_info` object that, when printed, displays information with pipeline and dataset names, ids of the load packages, optionally with the information on failed jobs.
```
Pipeline chess_pipeline completed in 1.80 seconds
1 load package(s) were loaded to destination duckdb and into dataset games_data
The duckdb destination used duckdb:////home/rudolfix/src/dlt_tests/dlt-cmd-test-3/chess_pipeline.duckdb location to store data
Load package 1679931001.985323 is COMPLETED and contains no failed jobs
```

##

## Inspect a load
You can inspect load info

## Save load info

## Handle problems
What if something goes wrong?

### Missing secret and

## Further readings
- []
- [Deploy this pipeline](./walkthroughs/deploy-a-pipeline), so that your pipeline script is automatically executed on a schedule