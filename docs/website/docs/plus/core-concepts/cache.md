# Local Transformation Cache

In dlt+ you have the option of executing your data transformations inside a temporary storage created by dlt+ ("dlt+ Cache") in your local system. 

## How it works

You specify which datasets you want to pass to the cache in your dlt manifest file (`dlt.yml`). The cache automatically discovers the source schema from the data and runs your transformations using the cache and duckdb as a query engine. Currently you can define your transformations in dbt or Python (pands, arrows, polars, etc.). After running your transformations, the cache will sync the results to the output dataset in your destination. Output schema is also automatically discovered (when not explicitly declared).


## Why you should use it

Using local cache for transformations provides several advantages, like:
1. your source schema is discovered automatically.
2. you save unnecessary compute costs by having all your transformations done locally. 
3. there are no egress costs since your data remains in your local system.
4. the same engine is used for transformations (i.e., SQL dialect) irrespective of where you're loading your data.
5. there is support for pythonic transformations (Hamilton/Kedro), dbt, sqlmesh, and sdf.
6. metadata is easily propagated from the input to the output dataset and dataset catalogs are manitained automatically.

