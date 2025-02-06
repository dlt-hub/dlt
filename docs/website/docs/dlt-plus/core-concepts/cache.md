# Local Transformation Cache

If you are familiar with dlt pipelines, the concept of local transformations is easy to grasp. Pipelines simplify and automate the loading of data. Local transformations simplify and automate the transformation of data — primarily locally. In a nutshell:

1. You pass a set of input dlt datasets to the transformation cache.
2. The cache discovers the inputs (source schemas) for your transformations.
3. The cache exposes your data locally using duckdb (we support VIEWs for data lakes and full table copy for other destinations).
4. You can use the cache and duckdb as a query engine to run your transformations (currently, we support dbt and anything Python: pandas, arrow, polars, etc.).
5. The cache infers the output schema (if not declared) and syncs the results of the transformations to the output dataset.  

Why you should use it:

1. Automatic source schema discovery.
2. Save costs by transforming locally.
3. No egress cost when close to the data.
4. The same engine (i.e., SQL dialect) no matter what the final destination.
5. Python transformations (Hamilton/Kedro).
6. dbt, sqlmesh, sdf supported.
7. Metadata propagation from input to output dataset, automatic cataloging.  
  

Currently, a lot of things below are WIP:

1. A local (ad hoc) data catalog and a data cache for larger, distributed data (see your data lake and report tables in one place).
2. A local query engine (duckdb): universal schema and SQL dialect for transformations.
3. Arrow/polars transformations (via Python modules).
4. Incremental transformations [partial 🚧 WIP! - _dlt_load_id currently supported].
5. Syncing the cache back to output datasets.
6. Declarative cache behavior [🚧 WIP!].
7. Convenient Python interface [🚧 WIP!].
8. Many input and output datasets [🚧 WIP!].
