import dlt
from dlt.helpers.pandas import query_results_to_df

pipeline = dlt.pipeline(destination="duckdb", dataset_name="jaffle_jaffle")

# create or restore virtual environment in which dbt is installed, use the newest version of dbt
venv = dlt.dbt.get_venv(pipeline)

# get runner, optionally pass the venv
dbt = dlt.dbt.package(pipeline, "https://github.com/dbt-labs/jaffle_shop.git", venv=venv)
# run the package (clone/pull repo, deps, seed, source tests, run)
models = dbt.run_all()
for m in models:
    print(f"Model {m.model_name} materialized in {m.time} with status {m.status} and message {m.message}")
# test the model
models = dbt.test()
for m in models:
    print(f"Test {m.model_name} executed in {m.time} with status {m.status} and message {m.message}")

# get and display dataframe with customers
with pipeline.sql_client() as client:
    print(query_results_to_df(client, "SELECT * FROM customers"))
