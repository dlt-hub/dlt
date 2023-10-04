import dlt

pipeline = dlt.pipeline(destination="duckdb", dataset_name="jaffle_jaffle")

print("create or restore virtual environment in which dbt is installed, use the newest version of dbt")
venv = dlt.dbt.get_venv(pipeline)

print("get runner, optionally pass the venv")
dbt = dlt.dbt.package(pipeline, "https://github.com/dbt-labs/jaffle_shop.git", venv=venv)

print("run the package (clone/pull repo, deps, seed, source tests, run)")
models = dbt.run_all()
for m in models:
    print(f"Model {m.model_name} materialized in {m.time} with status {m.status} and message {m.message}")

print("")
print("test the model")
models = dbt.test()
for m in models:
    print(f"Test {m.model_name} executed in {m.time} with status {m.status} and message {m.message}")

print("")
print("get and display data frame with customers")
with pipeline.sql_client() as client:
    with client.execute_query("SELECT * FROM customers") as curr:
        print(curr.df())
