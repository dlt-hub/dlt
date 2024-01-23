
import dlt, asyncio

@dlt.resource(table_name="hello")
async def async_gen_resource(idx):
    for l in ["a", "b", "c"] * 3:
        await asyncio.sleep(0.1)
        yield {"async_gen": idx, "letter": l}

pipeline_1 = dlt.pipeline("pipeline_1", destination="duckdb", full_refresh=True)
pipeline_1.run(
    async_gen_resource(10), table_name="hello"
)
with pipeline_1.sql_client() as c:
    with c.execute_query("SELECT * FROM hello") as cur:
        rows = list(cur.fetchall())
        for r in rows:
            print(r)

# pipeline_1.run(
#     async_gen_resource(11)
# )