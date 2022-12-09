import dlt
from dlt.destinations import postgres
from dlt.common.schema import utils

from examples.sources.sql_query import query_table, query_sql

# the connection string to redshift instance holding some ethereum data
# the connection string does not contain the password element and you should provide it in environment variable: SOURCES__CREDENTIALS__PASSWORD
source_dsn = "redshift+redshift_connector://loader@chat-analytics.czwteevq7bpe.eu-central-1.redshift.amazonaws.com:5439/chat_analytics_rasa"

# get data from table, we preserve method signature from pandas
items = query_table("blocks__transactions", source_dsn, table_schema_name="mainnet_2_ethereum", coerce_float=False)
# the data is also an iterator

for i in items:
    assert isinstance(i, dict)
    for k, v in i.items():
        print(f"{k}:{v} ({type(v)}:{utils.py_type_to_sc_type(type(v))})")

# get data from query
items = query_sql("select *  from mainnet_2_ethereum.blocks__transactions limit 10", source_dsn)

# and load it into a local postgres instance
# the connection string does not have the password part. provide it in DESTINATION__CREDENTIALS__PASSWORD
# you can find a docker compose file that spins up required instance in tests/load/postgres
# note: run the script without required env variables to see info on possible secret configurations that were tried

info = dlt.pipeline().run(items, destination=postgres, dataset_name="ethereum", table_name="transactions")
print(info)
