import os

import dlt
from dlt.destinations import bigquery, postgres

from .sources.jsonl import jsonl_files
from .sources.rasa import rasa

from ._helpers import pub_bigquery_credentials

# let's load to bigquery, here we provide the credentials for our public project
# credentials = pub_bigquery_credentials
credentials = "postgres://loader@localhost:5432/dlt_data"


# this is also example of resource pipelining: one resource can be source of data for another one that we call "transformer"
# in case of rasa the base source can be a file, database table (see sql_query.py), kafka topic, rabbitmq queue etc. corresponding to store or broker type

# for the simplicity let's use jsonl source to read all files with events in a directory
event_files = jsonl_files([file for file in os.scandir("docs/examples/data/rasa_trackers")])

info = dlt.pipeline(
    dev_mode=True,
    destination=postgres,
    # export_schema_path=...  # uncomment to see the final schema in the folder you want
).run(
    rasa(
        event_files, store_last_timestamp=True
    ),  # also store last timestamp so we have no duplicate events
    credentials=credentials,  # if you skip this parameter, the credentials will be injected by the config providers
)

print(info)

# uncomment to see the final schema
# print(dlt.pipeline().default_schema.to_pretty_yaml())
