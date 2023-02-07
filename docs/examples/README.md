# Writing Source Extractors
`dlt` sources are iterators or lists and writing them does not require any additional knowledge beyond basic python. `dlt` sources are also pythonic in nature: they are simple, can be chained, pipelined and composed like any other python iterator or a sequence.


# Examples

1. `quickstart` loads a nested json document into `duckdb` and then queries it with built in `sql_client` demonstrating the parent-child table joins.

1. `sql_query` source and `read_table` example. This source iterates over any `SELECT` statement made against database system supported by `SqlAlchemy`. The example connects to Redshift and iterates a table containing Ethereum transactions. Shows the inferred schema (which nicely preserves typing). Mind that our source is a one-liner :)

1. `rasa` example and `rasa_tracker_store` source extracts rasa tracker store events to a set of inferred tables. It shows a few common patterns
- shows how to **pipeline resources**: it depends on a "head" resource that reads base data (ie. events from kafka/postgres/file). the dependent resource is called `transformer`
- it shows how to write **stream resource** which creates table schemas and sends data to those tables depending on the event type
- it stores `last_timestamp_value` in the state

1. `singer_tap`, `stdout` and `singer_tap_example` is fully functional wrapper for any singer/meltano source
- clones the desired tap, installs it and runs it in a virtual env
- passes the catalog and config files
- like rasa it is a **transformer** (on stdio pipe) and `stream` resource
- it stores singer state in `dlt` state

1. `singer_tap_jsonl_example` like the above but instead of process pipe it reads singer messages from file. it creates a huge hubspot schema.

1. `google_sheets` a source that returns values from specified sheet. The example takes a sheet, infers a schema, loads it to BigQuery/Redshift and displays inferred schema. it uses the `secrets.toml` to manage credentials and is an example of one-liner pipeline

1. `chess` an example of a pipeline project with its own config and credential files. it is also an example of how transformers are connected to resources and resource selection. **it should be run from examples/chess` folder**

2. `chess/chess_dbt.py`: an example of a `dbt` transformations package working with a dataset loaded by `dlt`. The package is incrementally processing the loaded data following the new loaded packages stored in `_dlt_loads` table at the end of every pipeline run. Note the automatic usage of isolated virtual environment to run dbt and sharing of the credentials.

1. `run_dbt_jaffle` runs dbt's jaffle shop example taken directly from the github repo and queries the results with `sql_client`. `duckdb` database is used to load and transform the data. The database `write` access is passed from `dlt` to `dbt` and back.

Not yet ported:

1. `discord_iterator` an example that load example discord data (messages, channels) into warehouse from supplied files. Shows several auxiliary pipeline functions and an example of pipelining iterators (with `map` function). You can also see that produced schema is quite complicated due to several layers of nesting.

2. `ethereum` source shows that you can build highly scalable, parallel and robust sources as simple iterators.

