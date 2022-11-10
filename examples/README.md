# Writing Sources
DLT sources are iterators or lists and writing them does not require any additional knowledge beyond basic python. DLT sources are also pythonic in nature: they are simple, can be chained, pipelined and composed like any other python iterator or a sequence.


# Examples

1. `quickstart` accompanies [Quickstart](../QUICKSTART.md) and shows how you can load a complex nested json and get nice schema at the end.

2. `sql_query` source and `read_table` example. This source iterates over any `SELECT` statement made against database system supported by `SqlAlchemy`. The example connects to Redshift and iterates a table containing Ethereum transactions. Shows the inferred schema (which nicely preserves typing). Mind that our source is a one-liner :)

3. `rasa` example and `rasa_tracker_store` source extracts rasa tracker store events to a set of inferred tables. It shows a few common patterns
- shows how to **pipeline resources**: it depends on a "head" resource that reads base data (ie. events from kafka/postgres/file). the dependent resource is called `transformer`
- it shows how to write **stream resource** which creates table schemas and sends data to those tables depending on the event type
- it stores `last_timestamp_value` in the state

4. `singer_tap`, `stdout` and `singer_tap_example` is fully functional wrapper for any singer/meltano source
- clones the desired tap, installs it and runs it in a virtual env
- passes the catalog and config files
- like rasa it is a **transformer** (on stdio pipe) and `stream` resource
- it stores singer state in `dlt` state

5. `singer_tap_jsonl_example` like the above but instead of process pipe it reads singer messages from file. it creates a huge hubspot schema.

6. `google_sheets` a source that returns values from specified sheet. The example takes a sheet, infers a schema, loads it to BigQuery/Redshift and displays inferred schema. it uses the `secrets.toml` to manage credentials and is an example of one-liner pipeline

7. `chess` an example of a pipeline project with its own config and credential files. it is also an example of how transformers are connected to resources and resource selection. **it should be run from examples/chess` folder**

Not yet ported:

1. `discord_iterator` an example that load example discord data (messages, channels) into warehouse from supplied files. Shows several auxiliary pipeline functions and an example of pipelining iterators (with `map` function). You can also see that produced schema is quite complicated due to several layers of nesting.

2. `ethereum` source shows that you can build highly scalable, parallel and robust sources as simple iterators.


```
# this source gets full state of any Ethereum compatible blockchain.
# - can be easily scaled up to hundreds of parallel extractions (ie. via Airflow)
# - supports retrying if blockchain node fails
# - supports multi threaded extraction on single machine via `get_deferred_source`
# - supports pipeline state so it may be used to just get new blocks when they are available
# - the return value is iterator (or deferred iterator) so still mappings may be used (ie. to decode transactions and logs), see example
```

The example shows:
- how to use schema discovery
- how to write complex transformations (we convert `wei` into `ether` and this is not trivial)
- how to re-instantiate pipeline to get newly produced blockchain blocks

# Running

Prepare the repo (you need `make`)
1. install poetry `make install-poetry`
2. `make dev`
3. `poetry shell`

Run an example
1. Run from `examples` directory: `cd examples`
2. Some examples require credentials to run. see `example.secrets.toml` then copy to `secrets.toml` and put your values
3. Run with python ie. `python quickstart.py`
