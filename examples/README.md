# Writing Sources
DLT sources are iterators or lists and writing them does not require any additional knowledge beyond basic python. DLT sources are also pythonic in nature: they are simple, can be chained, pipelined and composed like any other python iterator or sequence.


# Examples

1. `google_sheets` a source that returns values from specified sheet. The example takes a sheet, infers a schema, loads it to BigQuery/Redshift and displays inferred schema.

2. `discord_iterator` an example that load example discord data (messages, channels) into warehouse from supplied files. Shows several auxiliary pipeline functions and an example of pipelining iterators (with `map` function). You can also see that produced schema is quite complicated due to several layers of nesting.

3. `sql_query` source and `read_table` example. This source iterates over any `SELECT` statement made against database system supported by `SqlAlchemy`. The example connects to Redshift and iterates a table containing Ethereum transactions. Shows the inferred schema (which nicely preserves typing). Mind that our source is a one-liner :)

4. `ethereum` source shows that you can build highly scalable, parallel and robust sources as simple iterators.

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

5. `quickstart` and `demo` accompanies [Quickstart](../QUICKSTART.md) and shows how you can load a complex nested json and get nice schema at the end.

6. `rasa` example and `rasa_tracker_store` source extracts rasa tracker store events to a set of inferred tables. it's also an example of pipelining sources (ie. events from kafka/file source are fed into rasa source to obtain final iterator)
