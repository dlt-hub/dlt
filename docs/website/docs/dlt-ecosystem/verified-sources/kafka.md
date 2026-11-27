---
title: Kafka
description: dlt verified source for Confluent Kafka
keywords: [kafka api, kafka verified source, kafka]
---
# Kafka

import Header from './_source-info-header.md';

<Header/>

[Kafka](https://www.confluent.io/) is an open-source distributed event streaming platform, organized
in the form of a log with message publishers and subscribers.
The Kafka `dlt` verified source loads data using the Confluent Kafka API to the destination of your choice.
See a [pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/kafka_pipeline.py).

Every run reads the messages up to the end that each partition had when the run started. Then the
run stops. The pipeline state keeps the last offset read from each partition, and the next run
continues after it. dlt does not commit offsets to the Kafka consumer group. The source works with
`confluent-kafka>=2.13.2,<3` and `dlt>=1.28.1`.

The resource that can be loaded:

| Name           | Description                         |
| -------------- | ----------------------------------- |
| kafka_consumer | Extracts messages from Kafka topics |

## Setup guide

### Grab Kafka cluster credentials

1. Follow the [Kafka Setup](https://developer.confluent.io/get-started/python/#kafka-setup) to tweak a
project.
2. Follow the [Configuration](https://developer.confluent.io/get-started/python/#configuration) to
get the project credentials.

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```cli
   dlt init kafka duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/kafka_pipeline.py)
   with Kafka as the [source](../../general-usage/source) and [duckdb](../destinations/duckdb.md)
   as the [destination](../destinations).

2. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

3. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe.

   Use the following format for Confluent Cloud API keys and the other `SASL` mechanisms
   (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`):

```toml
[sources.kafka.credentials]
bootstrap_servers="web.address.gcp.confluent.cloud:9092"
security_protocol="SASL_SSL"
sasl_mechanisms="PLAIN"
sasl_username="example_username"
sasl_password="example_secret"
```

   The credential fields are librdkafka properties with underscores instead of periods. For
   example, `bootstrap_servers` is `bootstrap.servers`. `group_id` is optional, and its default is
   `dlt`.

   For SSL with client certificates (mTLS), give the certificates as PEM strings. You can also give
   file paths in `ssl_ca_location`, `ssl_certificate_location` and `ssl_key_location`:

```toml
[sources.kafka.credentials]
bootstrap_servers="broker:9093"
security_protocol="SSL"
ssl_ca_pem="""-----BEGIN CERTIFICATE-----
...
-----END CERTIFICATE-----"""
ssl_certificate_pem="""-----BEGIN CERTIFICATE-----
...
-----END CERTIFICATE-----"""
ssl_key_pem="""-----BEGIN PRIVATE KEY-----
...
-----END PRIVATE KEY-----"""
```

   For OAuth, use OIDC client credentials:

```toml
[sources.kafka.credentials]
bootstrap_servers="broker:9093"
security_protocol="SASL_SSL"
sasl_mechanisms="OAUTHBEARER"
sasl_oauthbearer_method="oidc"
sasl_oauthbearer_token_endpoint_url="https://idp.example.com/oauth2/token"
sasl_oauthbearer_client_id="client_id"
sasl_oauthbearer_client_secret="client_secret"
sasl_oauthbearer_scope="kafka"
```

   For Azure Event Hubs, use `SASL_SSL` with `PLAIN` on port 9093. Set `sasl_username` to
   `$ConnectionString` and `sasl_password` to the connection string of the namespace.

   For Amazon MSK with IAM access control, install `aws-msk-iam-sasl-signer-python`. When
   `aws_region` is set, the credentials sign tokens with the default AWS credentials chain. You can
   also set `aws_profile` or `aws_role_arn`:

```toml
[sources.kafka.credentials]
bootstrap_servers="b-1.cluster.kafka.eu-central-1.amazonaws.com:9098"
aws_region="eu-central-1"
```

   Put any other librdkafka property in `consumer_config`. Kerberos needs its properties there.
   On Linux, Kerberos also needs `confluent-kafka` built from source. To read open transactions,
   set the isolation level:

```toml
[sources.kafka.credentials.consumer_config]
"isolation.level"="read_uncommitted"
```

2. Enter credentials for your chosen destination as per the [docs](../destinations/).

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:

   ```cli
   pip install -r requirements.txt
   ```

2. You're now ready to run the pipeline! To get started, run the following command:

   ```sh
   python kafka_pipeline.py
   ```

3. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:

   ```cli
   dlt pipeline <pipeline_name> show
   ```

For more information, read the [Walkthrough: Run a pipeline](../../walkthroughs/run-a-pipeline).

:::info
A run ends when dlt reads every partition to the offset that it had when the run started. A run
also ends when no message arrives within `batch_timeout`. Under the default
`isolation.level=read_committed`, messages of open transactions load on a later run.
:::

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Source `kafka_consumer`

This function retrieves messages from the given Kafka topics.

```py notype
@dlt.resource(name="kafka_messages", standalone=True)
def kafka_consumer(
    topics: Union[str, list[str]],
    credentials: Union[KafkaMskIamCredentials, KafkaCredentials, Consumer] = dlt.secrets.value,
    msg_processor: Union[BatchProcessor, Callable[[Message], dict[str, Any]]] = default_msg_processor,
    batch_size: int = 3000,
    batch_timeout: float = 3,
    start_from: Optional[TAnyDateTime] = None,
    end_time: Optional[TAnyDateTime] = None,
    request_timeout: float = 30,
    schema_registry: Optional[SchemaRegistryCredentials] = None,
    on_type_change: Literal["strict", "skip"] = "strict",
) -> Iterable[TDataItem]:
   ...
```

`topics`: A list of Kafka topics to be extracted.

`credentials`: By default, it is initialized with the data from
the `secrets.toml`. You can also pass an initialized Kafka `Consumer`. If you pass a `Consumer`,
set `enable.partition.eof` in it. Then the run ends after dlt reads all partitions.

`msg_processor`: Converts the messages before dlt loads them. A function gets every message and
returns a dict. The `_kafka.topic` key of the dict names the table. See the
[default processor](https://github.com/dlt-hub/verified-sources/blob/master/sources/kafka/helpers/processors.py)
as an example. The default processor loads keys and values as UTF-8 text, or as binary when they
are not valid UTF-8. Tombstones have a null value. `_kafka_msg_id` is a hash of topic, partition
and key, so messages with the same key share it. To load JSON or Avro values as typed columns,
pass a `JsonProcessor` or an `AvroProcessor`. See [Use Schema Registry](#use-schema-registry).

`batch_size`: The number of messages to extract from the cluster
at once. It can be set to tweak performance.

`batch_timeout`: The maximum time (in seconds) to wait for a batch. The run stops when no message
arrives in this time.

`start_from`: A timestamp. The run reads the messages from this time on. dlt asks the Kafka
cluster for the first offset with a timestamp at or after it. When the state has a later offset for
a partition, the partition continues after that offset. The next runs with the same `start_from`
read only new messages.

`end_time`: A timestamp. The run reads the messages before this time. With `end_time`, dlt does
not use or update the offsets in the state. You can load a window of messages, for example into
another pipeline. The window does not change what the next incremental run reads.

`request_timeout`: The timeout of the requests for topic metadata and offsets, in seconds. When dlt
cannot connect to a broker, the run fails after this time.

`schema_registry`: A Schema Registry with the Confluent API. By default, dlt takes it from
`[sources.kafka.schema_registry]` in `secrets.toml`. See
[Use Schema Registry](#use-schema-registry).

`on_type_change`: What happens when a new schema version changes the type of a column that dlt
already loaded. See [Column type changes](#column-type-changes).

## Customization

### Create your own pipeline

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
        pipeline_name="kafka",     # Use a custom name if desired
        destination="duckdb",      # Choose the appropriate destination (e.g., duckdb, redshift, post)
        dataset_name="kafka_data"  # Use a custom name if desired
   )
   ```

2. To extract several topics:

   ```py notype
   topics = ["topic1", "topic2", "topic3"]

   resource = kafka_consumer(topics)
   pipeline.run(resource, write_disposition="replace")
   ```

3. To extract messages and process them in a custom way:

   ```py notype
    def custom_msg_processor(msg: confluent_kafka.Message) -> dict[str, Any]:
        key, value = msg.key(), msg.value()
        return {
            "_kafka": {
                "topic": msg.topic(),  # required field
                "key": key.decode("utf-8") if key is not None else None,
                "partition": msg.partition(),
            },
            # tombstones have no value
            "data": value.decode("utf-8") if value is not None else None,
        }

    resource = kafka_consumer("topic", msg_processor=custom_msg_processor)
    pipeline.run(resource)
   ```

4. To extract messages from a timestamp on, pass `start_from`. The next runs continue after the
   last message that dlt read:

   ```py notype
    from datetime import datetime, timezone

    resource = kafka_consumer("topic", start_from=datetime(2023, 12, 15, tzinfo=timezone.utc))
    pipeline.run(resource)
   ```

5. To load a window of messages and keep the offsets in the state as they are:

   ```py notype
    resource = kafka_consumer(
        "topic",
        start_from=datetime(2023, 12, 1, tzinfo=timezone.utc),
        end_time=datetime(2023, 12, 15, tzinfo=timezone.utc),
    )
    pipeline.run(resource)
   ```

## Use Schema Registry

A Schema Registry keeps the schemas that producers write their messages with. With a registry, dlt
loads message values as typed columns. A message processor decodes every value and types its
columns with the schema of the message. The processor depends on the format of the values:

| Processor       | Values       | Registry                                    |
| --------------- | ------------ | ------------------------------------------- |
| `JsonProcessor` | JSON objects | optional, without it dlt infers the types   |
| `AvroProcessor` | Avro records | required, the values carry only a schema id |

dlt emits the schema of a topic as table hints before the first message. When a message brings a
new schema version, dlt emits the hints again. Columns that you set with `apply_hints` win.

### Configure the registry

Install `confluent-kafka[schemaregistry]`. Then add the registry to `secrets.toml`:

```toml
[sources.kafka.schema_registry]
url = "https://psrc-xxxxx.region.provider.confluent.cloud"
basic_auth_user_info = "<api key>:<api secret>"
```

For a registry with TLS, set `ssl_ca_location`, `ssl_certificate_location` and `ssl_key_location`.
Put any other property of the registry client in `client_config`, for example
`bearer.auth.credentials.source`. You can also pass `schema_registry` to `kafka_consumer`.

### Load JSON values as columns

`JsonProcessor` loads values that are JSON objects. Their fields become columns, and nested objects
become `parent__child` columns. With `backend="pyarrow"`, it parses the values directly into Arrow
tables, several times faster than into Python dicts. `"auto"` picks `pyarrow` when it is installed.

```py notype
from kafka import JsonProcessor, kafka_consumer

resource = kafka_consumer("orders", msg_processor=JsonProcessor(backend="pyarrow"))
pipeline.run(resource)
```

JSON values load with or without a registry:

- **With a registry**: values written with a Schema Registry serializer get the types of their JSON
  Schema. For example, `integer` loads as `bigint`, `date-time` strings load as timestamps, and
  arrays load as `json`. Values without a schema id get the types that dlt infers.
- **Without a registry**: the values are schema-less. dlt infers the column types from the values,
  as for any other JSON data. This is the default when you do not configure a registry.

Other options:

- `strict=True`: every topic must have a schema in the registry. Every message must have a schema
  id and no fields outside its schema. It needs a registry.
- `on_decode_error="skip"`: logs the messages that do not decode, and the run continues.
- `table_per_schema=True`: loads the messages of every schema into a table of their own, for
  example for topics with several record types. `table_name` names the tables.
- `envelope="kafka_connect"`: loads the payload of values that the Kafka Connect `JsonConverter`
  writes with `schemas.enable=true`, for example for Debezium. The schema next to the payload types
  the columns, so this option needs no registry.

### Load Avro values as columns

`AvroProcessor` loads Avro values that a Schema Registry serializer or the Kafka Connect
`AvroConverter` wrote. Their fields become columns, typed by their schema. It needs a registry.
Install `confluent-kafka[schemaregistry,avro]`, then
[configure the registry](#configure-the-registry).

```py notype
from kafka import AvroProcessor, kafka_consumer

resource = kafka_consumer("orders", msg_processor=AvroProcessor(backend="pyarrow"))
pipeline.run(resource)
```

- Records become `parent__child` columns. Decimals, dates, times, timestamps and UUIDs load with
  their types. The time and decimal types of Kafka Connect and Debezium also load with their types.
- Arrays, maps and unions of several types load as `json`. Decimals with more than 38 digits load
  as text, and nanosecond timestamps load as microseconds.
- Keys written with an Avro schema load into `_kafka__key`. A record key loads into
  `_kafka__key__<field>` columns, for example the primary key of a Debezium change event.
- dlt resolves schema references. A topic with several record types has a union of them as its
  schema. Its messages load into one table, or into a table per record type with
  `table_per_schema=True`.
- Every message must have a schema id. A value without a schema id is a decode error. A value with
  bytes that its schema does not read is a decode error too. To log and skip such messages, set
  `on_decode_error="skip"`.
- `AvroProcessor` does not apply the data contract rules of a schema. For example, encrypted fields
  load as ciphertext.

### Column type changes

A new schema version can change the type of a field, for example an Avro `int` to a `double`. dlt
does not change the type of a column in the destination. Without a check, the destination column
keeps the old type, and `2.5` loads as `2`.

With `on_type_change="strict"`, the default, dlt compares the new type with the type of every
column that it already loaded:

- If every value of the new type fits the column, the column keeps its type. For example, a
  `decimal(10,2)` fits a `decimal(12,2)` column.
- If not, the run fails with `IncompatibleColumnTypeException` before it loads the messages of the
  new schema. The error names the column, both types and the `columns` hint for the new type. To
  continue, do one of these steps:
  - Change the type of the destination column yourself. Then apply the hint to the resource.
  - Reload the table with `refresh="drop_resources"`. The refresh also resets the stored offsets.

With `on_type_change="skip"`, dlt emits the new type and the `data_type`
[schema contract](../../general-usage/schema-contracts.md) of the resource decides. To fail the run
with `DataValidationError`, set this contract to `freeze`:

```py notype
resource = kafka_consumer(
    "payments", msg_processor=AvroProcessor(), on_type_change="skip"
)
resource.apply_hints(schema_contract={"data_type": "freeze"})
pipeline.run(resource)
```

With the default `evolve` contract, the type changes in the dlt schema only, and the destination
column keeps the old type.

<!--@@@DLT_TUBA kafka-->
