---
title: Kafka
description: dlt verified source for Confluent Kafka
keywords: [kafka api, kafka verified source, kafka]
---
import Header from './_source-info-header.md';

# Kafka

<Header/>

[Kafka](https://www.confluent.io/) is an open-source distributed event streaming platform, organized
in the form of a log with message publishers and subscribers.
The Kafka `dlt` verified source loads data using the Confluent Kafka API to the destination of your choice.
See a [pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/kafka_pipeline.py).

The resource that can be loaded:

| Name              | Description                                |
| ----------------- |--------------------------------------------|
| kafka_consumer    | Extracts messages from Kafka topics        |

## Setup guide

### Grab Kafka cluster credentials

1. Follow the [Kafka Setup](https://developer.confluent.io/get-started/python/#kafka-setup) to tweak a
project.
2. Follow the [Configuration](https://developer.confluent.io/get-started/python/#configuration) to
get the project credentials.

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
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

For more information, read the
[Walkthrough: Add a verified source.](../../walkthroughs/add-a-verified-source)

### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe.

   Use the following format for service account authentication:

```toml
[sources.kafka.credentials]
bootstrap_servers="web.address.gcp.confluent.cloud:9092"
group_id="test_group"
security_protocol="SASL_SSL"
sasl_mechanisms="PLAIN"
sasl_username="example_username"
sasl_password="example_secret"
```

2. Enter credentials for your chosen destination as per the [docs](../destinations/).

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:

   ```sh
   pip install -r requirements.txt
   ```

2. You're now ready to run the pipeline! To get started, run the following command:

   ```sh
   python kafka_pipeline.py
   ```

3. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:

   ```sh
   dlt pipeline <pipeline_name> show
   ```

For more information, read the [Walkthrough: Run a pipeline](../../walkthroughs/run-a-pipeline).

:::info If you created a topic and start reading from it immediately, the brokers may not yet be synchronized, and the offset from which `dlt` reads messages may become invalid. In this case, the resource will return no messages. Pending messages will be received on the next run (or when brokers synchronize).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Source `kafka_consumer`

This function retrieves messages from the given Kafka topics.

```py
@dlt.resource(name="kafka_messages", table_name=lambda msg: msg["_kafka"]["topic"], standalone=True)
def kafka_consumer(
    topics: Union[str, List[str]],
    credentials: Union[KafkaCredentials, Consumer] = dlt.secrets.value,
    msg_processor: Optional[Callable[[Message], Dict[str, Any]]] = default_msg_processor,
    batch_size: Optional[int] = 3000,
    batch_timeout: Optional[int] = 3,
    start_from: Optional[TAnyDateTime] = None,
) -> Iterable[TDataItem]:
   ...
```

`topics`: A list of Kafka topics to be extracted.

`credentials`: By default, it is initialized with the data from
the `secrets.toml`. It may be used explicitly to pass an initialized
Kafka Consumer object.

`msg_processor`: A function that will be used to process every message
read from the given topics before saving them in the destination.
It can be used explicitly to pass a custom processor. See the
[default processor](https://github.com/dlt-hub/verified-sources/blob/fe8ed7abd965d9a0ca76d100551e7b64a0b95744/sources/kafka/helpers.py#L14-L50)
as an example of how to implement processors.

`batch_size`: The number of messages to extract from the cluster
at once. It can be set to tweak performance.

`batch_timeout`: The maximum timeout (in seconds) for a single batch reading
operation. It can be set to tweak performance.

`start_from`: A timestamp, starting from which the messages must
be read. When passed, `dlt` asks the Kafka cluster for an offset,
which is actual for the given timestamp, and starts to read messages from
this offset.


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

   ```py
   topics = ["topic1", "topic2", "topic3"]

   source = kafka_consumer(topics)
   pipeline.run(source, write_disposition="replace")
   ```

3. To extract messages and process them in a custom way:

   ```py
    def custom_msg_processor(msg: confluent_kafka.Message) -> Dict[str, Any]:
        return {
            "_kafka": {
                "topic": msg.topic(),  # required field
                "key": msg.key().decode("utf-8"),
                "partition": msg.partition(),
            },
            "data": msg.value().decode("utf-8"),
        }

    data = kafka_consumer("topic", msg_processor=custom_msg_processor)
    pipeline.run(data)
   ```

4. To extract messages, starting from a timestamp:

   ```py
    data = kafka_consumer("topic", start_from=pendulum.datetime(2023, 12, 15))
    pipeline.run(data)
   ```

<!--@@@DLT_TUBA kafka-->

