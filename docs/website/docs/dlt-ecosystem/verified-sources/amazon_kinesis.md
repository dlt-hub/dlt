---
title: Amazon Kinesis
description: dlt verified source for Amazon Kinesis
keywords: [amazon kinesis, verified source]
---
import Header from './_source-info-header.md';

# Amazon Kinesis

<Header/>

[Amazon Kinesis](https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html) is a cloud-based service for real-time data streaming and analytics, enabling the processing and analysis of large streams of data in real time.

Our AWS Kinesis [verified source](https://github.com/dlt-hub/verified-sources/tree/master/sources/kinesis) loads messages from Kinesis streams to your preferred [destination](../../dlt-ecosystem/destinations/).

Resources that can be loaded using this verified source are:

| Name             | Description                                                                              |
|------------------|------------------------------------------------------------------------------------------|
| kinesis_stream   | Load messages from the specified stream                                                  |


:::tip
You can check out our pipeline example [here](https://github.com/dlt-hub/verified-sources/blob/master/sources/kinesis_pipeline.py).
:::

## Setup guide

### Grab credentials

To use this verified source, you need an AWS `Access key` and `Secret access key`, which can be obtained as follows:

1. Sign in to your AWS Management Console.
1. Navigate to the IAM (Identity and Access Management) dashboard.
1. Select "Users" and choose your IAM username.
1. Click on the "Security Credentials" tab.
1. Choose "Create Access Key".
1. Download or copy the Access Key ID and Secret Access Key for future use.

:::info
The AWS UI, which is described here, might change. The full guide is available at this [link](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html).
:::

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init kinesis duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/kinesis_pipeline.py) with Kinesis as the [source](../../general-usage/source) and [duckdb](../destinations/duckdb.md) as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and configuration settings to get started.

For more information, read [Add a verified source.](../../walkthroughs/add-a-verified-source)

### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive information securely, like access tokens. Keep this file safe. Here's its format for service account authentication:

   ```toml
   # Put your secret values and credentials here.
   # Note: Do not share this file and do not push it to GitHub!
   [sources.kinesis.credentials]
   aws_access_key_id="AKIA********"
   aws_secret_access_key="K+o5mj********"
   region_name="please set me up!" # aws region name
   ```

1. Optionally, you can configure `stream_name`. Update `.dlt/config.toml`:

   ```toml
   [sources.kinesis]
   stream_name = "please set me up!" # Stream name (Optional).
   ```

1. Replace the value of `aws_access_key_id` and `aws_secret_access_key` with the one that [you copied above](#grab-credentials). This will ensure that the verified source can access your Kinesis resource securely.

1. Next, follow the instructions in [Destinations](../destinations/duckdb) to add credentials for your chosen destination. This will ensure that your data is properly routed to its final destination.

For more information, read [Credentials](../../general-usage/credentials).

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:
   ```sh
   pip install -r requirements.txt
   ```
2. You're now ready to run the pipeline! To get started, run the following command:
   ```sh
   python kinesis_pipeline.py
   ```
3. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:
   ```sh
   dlt pipeline <pipeline_name> show
   ```
   For example, the `pipeline_name` for the above pipeline example is `kinesis_pipeline`. You may
   also use any custom name instead.

For more information, read [Run a pipeline.](../../walkthroughs/run-a-pipeline)

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Resource `kinesis_stream`

This resource reads a Kinesis stream and yields messages. It supports
[incremental loading](../../general-usage/incremental-loading) and parses messages as JSON by
default.

```py
@dlt.resource(
    name=lambda args: args["stream_name"],
    primary_key="_kinesis_msg_id",
    standalone=True,
)
def kinesis_stream(
    stream_name: str = dlt.config.value,
    credentials: AwsCredentials = dlt.secrets.value,
    last_msg: Optional[dlt.sources.incremental[StrStr]] = dlt.sources.incremental(
        "_kinesis", last_value_func=max_sequence_by_shard
    ),
    initial_at_timestamp: TAnyDateTime = 0.0,
    max_number_of_messages: int = None,
    milliseconds_behind_latest: int = 1000,
    parse_json: bool = True,
    chunk_size: int = 1000,
) -> Iterable[TDataItem]:
    ...
```

`stream_name`: Name of the Kinesis stream. Defaults to config/secrets if unspecified.

`credentials`: Credentials for Kinesis access. Uses secrets or local credentials if not provided.

`last_msg`: Mapping from shard_id to a message sequence for incremental loading.

`initial_at_timestamp`: Starting timestamp for AT_TIMESTAMP or LATEST iterator; defaults to 0.

`max_number_of_messages`: Max messages per run; may exceed by chunk_size. Default: None (no limit).

`milliseconds_behind_latest`: Milliseconds to lag behind shard top; default is 1000.

`parse_json`: Parses messages as JSON if True. Default: False.

`chunk_size`: Records fetched per request; default is 1000.

### How does it work?

You create a resource `kinesis_stream` by passing the stream name and a few other options. The
resource will have the same name as the stream. When you iterate this resource (or pass it to
`pipeline.run` records), it will query Kinesis for all the shards in the requested stream. For each
 shard, it will create an iterator to read messages:

1. If `initial_at_timestamp` is present, the resource will read all messages after this timestamp.
2. If `initial_at_timestamp` is 0, only the messages at the tip of the stream are read.
3. If no initial timestamp is provided, all messages will be retrieved (from the TRIM HORIZON).

The resource stores all message sequences per shard in the state. If you run the resource again, it
will load messages incrementally:

1. For all shards that had messages, only messages after the last message are retrieved.
2. For shards that didn't have messages (or new shards), the last run time is used to get messages.

Please check the `kinesis_stream` [docstring](https://github.com/dlt-hub/verified-sources/blob/master/sources/kinesis/__init__.py#L31-L46)
for additional options, i.e., to limit the number of messages
returned or to automatically parse JSON messages.

### Kinesis message format

The `_kinesis` dictionary in the message stores the message envelope, including shard id, sequence,
partition key, etc. The message contains `_kinesis_msg_id`, which is the primary key: a hash over
(shard id + message sequence number). With `parse_json` set to True (default), the Data field is parsed;
if False, `data` is returned as bytes.

## Customization



### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this verified source.

1. Configure the [pipeline](../../general-usage/pipeline) by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
       pipeline_name="kinesis_pipeline",  # Use a custom name if desired
       destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
       dataset_name="kinesis"  # Use a custom name if desired
   )
   ```

1. To load messages from a stream from the last one hour:

   ```py
   # The resource below will take its name from the stream name,
   # it can be used multiple times. By default, it assumes that data is JSON and parses it,
   # here we disable that to just get bytes in data elements of the message.
   kinesis_stream_data = kinesis_stream(
       "kinesis_source_name",
       parse_json=False,
       initial_at_timestamp=pendulum.now().subtract(hours=1),
   )
   info = pipeline.run(kinesis_stream_data)
   print(info)
   ```

1. For incremental Kinesis streams, to fetch only new messages:

   ```py
   # Running pipeline will get only new messages.
   info = pipeline.run(kinesis_stream_data)
   message_counts = pipeline.last_trace.last_normalize_info.row_counts
   if "kinesis_source_name" not in message_counts:
       print("No messages in kinesis")
   else:
       print(pipeline.last_trace.last_normalize_info)
   ```

1. To parse JSON with a simple decoder:

   ```py
   def _maybe_parse_json(item: TDataItem) -> TDataItem:
       try:
           item.update(json.loadb(item["data"]))
       except Exception:
           pass
       return item

   info = pipeline.run(kinesis_stream_data.add_map(_maybe_parse_json))
   print(info)
   ```

1. To read Kinesis messages and send them somewhere without using a pipeline:

   ```py
   from dlt.common.configuration.container import Container
   from dlt.common.pipeline import StateInjectableContext

   STATE_FILE = "kinesis_source_name.state.json"

   # Load the state if it exists.
   if os.path.exists(STATE_FILE):
       with open(STATE_FILE, "rb") as f:
           state = json.typed_loadb(f.read())
   else:
       # Provide new state.
       state = {}

   with Container().injectable_context(
       StateInjectableContext(state=state)
   ) as managed_state:
       # dlt resources/source is just an iterator.
       for message in kinesis_stream_data:
           # Here you can send the message somewhere.
           print(message)
           # Save state after each message to have full transaction load.
           # DynamoDB is also OK.
           with open(STATE_FILE, "wb") as f:
               json.typed_dump(managed_state.state, f)
           print(managed_state.state)
   ```

<!--@@@DLT_TUBA kinesis-->

