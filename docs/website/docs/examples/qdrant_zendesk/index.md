---
title: Load Zendesk tickets data to Qdrant
description: Learn how to use the dlt source, Zendesk and dlt destination, Qdrant to conduct a similarity search on your tickets data.
keywords: [similarity search, example]
---

<Header
    intro="In this tutorial, you will learn how to do use dlt to store your
    vectorized Zendesk tickets data in the dlt destination; qdrant. You can
    use qdrant's vectorization and similarity searching capabilities on your tickets data,
    while using dlt as a medium to automate your pipeline."
    slug="qdrant_zendesk"
    run_file="qdrant-snippets"
/>

Qdrant has been introduced as a new [destination](https://dlthub.com/docs/dlt-ecosystem/destinations/qdrant) for dlt, adding onto our list of high-performance vector databases. Similar to our guide on using the [Zendesk source to extract ticket data and store it into weaviate](https://dlthub.com/docs/walkthroughs/zendesk-weaviate#configuration), this article drafts a similar system to map the vectorized ticket data from Zendesk to Qdrant!

## Importing Ticket Data from Zendesk API to Qdrant

In order to begin the import, we must setup first. This means making sure we have the correct configuration and environment to start working. To do so, we first install dlt with Qdrant support:

```commandline
pip install dlt[qdrant]
```
Next, configure the destination credentials for Qdrant in .dlt/secrets.toml as follows:

```commandline
[destination.qdrant.credentials]
location = "https://your-qdrant-url"
api_key = "your-qdrant-api-key"
```
Lastly, within the same secrets.toml file, we must also declare credentials for the source, Zendesk:

```commandline
[sources.zendesk.zendesk_support.credentials]
password = "..."
subdomain = "..."
email = "..."
```

## Running the pipeline:

You are now setup to start writing a custom pipeline to connect your Zendesk ticket data to Qdrant. The following lines of code do exactly that:

<!--@@@DLT_SNIPPET_START ./code/qdrant-snippets.py::main_code-->
<!--@@@DLT_SNIPPET_END ./code/qdrant-snippets.py::main_code-->

Overview of the code above:
1. We create a pipeline with the name `qdrant_zendesk_pipeline` and the destination Qdrant.
2. Then, we initialize the Zendesk verified source. We only need to load the tickets data, so we get tickets resource from the source by getting the tickets attribute.
3. pipeline.run() runs the pipeline and returns information about the load process.
4. Since Qdrant is a vector database, it specializes in conducting similarity searches within its embedded data. To make that possible, we use the special Qdrant adapter to embed (or vectorize) our data before loading it.

## Querying the data

In the code above, we also imported the `QdrantClient`. That is what we will use to connect to Qdrant and see what sort of data we have thus stored so far.
We connect to the Qdrant client with our credentials:

```python
qdrant_client = QdrantClient(
    url="https://your-qdrant-url",
    api_key="your-qdrant-api-key",
)
```

If needed, you can list all your datasets or Qdrant "collections" with the following function call:

```python
qdrant_client.get_collections()
```

You should be able to see your own data there. For the purposes of this article, it would be the same name as the dataset you declared in the pipeline above, i.e. `zendesk_data_tickets`.

Next, we query Qdrant to conduct a similarity search using their "query" function. For example, we would like to see tickets that are similar to the ("subject", "description") pair: ("cancel", "cancel subscription"). It can be queried as follows:

```python
qdrant_client.query(
    "zendesk_data_tickets", # collection/dataset name
    query_text=["cancel", "cancel subscription"], # prompt to search
    limit=3 # limit the number of results to the nearest 3 embeddings
)
```
The query above gives the following response:
```json
[QueryResponse(id='6aeacd21-b3d0-5174-97ef-5aaa59486414', embedding=None, metadata={'_dlt_id': 'Nx3wBiL29xTgaQ', '_dlt_load_id': '1700130284.002391', 'allow_attachments': True, 'allow_channelback': False, 'assignee_id': 12765072569105, 'brand_id': 12765073054225, 'created_at': '2023-09-01T11:19:25+00:00', 'custom_status_id': 12765028278545, 'description': 'I have been trying to cancel my subscription but the system won’t let me do it. Can you please help?', 'from_messaging_channel': False, 'generated_timestamp': 1693567167, 'group_id': 12765036328465, 'has_incidents': False, 'id': 12, 'is_public': True, 'organization_id': 12765041119505, 'raw_subject': 'Unable to Cancel Subscription', 'requester_id': 12765072569105, 'status': 'open', 'subject': 'Unable to Cancel Subscription', 'submitter_id': 12765072569105, 'tags': ['test1'], 'test_field': 'test1', 'ticket_form_id': 12765054772497, 'updated_at': '2023-09-01T11:19:25+00:00', 'url': 'https://d3v-dlthub.zendesk.com/api/v2/tickets/12.json', 'via__channel': 'web'}, document='', score=0.89545774),
 QueryResponse(id='a22189c1-70ab-5421-938b-1caae3e7d6d8', embedding=None, metadata={'_dlt_id': 'bc/xloksL89EUg', '_dlt_load_id': '1700130284.002391', 'allow_attachments': True, 'allow_channelback': False, 'assignee_id': 12765072569105, 'brand_id': 12765073054225, 'created_at': '2023-07-18T17:23:42+00:00', 'custom_status_id': 12765028278545, 'description': 'ABCDEF', 'from_messaging_channel': False, 'generated_timestamp': 1689701023, 'group_id': 12765036328465, 'has_incidents': False, 'id': 4, 'is_public': True, 'organization_id': 12765041119505, 'raw_subject': 'What is this ticket', 'requester_id': 12765072569105, 'status': 'open', 'subject': 'What is this ticket', 'submitter_id': 12765072569105, 'tags': ['test1'], 'test_field': 'test1', 'ticket_form_id': 12765054772497, 'updated_at': '2023-07-18T17:23:42+00:00', 'url': 'https://d3v-dlthub.zendesk.com/api/v2/tickets/4.json', 'via__channel': 'web'}, document='', score=0.8643349),
 QueryResponse(id='ce2f1c5c-41c3-56c3-a31d-2399a7a9239d', embedding=None, metadata={'_dlt_id': 'ZMuFJZo0AJxV4A', '_dlt_load_id': '1700130284.002391', 'allow_attachments': True, 'allow_channelback': False, 'assignee_id': 12765072569105, 'brand_id': 12765073054225, 'created_at': '2023-03-14T10:52:28+00:00', 'custom_status_id': 12765028278545, 'description': 'X', 'from_messaging_channel': False, 'generated_timestamp': 1696163084, 'group_id': 12765036328465, 'has_incidents': False, 'id': 2, 'is_public': True, 'priority': 'high', 'raw_subject': 'SCRUBBED', 'requester_id': 13726460510097, 'status': 'deleted', 'subject': 'SCRUBBED', 'submitter_id': 12765072569105, 'tags': [], 'ticket_form_id': 13726337882769, 'type': 'question', 'updated_at': '2023-09-01T12:10:35+00:00', 'url': 'https://d3v-dlthub.zendesk.com/api/v2/tickets/2.json', 'via__channel': 'web'}, document='', score=0.8467072)]
```
To get a closer look at what the Zendesk ticket was, and how dlt dealt with it, we can index into the metadata of the first `QueryResponse` object:
```json lines
{'_dlt_id': 'Nx3wBiL29xTgaQ',
 '_dlt_load_id': '1700130284.002391',
 'allow_attachments': True,
 'allow_channelback': False,
 'assignee_id': 12765072569105,
 'brand_id': 12765073054225,
 'created_at': '2023-09-01T11:19:25+00:00',
 'custom_status_id': 12765028278545,
 'description': 'I have been trying to cancel my subscription but the system won’t let me do it. Can you please help?',
 'from_messaging_channel': False,
 'generated_timestamp': 1693567167,
 'group_id': 12765036328465,
 'has_incidents': False,
 'id': 12,
 'is_public': True,
 'organization_id': 12765041119505,
 'raw_subject': 'Unable to Cancel Subscription',
 'requester_id': 12765072569105,
 'status': 'open',
 'subject': 'Unable to Cancel Subscription',
 'submitter_id': 12765072569105,
 'tags': ['test1'],
 'test_field': 'test1',
 'ticket_form_id': 12765054772497,
 'updated_at': '2023-09-01T11:19:25+00:00',
 'url': 'https://d3v-dlthub.zendesk.com/api/v2/tickets/12.json',
 'via__channel': 'web'}
```