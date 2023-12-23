---
title: Similarity Searching with Qdrant
description: Learn how to use the dlt source, Zendesk and dlt destination, Qdrant to conduct a similarity search on your tickets data.
keywords: [similarity search, example]
---
import Header from '../_examples-header.md';

<Header
    intro="In this tutorial, you will learn how to do use dlt to store your
    vectorized Zendesk tickets data in the dlt destination: Qdrant. You can
    use Qdrant's vectorization and similarity searching capabilities on your tickets data,
    while using dlt as a medium to automate your pipeline."
    slug="qdrant_zendesk"
    run_file="qdrant"
    destination="qdrant"
/>

This article outlines a system to map vectorized ticket data from Zendesk to Qdrant, similar to our guide on the topic concerning [Weaviate](https://dlthub.com/docs/dlt-ecosystem/destinations/qdrant). In this example, we will:
- Connect to our [Zendesk source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/zendesk).
- Extract tickets data from our Zendesk source.
- [Create a dlt pipeline](https://dlthub.com/docs/walkthroughs/create-a-pipeline) with Qdrant as destination.
- Vectorize/embed the tickets data from Zendesk.
- Pass the vectorized data to be stored in Qdrant via the dlt pipeline.
- Query data that we stored in Qdrant.
- Explore the similarity search results.

First, configure the destination credentials for [Qdrant](https://dlthub.com/docs/dlt-ecosystem/destinations/qdrant#setup-guide) and [Zendesk](https://dlthub.com/docs/walkthroughs/zendesk-weaviate#configuration) in `.dlt/secrets.toml`.

Next, make sure you have the following dependencies installed:

```commandline
pip install qdrant-client>=1.6.9
pip install fastembed>=0.1.1
```

## Connect to Zendesk and load tickets data

<!--@@@DLT_SNIPPET_START ./code/qdrant-snippets.py::zendesk_conn-->
```py
from typing import Optional, Dict, Any, Tuple

import dlt
from dlt.common import pendulum
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import TAnyDateTime
from dlt.sources.helpers.requests import client
from dlt.destinations.impl.qdrant import qdrant_adapter
from qdrant_client import QdrantClient

from dlt.common.configuration.inject import with_config

# function from: https://github.com/dlt-hub/verified-sources/tree/master/sources/zendesk
@dlt.source(max_table_nesting=2)
def zendesk_support(
    credentials: Dict[str, str] = dlt.secrets.value,
    start_date: Optional[TAnyDateTime] = pendulum.datetime(  # noqa: B008
        year=2000, month=1, day=1
    ),
    end_date: Optional[TAnyDateTime] = None,
):
    """
    Retrieves data from Zendesk Support for tickets events.

    Args:
        credentials: Zendesk credentials (default: dlt.secrets.value)
        start_date: Start date for data extraction (default: 2000-01-01)
        end_date: End date for data extraction (default: None).
            If end time is not provided, the incremental loading will be
            enabled, and after the initial run, only new data will be retrieved.

    Returns:
        DltResource.
    """
    # Convert start_date and end_date to Pendulum datetime objects
    start_date_obj = ensure_pendulum_datetime(start_date)
    end_date_obj = ensure_pendulum_datetime(end_date) if end_date else None

    # Extract credentials from secrets dictionary
    auth = (credentials["email"], credentials["password"])
    subdomain = credentials["subdomain"]
    url = f"https://{subdomain}.zendesk.com"

    # we use `append` write disposition, because objects in tickets_data endpoint are never updated
    #  so we do not need to merge
    # we set primary_key so allow deduplication of events by the `incremental` below in the rare case
    #  when two events have the same timestamp
    @dlt.resource(primary_key="id", write_disposition="append")
    def tickets_data(
        updated_at: dlt.sources.incremental[pendulum.DateTime] = dlt.sources.incremental(
            "updated_at",
            initial_value=start_date_obj,
            end_value=end_date_obj,
            allow_external_schedulers=True,
        )
    ):
        # URL For ticket events
        # 'https://d3v-dlthub.zendesk.com/api/v2/incremental/tickets_data.json?start_time=946684800'
        event_pages = get_pages(
            url=url,
            endpoint="/api/v2/incremental/tickets",
            auth=auth,
            data_point_name="tickets",
            params={"start_time": updated_at.last_value.int_timestamp},
        )
        for page in event_pages:
            yield ([_fix_date(ticket) for ticket in page])

            # stop loading when using end_value and end is reached.
            # unfortunately, Zendesk API does not have the "end_time" parameter, so we stop iterating ourselves
            if updated_at.end_out_of_range:
                return

    return tickets_data
```
<!--@@@DLT_SNIPPET_END ./code/qdrant-snippets.py::zendesk_conn-->

## Inititating a pipeline with Qdrant
<!--@@@DLT_SNIPPET_START ./code/qdrant-snippets.py::main_code-->
```py
if __name__ == "__main__":
    # create a pipeline with an appropriate name
    pipeline = dlt.pipeline(
        pipeline_name="qdrant_zendesk_pipeline",
        destination="qdrant",
        dataset_name="zendesk_data",
    )

    # run the dlt pipeline and save info about the load process
    load_info = pipeline.run(
        # here we use a special function to tell Qdrant which fields to embed
        qdrant_adapter(
            zendesk_support(),  # retrieve tickets data
            embed=["subject", "description"],
        )
    )

    print(load_info)
```
<!--@@@DLT_SNIPPET_END ./code/qdrant-snippets.py::main_code-->
## Querying the data

<!--@@@DLT_SNIPPET_START ./code/qdrant-snippets.py::declare_qdrant_client-->
```py
# running the Qdrant client to connect to your Qdrant database

@with_config(sections=("destination", "qdrant", "credentials"))
def get_qdrant_client(location=dlt.secrets.value, api_key=dlt.secrets.value):
    return QdrantClient(
        url=location,
        api_key=api_key,
    )

# running the Qdrant client to connect to your Qdrant database
qdrant_client = get_qdrant_client()

# view Qdrant collections you'll find your dataset here:
print(qdrant_client.get_collections())
```
<!--@@@DLT_SNIPPET_END ./code/qdrant-snippets.py::declare_qdrant_client-->

<!--@@@DLT_SNIPPET_START ./code/qdrant-snippets.py::get_response-->
```py
# query Qdrant with prompt: getting tickets info close to "cancellation"
response = qdrant_client.query(
    "zendesk_data_content",  # collection/dataset name with the 'content' suffix -> tickets content table
    query_text=["cancel", "cancel subscription"],  # prompt to search
    limit=3,  # limit the number of results to the nearest 3 embeddings
)
```
<!--@@@DLT_SNIPPET_END ./code/qdrant-snippets.py::get_response-->

The query above gives stores the following results in the `response` variable:
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