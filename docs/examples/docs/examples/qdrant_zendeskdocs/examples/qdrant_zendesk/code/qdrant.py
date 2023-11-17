import dlt
from dlt.destinations.qdrant import qdrant_adapter
from qdrant_client import QdrantClient

from zendesk import zendesk_support

def main():
    # 1. Create a pipeline
    pipeline = dlt.pipeline(
        pipeline_name="qdrant_zendesk_pipeline",
        destination="qdrant",
        dataset_name="zendesk_data_tickets",
    )

    # 2. Initialize Zendesk source to get the ticket data
    zendesk_source = zendesk_support(load_all=False)
    tickets = zendesk_source.tickets

    # 3. Run the dlt pipeline
    info = pipeline.run(
        # 4. Here we use a special function to tell Qdrant
        # which fields to embed
         qdrant_adapter(
        tickets,
        embed=["subject", "description"],
        )
    )

    return info

if __name__ == "__main__":
    load_info = main()
    print(load_info)


    # running the Qdrant client
    qdrant_client = QdrantClient(
        url="https://your-qdrant-url",
        api_key="your-qdrant-api-key",
    )

    # view Qdrant collections
    print(qdrant_client.get_collections())

    response = qdrant_client.query(
        "zendesk_data_tickets",  # collection/dataset name
        query_text=["cancel", "cancel subscription"],  # prompt to search
        limit=3  # limit the number of results to the nearest 3 embeddings
    )

    print(response)