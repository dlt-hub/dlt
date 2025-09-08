"""
---
title: Custom destination with LanceDB
description: Learn how use the custom destination to load to LanceDB.
keywords: [destination, credentials, example, lancedb, custom destination, vectorstore, AI, LLM]
---

This example showcases a Python script that demonstrates the integration of LanceDB, an open-source vector database,
as a custom destination within the dlt ecosystem.
The script illustrates the implementation of a custom destination as well as the population of the LanceDB vector
store with data from various sources.
This highlights the seamless interoperability between dlt and LanceDB.

You can get a Spotify client ID and secret from https://developer.spotify.com/.

We'll learn how to:
- Use the [custom destination](../dlt-ecosystem/destinations/destination.md)
- Delegate the embeddings to LanceDB using OpenAI Embeddings
"""

__source_name__ = "spotify"

import datetime  # noqa: I251
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

import lancedb  # type: ignore
from lancedb.embeddings import get_registry  # type: ignore
from lancedb.pydantic import LanceModel, Vector  # type: ignore

import dlt
from dlt.common.configuration import configspec
from dlt.common.schema import TTableSchema
from dlt.common.typing import TDataItems, TSecretStrValue
from dlt.sources.helpers import requests
from dlt.sources.helpers.rest_client import RESTClient, AuthConfigBase

# access secrets to get openai key and instantiate embedding function
openai_api_key: str = dlt.secrets.get(
    "destination.lancedb.credentials.embedding_model_provider_api_key"
)
func = get_registry().get("openai").create(name="text-embedding-3-small", api_key=openai_api_key)


class EpisodeSchema(LanceModel):
    id: str  # noqa: A003
    name: str
    description: str = func.SourceField()
    vector: Vector(func.ndims()) = func.VectorField()  # type: ignore[valid-type]
    release_date: datetime.date
    href: str


@dataclass(frozen=True)
class Shows:
    monday_morning_data_chat: str = "3Km3lBNzJpc1nOTJUtbtMh"
    latest_space_podcast: str = "2p7zZVwVF6Yk0Zsb4QmT7t"
    superdatascience_podcast: str = "1n8P7ZSgfVLVJ3GegxPat1"
    lex_fridman: str = "2MAi0BvDc6GTFvKFPXnkCL"


@configspec
class SpotifyAuth(AuthConfigBase):
    client_id: str = None
    client_secret: TSecretStrValue = None

    def __call__(self, request) -> Any:
        if not hasattr(self, "access_token"):
            self.access_token = self._get_access_token()
        request.headers["Authorization"] = f"Bearer {self.access_token}"
        return request

    def _get_access_token(self) -> Any:
        auth_url = "https://accounts.spotify.com/api/token"
        auth_response = requests.post(
            auth_url,
            {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        )
        return auth_response.json()["access_token"]


@dlt.source
def spotify_shows(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
):
    spotify_base_api_url = "https://api.spotify.com/v1"
    client = RESTClient(
        base_url=spotify_base_api_url,
        auth=SpotifyAuth(client_id=client_id, client_secret=client_secret),
    )

    for show in fields(Shows):
        show_name = show.name
        show_id = show.default
        url = f"/shows/{show_id}/episodes"
        yield dlt.resource(
            client.paginate(url, params={"limit": 50}),
            name=show_name,
            write_disposition="merge",
            primary_key="id",
            parallelized=True,
            max_table_nesting=0,
        )


@dlt.destination(batch_size=250, name="lancedb")
def lancedb_destination(items: TDataItems, table: TTableSchema) -> None:
    db_path = Path(dlt.config.get("lancedb.db_path"))
    db = lancedb.connect(db_path)

    # since we are embedding the description field, we need to do some additional cleaning
    # for openai. Openai will not accept empty strings or input with more than 8191 tokens
    for item in items:
        item["description"] = item.get("description") or "No Description"
        item["description"] = item["description"][0:8000]
    try:
        tbl = db.open_table(table["name"])
    except FileNotFoundError:
        tbl = db.create_table(table["name"], schema=EpisodeSchema)
    tbl.add(items)


if __name__ == "__main__":
    db_path = Path(dlt.config.get("lancedb.db_path"))
    db = lancedb.connect(db_path)

    for show in fields(Shows):
        db.drop_table(show.name, ignore_missing=True)

    pipeline = dlt.pipeline(
        pipeline_name="spotify",
        destination=lancedb_destination,
        dataset_name="spotify_podcast_data",
        progress="log",
    )

    load_info = pipeline.run(spotify_shows())
    print(load_info)

    row_counts = pipeline.last_trace.last_normalize_info
    print(row_counts)

    query = "French AI scientist with Lex, talking about AGI and Meta and Llama"
    table_to_query = "lex_fridman"

    tbl = db.open_table(table_to_query)

    results = tbl.search(query=query).to_list()
    assert results
