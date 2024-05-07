"""
---
title: Custom Destination with LanceDB
description: Learn how use the custom destination to load to LanceDB.
keywords: [destination, credentials, example, lancedb, custom destination, vectorstore, AI, LLM]
---

This example showcases a Python script that demonstrates the integration of LanceDB, an open-source vector database,
as a custom destination within the dlt ecosystem.
The script illustrates the implementation of a custom destination as well as the population of the LanceDB vector
store with data from various sources.
This highlights the seamless interoperability between dlt and LanceDB.

We'll learn how to:
- Use the [custom destination](../dlt-ecosystem/destinations/destination.md)
- Delegate the embeddings to LanceDB
"""

import datetime  # noqa: I251
import os
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Dict, Any, Optional

import lancedb  # type: ignore
from lancedb.embeddings import get_registry, OpenAIEmbeddings  # type: ignore
from lancedb.pydantic import LanceModel, Vector  # type: ignore

import dlt
from dlt.common.schema import TTableSchema
from dlt.common.typing import TDataItems
from dlt.sources.helpers import requests


BASE_SPOTIFY_URL = "https://api.spotify.com/v1"
os.environ["SPOTIFY__CLIENT_ID"] = ""
os.environ["SPOTIFY__CLIENT_SECRET"] = ""
os.environ["OPENAI_API_KEY"] = ""

DB_PATH = "spotify.db"

# LanceDB global registry keeps track of text embedding callables implicitly.
openai = get_registry().get("openai")

embedding_model = openai.create()

db_path = Path(DB_PATH)


class EpisodeSchema(LanceModel):
    id: str  # noqa: A003
    name: str
    description: str = embedding_model.SourceField()
    vector: Vector(embedding_model.ndims()) = embedding_model.VectorField()  # type: ignore[valid-type]
    release_date: datetime.date
    href: str


@dataclass(frozen=True)
class Shows:
    monday_morning_data_chat: str = "3Km3lBNzJpc1nOTJUtbtMh"
    latest_space_podcast: str = "2p7zZVwVF6Yk0Zsb4QmT7t"
    superdatascience_podcast: str = "1n8P7ZSgfVLVJ3GegxPat1"
    lex_fridman: str = "2MAi0BvDc6GTFvKFPXnkCL"


def get_spotify_access_token(client_id: str, client_secret: str) -> str:
    auth_url = "https://accounts.spotify.com/api/token"

    auth_response = requests.post(
        auth_url,
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )

    return auth_response.json()["access_token"]  # type: ignore[no-any-return]


def fetch_show_episode_data(
    show_id: str, access_token: Optional[str] = None, params: Dict[str, Any] = None
):
    """Fetch all shows data from Spotify API based on endpoint and params."""
    url = f"{BASE_SPOTIFY_URL}/shows/{show_id}/episodes"
    if params is None:
        params = {}
    headers = {"Authorization": f"Bearer {access_token}"} if access_token else {}
    while True:
        response_ = requests.get(url, params=params, headers=headers)
        response_.raise_for_status()
        response = response_.json()
        yield response["items"]
        if not response or "next" not in response or response["next"] is None:
            break
        url = response["next"]


@dlt.source
def spotify_shows(client_id: str = dlt.secrets.value, client_secret: str = dlt.secrets.value):
    access_token: str = get_spotify_access_token(client_id, client_secret)
    params: Dict[str, Any] = {"limit": 50}
    for show in fields(Shows):
        show_name: str = show.name
        show_id: str = show.default  # type: ignore[assignment]
        yield dlt.resource(
            fetch_show_episode_data(show_id, access_token, params),
            name=show_name,
            write_disposition="merge",
            primary_key="id",
            parallelized=True,
            max_table_nesting=0,
        )


@dlt.destination(batch_size=250, name="lancedb")
def lancedb_destination(items: TDataItems, table: TTableSchema) -> None:
    db = lancedb.connect(db_path, read_consistency_interval=datetime.timedelta(0))
    try:
        tbl = db.open_table(table["name"])
    except FileNotFoundError:
        tbl = db.create_table(table["name"], schema=EpisodeSchema)
    tbl.checkout_latest()
    tbl.add(items)


if __name__ == "__main__":
    db = lancedb.connect(db_path, read_consistency_interval=datetime.timedelta(0))

    for show in fields(Shows):
        db.drop_table(show.name, ignore_missing=True)

    pipeline = dlt.pipeline(
        pipeline_name="spotify",
        destination=lancedb_destination,
        dataset_name="spotify_podcast_data",
        progress="log",
    )

    load_info = pipeline.run(
        spotify_shows(client_id=dlt.secrets.value, client_secret=dlt.secrets.value)
    )

    row_counts = pipeline.last_trace.last_normalize_info

    print(row_counts)
    print("------")

    print(load_info)

    print("------")
    print("------")

    # Showcase vector search capabilities over our dataset with lancedb.
    # Perform brute force search while we have small data.
    query = "French AI scientist with Lex, talking about AGI and Meta and Llama"
    table_to_query = "lex_fridman"

    print(f"Query: {query}")
    print(f"Querying table: {table_to_query}")

    tbl = db.open_table(table_to_query)
    tbl.checkout_latest()

    results = tbl.search(query=query).to_list()
    print(results)
