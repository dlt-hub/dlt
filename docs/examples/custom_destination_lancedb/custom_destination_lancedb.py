"""
---
title: Custom destination with LanceDB
description: Learn how use the custom destination to load to LanceDB.
keywords: [destination, credentials, example, lancedb, custom destination, vectorstore, AI, LLM]
---

This example showcases a Python script that demonstrates the integration of LanceDB, an open-source vector database,
as a custom destination within the dlt ecosystem.
The script illustrates the implementation of a custom destination as well as the population of the LanceDB vector
store with podcast episode data from Podcast Index.
This highlights the seamless interoperability between dlt and LanceDB.

You can get a Podcast Index API key and secret from https://api.podcastindex.org/developer_home.

We'll learn how to:
- Use the [custom destination](../dlt-ecosystem/destinations/destination.md)
- Delegate the embeddings to LanceDB using OpenAI Embeddings
- Use Pydantic for unified dlt and lancedb schema validation
"""

__source_name__ = "podcastindex"

from dataclasses import dataclass, fields
import hashlib
import os
from pathlib import Path
import time
from typing import Any

import lancedb
from lancedb.embeddings import get_registry
from lancedb.pydantic import LanceModel, Vector

import dlt
from dlt.common.configuration import configspec
from dlt.common.schema import TTableSchema
from dlt.common.typing import TDataItems, TSecretStrValue
from dlt.sources.helpers.rest_client import AuthConfigBase, RESTClient

# access secrets to get openai key
openai_api_key: str = dlt.secrets.get(
    "destination.lancedb.credentials.embedding_model_provider_api_key"
)
# usually the api-key would be provided to the embedding function via the registry, but there
# currently is a bug: https://github.com/lancedb/lancedb/issues/2387
registry = get_registry()
registry.set_var("openai_api_key", openai_api_key)
# create the embedding function
func = (
    get_registry()
    .get("openai")
    .create(
        name="text-embedding-3-small",
        # api_key="$var:api_key" # << currently broken
    )
)
# so instead we provide it via environment variable
os.environ["OPENAI_API_KEY"] = openai_api_key


class EpisodeSchema(LanceModel):
    """Used for dlt and lance schema validation"""

    id: int  # noqa: A003
    title: str
    description: str = func.SourceField()
    datePublished: int
    link: str
    duration: int
    # there is more data but we are not using it ...


class EpisodeSchemaVector(EpisodeSchema):
    """Adds lance vector field"""

    vector: Vector(func.ndims()) = func.VectorField()  # type: ignore[valid-type]


@dataclass(frozen=True)
class Shows:
    latent_space: str = "6058902"
    superdatascience_podcast: str = "4299005"
    lex_fridman: str = "745287"


@configspec
class PodcastIndexAuth(AuthConfigBase):
    api_key: str = None
    api_secret: TSecretStrValue = None

    def __call__(self, request) -> Any:
        epoch_time = int(time.time())
        signature = hashlib.sha1(
            f"{self.api_key}{self.api_secret}{str(epoch_time)}".encode()
        ).hexdigest()
        headers = {
            "X-Auth-Date": str(epoch_time),
            "X-Auth-Key": self.api_key,
            "Authorization": signature,
            "User-Agent": "DltPipeline/1.0",
        }
        request.headers.update(headers)
        return request


@dlt.source
def podcast_episodes(
    api_key: str = dlt.secrets.value,
    api_secret: str = dlt.secrets.value,
):
    podcast_index_base_api_url = "https://api.podcastindex.org/api/1.0"
    client = RESTClient(
        base_url=podcast_index_base_api_url,
        auth=PodcastIndexAuth(api_key=api_key, api_secret=api_secret),
    )

    for show in fields(Shows):
        show_name = show.name
        show_id = show.default
        url = f"/episodes/byfeedid?id={show_id}"
        yield dlt.resource(
            client.get(url, params={"limit": 50}).json()["items"] or [],
            name=show_name,
            primary_key="id",
            max_table_nesting=0,
            # reuse lance model to filter out all non-matching items and extra columns from the Podcast Index API
            # 1. unknown columns are removed ("columns": "discard_value")
            # 2. non-validating items (for example missing `id` or `link`) are removed ("data_type": "discard_row")
            columns=EpisodeSchema,
            schema_contract={
                "tables": "evolve",
                "columns": "discard_value",
                "data_type": "discard_row",
            },
        ).add_filter(lambda i: i["description"] != "")


@dlt.destination(batch_size=250, name="lancedb")
def lancedb_destination(items: TDataItems, table: TTableSchema) -> None:
    db_path = Path(dlt.config.get("lancedb.db_path"))
    db = lancedb.connect(db_path)

    try:
        tbl = db.open_table(table["name"])
    except ValueError:
        tbl = db.create_table(table["name"], schema=EpisodeSchemaVector)

    tbl.add(items)


if __name__ == "__main__":
    db_path = Path(dlt.config.get("lancedb.db_path"))
    db = lancedb.connect(db_path)

    for show in fields(Shows):
        try:
            db.drop_table(show.name)
        except ValueError:
            # table is not there
            pass

    pipeline = dlt.pipeline(
        pipeline_name="podcastindex",
        destination=lancedb_destination,
        dataset_name="podcastindex_data",
        progress="log",
    )

    load_info = pipeline.run(podcast_episodes())
    print(load_info)

    row_counts = pipeline.last_trace.last_normalize_info
    print(row_counts)

    query = "French AI scientist with Lex, talking about AGI and Meta and Llama"
    table_to_query = "lex_fridman"

    tbl = db.open_table(table_to_query)

    results = tbl.search(query=query).to_list()
    assert results
