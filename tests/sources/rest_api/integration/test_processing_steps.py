import copy
from typing import Any, Callable, Dict, List

import pytest

import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source
from ..conftest import (
    DEFAULT_COMMENTS_COUNT,
    DEFAULT_PAGE_SIZE,
    DEFAULT_TOTAL_PAGES,
    DEFAULT_REACTIONS_COUNT,
)


def _make_pipeline(destination_name: str):
    return dlt.pipeline(
        pipeline_name="rest_api",
        destination=destination_name,
        dataset_name="rest_api_data",
        dev_mode=True,
    )


def test_rest_api_source_filtered(mock_api_server) -> None:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "posts",
                "endpoint": "posts",
                "processing_steps": [
                    {"filter": lambda x: x["id"] == 1},
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("posts"))
    assert len(data) == 1
    assert data[0]["title"] == "Post 1"


def test_rest_api_source_exclude_columns(mock_api_server) -> None:
    def exclude_columns(columns: List[str]) -> Callable[..., Any]:
        def pop_columns(record: Dict[str, Any]) -> Dict[str, Any]:
            for col in columns:
                record.pop(col)
            return record

        return pop_columns

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "posts",
                "endpoint": "posts",
                "processing_steps": [
                    {
                        "map": exclude_columns(["title"]),
                    },
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("posts"))

    assert all("title" not in record for record in data)


def test_rest_api_source_anonymize_columns(mock_api_server) -> None:
    def anonymize_columns(columns: List[str]) -> Callable[..., Any]:
        def empty_columns(record: Dict[str, Any]) -> Dict[str, Any]:
            for col in columns:
                record[col] = "dummy"
            return record

        return empty_columns

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "posts",
                "endpoint": "posts",
                "processing_steps": [
                    {
                        "map": anonymize_columns(["title"]),
                    },
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("posts"))

    assert all(record["title"] == "dummy" for record in data)


def test_rest_api_source_map(mock_api_server) -> None:
    def lower_title(row):
        row["title"] = row["title"].lower()
        return row

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "posts",
                "endpoint": "posts",
                "processing_steps": [
                    {"map": lower_title},
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("posts"))

    assert all(record["title"].startswith("post ") for record in data)


def test_rest_api_source_filter_and_map(mock_api_server) -> None:
    def id_by_10(row):
        row["id"] = row["id"] * 10
        return row

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "posts",
                "endpoint": "posts",
                "processing_steps": [
                    {"map": id_by_10},
                    {"filter": lambda x: x["id"] == 10},
                ],
            },
            {
                "name": "posts_2",
                "endpoint": "posts",
                "processing_steps": [
                    {"filter": lambda x: x["id"] == 10},
                    {"map": id_by_10},
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("posts"))
    assert len(data) == 1
    assert data[0]["title"] == "Post 1"

    data = list(mock_source.with_resources("posts_2"))
    assert len(data) == 1
    assert data[0]["id"] == 100
    assert data[0]["title"] == "Post 10"


@pytest.mark.parametrize(
    "comments_endpoint",
    [
        {
            "path": "/posts/{post_id}/comments",
            "params": {
                "post_id": {
                    "type": "resolve",
                    "resource": "posts",
                    "field": "id",
                }
            },
        },
        "posts/{resources.posts.id}/comments",
    ],
)
def test_rest_api_source_filtered_child(mock_api_server, comments_endpoint) -> None:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "posts",
                "endpoint": "posts",
                "processing_steps": [
                    {"filter": lambda x: x["id"] in (1, 2)},
                ],
            },
            {
                "name": "comments",
                "endpoint": comments_endpoint,
                "processing_steps": [
                    {"filter": lambda x: x["id"] == 1},
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("comments"))
    assert len(data) == 2


@pytest.mark.parametrize(
    "comments_endpoint",
    [
        {
            "path": "/posts/{post_id}/comments",
            "params": {
                "post_id": {
                    "type": "resolve",
                    "resource": "posts",
                    "field": "id",
                }
            },
        },
        "posts/{resources.posts.id}/comments",
    ],
)
def test_rest_api_source_filtered_and_map_child(mock_api_server, comments_endpoint) -> None:
    def extend_body(row):
        row["body"] = f"{row['_posts_title']} - {row['body']}"
        return row

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "posts",
                "endpoint": "posts",
                "processing_steps": [
                    {"filter": lambda x: x["id"] in (1, 2)},
                ],
            },
            {
                "name": "comments",
                "endpoint": comments_endpoint,
                "include_from_parent": ["title"],
                "processing_steps": [
                    {"map": extend_body},
                    {"filter": lambda x: x["body"].startswith("Post 2")},
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("comments"))
    assert data[0]["body"] == "Post 2 - Comment 0 for post 2"


def flatten_reactions(post):
    post_without_reactions = copy.deepcopy(post)
    post_without_reactions.pop("reactions")
    for reaction in post["reactions"]:
        yield {"reaction": reaction, **post_without_reactions}


def test_rest_api_source_yield_map(mock_api_server) -> None:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "posts",
                "endpoint": "posts_with_reactions",
                "processing_steps": [
                    {"yield_map": flatten_reactions},
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("posts"))

    assert len(data) == DEFAULT_PAGE_SIZE * DEFAULT_TOTAL_PAGES * DEFAULT_REACTIONS_COUNT
    assert all("reaction" in record and "reactions" not in record for record in data)
    assert all(
        record["reaction"]["title"]
        == f"Reaction {record['reaction']['id']} for post {record['id']}"
        for record in data
    )


def test_rest_api_source_filter_then_yield_map(mock_api_server) -> None:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "posts",
                "endpoint": "posts_with_reactions",
                "processing_steps": [
                    {"filter": lambda x: x["id"] != 1},
                    {"yield_map": flatten_reactions},
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("posts"))

    assert len(data) == (DEFAULT_PAGE_SIZE * DEFAULT_TOTAL_PAGES - 1) * DEFAULT_REACTIONS_COUNT
    assert all(record["id"] != 1 for record in data)


def test_rest_api_source_yield_map_then_filter_reactions(mock_api_server) -> None:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "posts",
                "endpoint": "posts_with_reactions",
                "processing_steps": [
                    {"yield_map": flatten_reactions},
                    {"filter": lambda x: x["reaction"]["id"] != 0},
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("posts"))

    assert len(data) == DEFAULT_PAGE_SIZE * DEFAULT_TOTAL_PAGES * (DEFAULT_REACTIONS_COUNT - 1)
    assert all(record["reaction"]["id"] != 0 for record in data)


@pytest.mark.parametrize(
    "comments_endpoint",
    [
        {
            "path": "/posts/{post_id}/comments",
            "params": {
                "post_id": {
                    "type": "resolve",
                    "resource": "posts",
                    "field": "id",
                }
            },
        },
        "posts/{resources.posts.id}/comments",
    ],
)
def test_rest_api_source_yield_map_child(mock_api_server, comments_endpoint) -> None:
    def extend_body(row):
        row["body"] = f"{row['_posts_title']} - {row['body']}"
        return row

    def flatten_comment_reactions(comment_enriched):
        comment_without_reactions = copy.deepcopy(comment_enriched)
        comment_without_reactions.pop("_posts_reactions")
        for reaction in comment_enriched["_posts_reactions"]:
            yield {"_posts_reaction": reaction, **comment_without_reactions}

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "posts",
                "endpoint": "posts_with_reactions",
                "processing_steps": [
                    {"filter": lambda x: x["id"] in (1, 2)},
                ],
            },
            {
                "name": "comments",
                "endpoint": comments_endpoint,
                "include_from_parent": ["title", "reactions"],
                "processing_steps": [
                    {"map": extend_body},
                    {"filter": lambda x: x["body"].startswith("Post 2")},
                    {"yield_map": flatten_comment_reactions},
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("comments"))

    assert len(data) == DEFAULT_COMMENTS_COUNT * DEFAULT_REACTIONS_COUNT
    assert data[0]["body"] == "Post 2 - Comment 0 for post 2"
    assert data[0]["_posts_reaction"]["title"] == "Reaction 0 for post 2"
