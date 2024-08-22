from typing import Any, Callable, Dict, List

import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source


def _make_pipeline(destination_name: str):
    return dlt.pipeline(
        pipeline_name="rest_api",
        destination=destination_name,
        dataset_name="rest_api_data",
        full_refresh=True,
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
                    {"filter": lambda x: x["id"] == 1},  # type: ignore[typeddict-item]
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
                        "map": exclude_columns(["title"]),  # type: ignore[typeddict-item]
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
                        "map": anonymize_columns(["title"]),  # type: ignore[typeddict-item]
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
                    {"map": lower_title},  # type: ignore[typeddict-item]
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
                    {"map": id_by_10},  # type: ignore[typeddict-item]
                    {"filter": lambda x: x["id"] == 10},  # type: ignore[typeddict-item]
                ],
            },
            {
                "name": "posts_2",
                "endpoint": "posts",
                "processing_steps": [
                    {"filter": lambda x: x["id"] == 10},  # type: ignore[typeddict-item]
                    {"map": id_by_10},  # type: ignore[typeddict-item]
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


def test_rest_api_source_filtered_child(mock_api_server) -> None:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "posts",
                "endpoint": "posts",
                "processing_steps": [
                    {"filter": lambda x: x["id"] in (1, 2)},  # type: ignore[typeddict-item]
                ],
            },
            {
                "name": "comments",
                "endpoint": {
                    "path": "/posts/{post_id}/comments",
                    "params": {
                        "post_id": {
                            "type": "resolve",
                            "resource": "posts",
                            "field": "id",
                        }
                    },
                },
                "processing_steps": [
                    {"filter": lambda x: x["id"] == 1},  # type: ignore[typeddict-item]
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("comments"))
    assert len(data) == 2


def test_rest_api_source_filtered_and_map_child(mock_api_server) -> None:
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
                    {"filter": lambda x: x["id"] in (1, 2)},  # type: ignore[typeddict-item]
                ],
            },
            {
                "name": "comments",
                "endpoint": {
                    "path": "/posts/{post_id}/comments",
                    "params": {
                        "post_id": {
                            "type": "resolve",
                            "resource": "posts",
                            "field": "id",
                        }
                    },
                },
                "include_from_parent": ["title"],
                "processing_steps": [
                    {"map": extend_body},  # type: ignore[typeddict-item]
                    {"filter": lambda x: x["body"].startswith("Post 2")},  # type: ignore[typeddict-item]
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("comments"))
    assert data[0]["body"] == "Post 2 - Comment 0 for post 2"
