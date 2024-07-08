import pytest


@pytest.mark.usefixtures("mock_api_server")
class TestMockAPIServer:
    @pytest.mark.parametrize(
        "test_case",
        [
            # Page number is one-based
            {
                "url": "/posts",
                "status_code": 200,
                "expected_json": {
                    "data": [
                        {"id": 0, "title": "Post 0"},
                        {"id": 1, "title": "Post 1"},
                        {"id": 2, "title": "Post 2"},
                        {"id": 3, "title": "Post 3"},
                        {"id": 4, "title": "Post 4"},
                    ],
                    "page": 1,
                    "total_pages": 5,
                    "next_page": "https://api.example.com/posts?page=2",
                },
            },
            {
                "url": "/posts?page=2",
                "status_code": 200,
                "expected_json": {
                    "data": [
                        {"id": 5, "title": "Post 5"},
                        {"id": 6, "title": "Post 6"},
                        {"id": 7, "title": "Post 7"},
                        {"id": 8, "title": "Post 8"},
                        {"id": 9, "title": "Post 9"},
                    ],
                    "page": 2,
                    "total_pages": 5,
                    "next_page": "https://api.example.com/posts?page=3",
                },
            },
            {
                "url": "/posts?page=3",
                "status_code": 200,
                "expected_json": {
                    "data": [
                        {"id": 10, "title": "Post 10"},
                        {"id": 11, "title": "Post 11"},
                        {"id": 12, "title": "Post 12"},
                        {"id": 13, "title": "Post 13"},
                        {"id": 14, "title": "Post 14"},
                    ],
                    "page": 3,
                    "total_pages": 5,
                    "next_page": "https://api.example.com/posts?page=4",
                },
            },
            {
                "url": "/posts?page=4",
                "status_code": 200,
                "expected_json": {
                    "data": [
                        {"id": 15, "title": "Post 15"},
                        {"id": 16, "title": "Post 16"},
                        {"id": 17, "title": "Post 17"},
                        {"id": 18, "title": "Post 18"},
                        {"id": 19, "title": "Post 19"},
                    ],
                    "page": 4,
                    "total_pages": 5,
                    "next_page": "https://api.example.com/posts?page=5",
                },
            },
            {
                "url": "/posts?page=5",
                "status_code": 200,
                "expected_json": {
                    "data": [
                        {"id": 20, "title": "Post 20"},
                        {"id": 21, "title": "Post 21"},
                        {"id": 22, "title": "Post 22"},
                        {"id": 23, "title": "Post 23"},
                        {"id": 24, "title": "Post 24"},
                    ],
                    "page": 5,
                    "total_pages": 5,
                },
            },
            # Page number is zero-based
            {
                "url": "/posts_zero_based",
                "status_code": 200,
                "expected_json": {
                    "data": [
                        {"id": 0, "title": "Post 0"},
                        {"id": 1, "title": "Post 1"},
                        {"id": 2, "title": "Post 2"},
                        {"id": 3, "title": "Post 3"},
                        {"id": 4, "title": "Post 4"},
                    ],
                    "page": 0,
                    "total_pages": 5,
                    "next_page": "https://api.example.com/posts_zero_based?page=1",
                },
            },
            {
                "url": "/posts_zero_based?page=1",
                "status_code": 200,
                "expected_json": {
                    "data": [
                        {"id": 5, "title": "Post 5"},
                        {"id": 6, "title": "Post 6"},
                        {"id": 7, "title": "Post 7"},
                        {"id": 8, "title": "Post 8"},
                        {"id": 9, "title": "Post 9"},
                    ],
                    "page": 1,
                    "total_pages": 5,
                    "next_page": "https://api.example.com/posts_zero_based?page=2",
                },
            },
            {
                "url": "/posts_zero_based?page=2",
                "status_code": 200,
                "expected_json": {
                    "data": [
                        {"id": 10, "title": "Post 10"},
                        {"id": 11, "title": "Post 11"},
                        {"id": 12, "title": "Post 12"},
                        {"id": 13, "title": "Post 13"},
                        {"id": 14, "title": "Post 14"},
                    ],
                    "page": 2,
                    "total_pages": 5,
                    "next_page": "https://api.example.com/posts_zero_based?page=3",
                },
            },
            {
                "url": "/posts_zero_based?page=3",
                "status_code": 200,
                "expected_json": {
                    "data": [
                        {"id": 15, "title": "Post 15"},
                        {"id": 16, "title": "Post 16"},
                        {"id": 17, "title": "Post 17"},
                        {"id": 18, "title": "Post 18"},
                        {"id": 19, "title": "Post 19"},
                    ],
                    "page": 3,
                    "total_pages": 5,
                    "next_page": "https://api.example.com/posts_zero_based?page=4",
                },
            },
            {
                "url": "/posts_zero_based?page=4",
                "status_code": 200,
                "expected_json": {
                    "data": [
                        {"id": 20, "title": "Post 20"},
                        {"id": 21, "title": "Post 21"},
                        {"id": 22, "title": "Post 22"},
                        {"id": 23, "title": "Post 23"},
                        {"id": 24, "title": "Post 24"},
                    ],
                    "page": 4,
                    "total_pages": 5,
                },
            },
            # Test offset-limit pagination
            {
                "url": "/posts_offset_limit",
                "status_code": 200,
                "expected_json": {
                    "data": [
                        {"id": 0, "title": "Post 0"},
                        {"id": 1, "title": "Post 1"},
                        {"id": 2, "title": "Post 2"},
                        {"id": 3, "title": "Post 3"},
                        {"id": 4, "title": "Post 4"},
                        {"id": 5, "title": "Post 5"},
                        {"id": 6, "title": "Post 6"},
                        {"id": 7, "title": "Post 7"},
                        {"id": 8, "title": "Post 8"},
                        {"id": 9, "title": "Post 9"},
                    ],
                    "total_records": 25,
                    "offset": 0,
                    "limit": 10,
                },
            },
            {
                "url": "/posts_offset_limit?offset=10&limit=10",
                "status_code": 200,
                "expected_json": {
                    "data": [
                        {"id": 10, "title": "Post 10"},
                        {"id": 11, "title": "Post 11"},
                        {"id": 12, "title": "Post 12"},
                        {"id": 13, "title": "Post 13"},
                        {"id": 14, "title": "Post 14"},
                        {"id": 15, "title": "Post 15"},
                        {"id": 16, "title": "Post 16"},
                        {"id": 17, "title": "Post 17"},
                        {"id": 18, "title": "Post 18"},
                        {"id": 19, "title": "Post 19"},
                    ],
                    "total_records": 25,
                    "offset": 10,
                    "limit": 10,
                },
            },
            {
                "url": "/posts_offset_limit?offset=20&limit=10",
                "status_code": 200,
                "expected_json": {
                    "data": [
                        {"id": 20, "title": "Post 20"},
                        {"id": 21, "title": "Post 21"},
                        {"id": 22, "title": "Post 22"},
                        {"id": 23, "title": "Post 23"},
                        {"id": 24, "title": "Post 24"},
                    ],
                    "total_records": 25,
                    "offset": 20,
                    "limit": 10,
                },
            },
            # Test cursor pagination
            {
                "url": "/posts_cursor",
                "status_code": 200,
                "expected_json": {
                    "data": [
                        {"id": 0, "title": "Post 0"},
                        {"id": 1, "title": "Post 1"},
                        {"id": 2, "title": "Post 2"},
                        {"id": 3, "title": "Post 3"},
                        {"id": 4, "title": "Post 4"},
                    ],
                    "next_cursor": 5,
                },
            },
            {
                "url": "/posts_cursor?cursor=5",
                "status_code": 200,
                "expected_json": {
                    "data": [
                        {"id": 5, "title": "Post 5"},
                        {"id": 6, "title": "Post 6"},
                        {"id": 7, "title": "Post 7"},
                        {"id": 8, "title": "Post 8"},
                        {"id": 9, "title": "Post 9"},
                    ],
                    "next_cursor": 10,
                },
            },
            {
                "url": "/posts_cursor?cursor=10",
                "status_code": 200,
                "expected_json": {
                    "data": [
                        {"id": 10, "title": "Post 10"},
                        {"id": 11, "title": "Post 11"},
                        {"id": 12, "title": "Post 12"},
                        {"id": 13, "title": "Post 13"},
                        {"id": 14, "title": "Post 14"},
                    ],
                    "next_cursor": 15,
                },
            },
            {
                "url": "/posts_cursor?cursor=15",
                "status_code": 200,
                "expected_json": {
                    "data": [
                        {"id": 15, "title": "Post 15"},
                        {"id": 16, "title": "Post 16"},
                        {"id": 17, "title": "Post 17"},
                        {"id": 18, "title": "Post 18"},
                        {"id": 19, "title": "Post 19"},
                    ],
                    "next_cursor": 20,
                },
            },
            {
                "url": "/posts_cursor?cursor=20",
                "status_code": 200,
                "expected_json": {
                    "data": [
                        {"id": 20, "title": "Post 20"},
                        {"id": 21, "title": "Post 21"},
                        {"id": 22, "title": "Post 22"},
                        {"id": 23, "title": "Post 23"},
                        {"id": 24, "title": "Post 24"},
                    ],
                    "next_cursor": None,
                },
            },
        ],
    )
    def test_paginate_success(self, test_case, rest_client):
        response = rest_client.get(test_case["url"])
        assert response.status_code == test_case["status_code"]
        assert response.json() == test_case["expected_json"]

    @pytest.mark.skip(reason="Not implemented")
    def test_paginate_by_page_number_invalid_page(self, rest_client):
        response = rest_client.get("/posts?page=6")
        assert response.status_code == 404
        assert response.json() == {"error": "Not Found"}
