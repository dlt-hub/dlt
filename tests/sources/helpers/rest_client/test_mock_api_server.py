import pytest


@pytest.mark.usefixtures("mock_api_server")
class TestMockAPIServer:
    def test_paginate_by_page_number_one_based(self, rest_client):
        response = rest_client.get("/posts")
        assert response.status_code == 200
        assert response.json() == {
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
        }

        response = rest_client.get("/posts?page=2")
        assert response.status_code == 200
        assert response.json() == {
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
        }

        response = rest_client.get("/posts?page=3")
        assert response.status_code == 200
        assert response.json() == {
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
        }

        response = rest_client.get("/posts?page=4")
        assert response.status_code == 200
        assert response.json() == {
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
        }

        response = rest_client.get("/posts?page=5")
        assert response.status_code == 200
        assert response.json() == {
            "data": [
                {"id": 20, "title": "Post 20"},
                {"id": 21, "title": "Post 21"},
                {"id": 22, "title": "Post 22"},
                {"id": 23, "title": "Post 23"},
                {"id": 24, "title": "Post 24"},
            ],
            "page": 5,
            "total_pages": 5,
        }

    def test_paginate_by_page_number_zero_based(self, rest_client):
        response = rest_client.get("/posts_zero_based")
        assert response.status_code == 200

        assert response.json() == {
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
        }

        response = rest_client.get("/posts_zero_based?page=1")
        assert response.status_code == 200

        assert response.json() == {
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
        }

        response = rest_client.get("/posts_zero_based?page=2")
        assert response.status_code == 200

        assert response.json() == {
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
        }

        response = rest_client.get("/posts_zero_based?page=3")
        assert response.status_code == 200

        assert response.json() == {
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
        }

        response = rest_client.get("/posts_zero_based?page=4")
        assert response.status_code == 200

        assert response.json() == {
            "data": [
                {"id": 20, "title": "Post 20"},
                {"id": 21, "title": "Post 21"},
                {"id": 22, "title": "Post 22"},
                {"id": 23, "title": "Post 23"},
                {"id": 24, "title": "Post 24"},
            ],
            "page": 4,
            "total_pages": 5,
        }

    @pytest.mark.skip(reason="Not implemented")
    def test_paginate_by_page_number_invalid_page(self, rest_client):
        response = rest_client.get("/posts?page=6")
        assert response.status_code == 404
        assert response.json() == {"error": "Not Found"}
