from dlt.sources.rest_api import process_parent_data_item
from dlt.sources.rest_api.typing import ResolvedParam


def test_process_parent_data_item():
    path = "{token}"
    item = {"token": "1234"}
    resolved_params = [
        ResolvedParam(
            "token",
            {"field": "token", "resource": "authenticate", "type": "resolve", "location": "header"},
        ),
        ResolvedParam("token", {"field": "token", "resource": "authenticate", "type": "resolve"}),
    ]
    include_from_parent = ["token"]
    headers = {"{token}": "{token}"}

    formatted_path, formatted_headers, parent_record = process_parent_data_item(
        path, item, resolved_params, include_from_parent, headers
    )

    assert formatted_path == "1234"
    assert formatted_headers == {"1234": "1234"}
    assert parent_record == {"_authenticate_token": "1234"}
