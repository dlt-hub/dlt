from typing import Mapping, cast

from dlt.common import json


def load_json_case(name: str) -> Mapping:
    with open(json_case_path(name), "tr") as f:
        return cast(Mapping, json.load(f))


def json_case_path(name: str) -> str:
    return f"./tests/unpacker/cases/{name}.json"