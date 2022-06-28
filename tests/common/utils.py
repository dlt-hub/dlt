import yaml
from typing import Mapping, cast

from dlt.common import json


def load_json_case(name: str) -> Mapping:
    with open(json_case_path(name), "r", encoding="utf-8") as f:
        return cast(Mapping, json.load(f))


def load_yml_case(name: str) -> Mapping:
    with open(f"./tests/common/cases/{name}.yml", "tr", encoding="utf-8") as f:
        return cast(Mapping, yaml.safe_load(f))


def json_case_path(name: str) -> str:
    return f"./tests/common/cases/{name}.json"
