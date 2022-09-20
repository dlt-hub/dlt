import yaml
from typing import Mapping, cast

from dlt.common import json
from dlt.common.typing import StrAny
from dlt.common.schema import utils
from dlt.common.schema.typing import TTableSchemaColumns


def load_json_case(name: str) -> Mapping:
    with open(json_case_path(name), "r", encoding="utf-8") as f:
        return cast(Mapping, json.load(f))


def load_yml_case(name: str) -> Mapping:
    with open(yml_case_path(name), "tr", encoding="utf-8") as f:
        return cast(Mapping, yaml.safe_load(f))


def json_case_path(name: str) -> str:
    return f"./tests/common/cases/{name}.json"


def yml_case_path(name: str) -> str:
    return f"./tests/common/cases/{name}.yml"


def row_to_column_schemas(row: StrAny) -> TTableSchemaColumns:
    return {k: utils.add_missing_hints({
                "name": k,
                "data_type": "text",
                "nullable": False
            }) for k in row.keys()}
