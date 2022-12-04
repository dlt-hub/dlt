import pytest
import os
import yaml
from git import Repo
from typing import Mapping, cast

from dlt.common import json
from dlt.common.typing import StrAny
from dlt.common.schema import utils
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.configuration.providers import environ as environ_provider


COMMON_TEST_CASES_PATH = "./tests/common/cases/"
# for import schema tests, change when upgrading the schema version
IMPORTED_VERSION_HASH_ETH_V5 = "y9FPYe9SXS7iE3OMnkTaJjdJN2jQKVCJxiv+z0os/Eo="

# preserve secrets path to be able to restore it
SECRET_STORAGE_PATH = environ_provider.SECRET_STORAGE_PATH


def load_json_case(name: str) -> Mapping:
    with open(json_case_path(name), "r", encoding="utf-8") as f:
        return cast(Mapping, json.load(f))


def load_yml_case(name: str) -> Mapping:
    with open(yml_case_path(name), "tr", encoding="utf-8") as f:
        return cast(Mapping, yaml.safe_load(f))


def json_case_path(name: str) -> str:
    return f"{COMMON_TEST_CASES_PATH}{name}.json"


def yml_case_path(name: str) -> str:
    return f"{COMMON_TEST_CASES_PATH}{name}.yml"


def row_to_column_schemas(row: StrAny) -> TTableSchemaColumns:
    return {k: utils.add_missing_hints({
                "name": k,
                "data_type": "text",
                "nullable": False
            }) for k in row.keys()}


@pytest.fixture(autouse=True)
def restore_secret_storage_path() -> None:
    environ_provider.SECRET_STORAGE_PATH = SECRET_STORAGE_PATH


def load_secret(name: str) -> str:
    environ_provider.SECRET_STORAGE_PATH = "./tests/common/cases/secrets/%s"
    secret, _ = environ_provider.EnvironProvider().get_value(name, environ_provider.TSecretValue)
    if not secret:
        raise FileNotFoundError(environ_provider.SECRET_STORAGE_PATH % name)
    return secret


def modify_and_commit_file(repo_path: str, file_name: str, content: str = "NEW README CONTENT") -> None:
    file_path = os.path.join(repo_path, file_name)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

    repo = Repo(repo_path)
    # one file modified
    index = repo.index.entries
    assert len(index) > 0
    assert any(e for e in index.keys() if e[0] == file_name)
    repo.index.add(file_name)
    repo.index.commit(f"mod {file_name}")

    return file_path