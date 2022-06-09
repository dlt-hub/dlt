import os
from git import Repo
import pytest
from prometheus_client import CollectorRegistry

from dlt.common.configuration import utils
from dlt.common.typing import StrAny
from dlt.dbt_runner.configuration import gen_configuration_variant
from dlt.dbt_runner import runner

from tests.utils import clean_storage


SECRET_STORAGE_PATH = utils.SECRET_STORAGE_PATH


@pytest.fixture(autouse=True)
def restore_secret_storage_path() -> None:
    utils.SECRET_STORAGE_PATH = SECRET_STORAGE_PATH


def load_secret(name: str) -> str:
    utils.SECRET_STORAGE_PATH = "./tests/dbt_runner/secrets/%s"
    secret = utils._get_key_value(name, utils.TConfigSecret)
    if not secret:
        raise FileNotFoundError(utils.SECRET_STORAGE_PATH % name)
    return secret


def modify_and_commit_file(repo_path: str, file_name: str, content: str = "NEW README CONTENT") -> None:
    file_path = os.path.join(repo_path, file_name)

    with open(file_path, "w") as f:
        f.write(content)

    repo = Repo(repo_path)
    # one file modified
    index = repo.index.entries
    assert len(index) > 0
    assert any(e for e in index.keys() if e[0] == file_name)
    repo.index.add(file_name)
    repo.index.commit(f"mod {file_name}")

    return file_path


def setup_runner(dest_schema_prefix: str, override_values: StrAny = None) -> None:
    clean_storage()
    C = gen_configuration_variant(initial_values=override_values)
    # set unique dest schema prefix by default
    C.DEST_SCHEMA_PREFIX = dest_schema_prefix
    C.PACKAGE_RUN_PARAMS = ["--fail-fast", "--full-refresh"]
    # override values including the defaults above
    if override_values:
        for k,v in override_values.items():
            setattr(C, k, v)
    runner.configure(C, CollectorRegistry(auto_describe=True))
