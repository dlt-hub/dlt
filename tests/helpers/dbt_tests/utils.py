import os
from typing import List, Sequence

from dlt.common.storages import FileStorage
from dlt.common.git import clone_repo
from dlt.helpers.dbt.exceptions import DBTNodeResult

JAFFLE_SHOP_REPO = "https://github.com/dbt-labs/jaffle_shop.git"
TEST_CASES_PATH = "./tests/helpers/dbt_tests/cases/"


def load_test_case(case: str) -> str:
    with open(os.path.join(TEST_CASES_PATH, case), "r", encoding="utf-8") as f:
        return f.read()


def find_run_result(results: Sequence[DBTNodeResult], model_name: str) -> DBTNodeResult:
    return next((r for r in results if r.model_name == model_name), None)


def clone_jaffle_repo(test_storage: FileStorage) -> str:
    repo_path = test_storage.make_full_path("jaffle_shop")
    # clone jaffle shop for dbt 1.0.0
    clone_repo(JAFFLE_SHOP_REPO, repo_path, with_git_command=None, branch="main").close()  # core-v1.0.0
    return repo_path


def assert_jaffle_completed(test_storage: FileStorage, results: List[DBTNodeResult], jaffle_dir: str = "jaffle/jaffle_shop") -> None:
    assert len(results) == 5
    assert all(r.status == "success" for r in results)
    assert results[0].message == "CREATE VIEW"
    customers = find_run_result(results, "customers")
    assert customers.message == "SELECT 100"
    # `run_dbt` has injected credentials into environ. make sure that credentials were removed
    assert "CREDENTIALS__PASSWORD" not in os.environ
    # make sure jaffle_shop was cloned into right dir
    assert test_storage.has_folder(jaffle_dir)
    assert test_storage.has_file(f"{jaffle_dir}/README.md")
    assert test_storage.has_file(f"{jaffle_dir}/target/run_results.json")
