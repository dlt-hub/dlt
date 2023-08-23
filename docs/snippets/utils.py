from typing import Dict, Any

from dlt.common.utils import set_working_dir

from tests.utils import TEST_STORAGE_ROOT, test_storage

BASEPATH = "docs/snippets"

def run_snippet(filename: str) -> Dict[str, Any]:
    with set_working_dir(BASEPATH):
        code = open(f"{filename}.py", encoding="utf-8").read()
        variables: Dict[str, Any] = {}
        exec(code, variables)
        return variables