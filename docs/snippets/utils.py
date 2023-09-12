import os
from typing import Dict, Any, List

from dlt.common.utils import set_working_dir

SNIPPETS_PATH = "docs/snippets"


def run_snippet(filename: str) -> Dict[str, Any]:
    """Runs a snippet of code with exec and returns local variables"""
    if not filename.endswith(".py"):
        filename = f"{filename}.py"
    with set_working_dir(os.path.dirname(os.path.join(SNIPPETS_PATH, filename))):
        with open(os.path.basename(filename), encoding="utf-8") as f:
            code = f.read()
        variables: Dict[str, Any] = {}
        exec(code, variables)
        return variables


def list_snippets(snippet_folder: str) -> List[str]:
    """List all python snippets from `snipper_folder` in SNIPPETS_PATH which are not tests"""
    print([snippet for snippet in os.listdir(os.path.join(SNIPPETS_PATH, snippet_folder))])
    return [
        snippet for snippet in os.listdir(os.path.join(SNIPPETS_PATH, snippet_folder))
            if snippet.endswith(".py") and not os.path.basename(snippet).startswith(("test_", "__"))]
