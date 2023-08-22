from typing import Dict, Any

BASEPATH = "docs/snippets"

def run_snippet(filename: str) -> Dict[str, Any]:
    variables: Dict[str, Any] = {}
    exec(open(f"{BASEPATH}/{filename}.py", encoding="utf-8").read(), variables)
    return variables