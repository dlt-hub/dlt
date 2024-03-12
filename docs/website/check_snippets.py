"""
Load all snippets in snippet folder, check wether they parse with ast and run them
"""
import typing as t
import os
import ast
import sys

SNIPPET_DIR = "../snippets"

def get_snippet_list() -> t.List[str]:
    """Get list of available snippets in the snippet folder."""
    return [s.replace(".py", "") for s in os.listdir(SNIPPET_DIR) if s.endswith(".py") and s != "__init__.py"]

def get_snippet(snippet_name: str) -> str:
    """Get the content of a snippet."""
    with open(os.path.join(SNIPPET_DIR, snippet_name + ".py"), "r") as f:
        return f.read()
    
def parse_snippet(snippet: str) -> bool:
    """Parse a snippet with ast."""
    try:
        ast.parse(snippet)
        print("\033[92m  -> Parse ok \033[0m")
        return True
    except:
        print("\033[91m  -> Failed to parse snippet, skipping run \033[0m")
        return False

def run_snippet(snippet: str) -> bool:
    """Run a snippet."""
    try:
        with open(os.devnull, "w") as devnull:
            old_stdout = sys.stdout
            sys.stdout = devnull
            exec(snippet,  {"__name__": "__main__"})
            sys.stdout = old_stdout
        print("\033[92m  -> Run ok \033[0m")
        return True
    except:
        sys.stdout = old_stdout
        print("\033[91m  -> Failed to run snippet\033[0m")
        return False


if __name__ == "__main__":
    
    print("Checking snippets")
    snippet_list = get_snippet_list()
    failed_parsing = []
    failed_running = []
    print(f"Found {len(snippet_list)} snippets")

    # parse and run all snippets
    for s in snippet_list:
        print(f"Checking snippet {s}")

        snippet = get_snippet(s)
        if parse_snippet(snippet) is False:
            failed_parsing.append(snippet)
            continue

        # snippet needs to be run in main function for some reason
        if run_snippet(snippet) is False:
            failed_running.append(snippet)

    print()
    if failed_parsing or failed_running:
        print(f"\033[91m{len(failed_parsing)} snippets failed to parse, {len(failed_running)} snippets failed to run")
        exit(1)
    else:
        print("\033[92mAll snippets ok")
        exit(0)


