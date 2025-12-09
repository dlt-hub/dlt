import json
import re
import shlex
import subprocess
from pathlib import Path

EDUCATION_NOTEBOOKS_DIR = Path(__file__).parent.parent.parent / "education"
TEMP_IPYNB_FILE_PREIFX = "tmp"

MUST_INSTALL_PACKAGES = {"numpy", "pandas", "sqlalchemy"}


def replace_colab_imports_in_notebook(notebook_dict: dict) -> dict:
    """
    Remove Google Colab-specific imports and replace Colab API calls with standard Python.

    Google Colab provides special APIs like `google.colab.userdata` for accessing secrets
    that don't exist outside the Colab environment. This function:
    - Removes: `from google.colab import userdata` (and similar imports)
    - Replaces: `userdata.get(...)` → `os.getenv(...)`

    Args:
        notebook_dict: Notebook as a Python dictionary

    Returns:
        Modified notebook dictionary
    """
    for cell in notebook_dict.get("cells", []):
        if cell.get("cell_type") == "code":
            source = cell.get("source", [])
            if isinstance(source, list):
                # Remove lines with Google Colab imports
                source = [
                    line
                    for line in source
                    if not re.match(r"^\s*from google\.colab import", line)
                ]
                # Replace userdata.get with os.getenv
                source = [
                    line.replace("userdata.get(", "os.getenv(") for line in source
                ]
                cell["source"] = source

    return notebook_dict


def process_shell_commands_in_notebook(notebook_dict: dict) -> tuple[dict, set[str]]:
    """
    Convert Jupyter shell commands to Python subprocess calls and extract dependencies.

    Jupyter/Colab notebooks support shell commands with `!` syntax (e.g., `!pip install dlt`),
    but this is IPython-specific magic syntax that doesn't work in standard Python or Marimo.
    This function:
    - Extracts package names from `!pip install` commands for dependency tracking
    - Converts other `!command` shell commands to `subprocess.run()` calls
    - Removes notebook-specific magic commands (e.g., `%%capture`)

    Args:
        notebook_dict: Notebook as a Python dictionary

    Returns:
        Tuple of (modified notebook dict, set of package names extracted from pip install commands)
    """
    packages: set[str] = set()
    subprocess_imported: bool = False

    for cell in notebook_dict.get("cells", []):
        if cell.get("cell_type") == "code":
            cell_code = cell.get("source", [])
            new_cell_code = []

            for line in cell_code:
                stripped = line.strip()

                # skip magic commands
                if stripped.startswith("%%capture"):
                    continue

                # extract packages from pip install
                if stripped.startswith("!pip install"):
                    match = re.search(r"!pip install\s+(.+?)(?:\n|$)", stripped)
                    if match:
                        cleaned = (
                            match.group(1).strip().replace('"', "").replace("'", "")
                        )
                        # Remove spaces around commas in brackets
                        cleaned = re.sub(r"\[\s*", "[", cleaned)  # Remove space after [
                        cleaned = re.sub(
                            r"\s*\]", "]", cleaned
                        )  # Remove space before ]
                        cleaned = re.sub(
                            r",\s+", ",", cleaned
                        )  # Remove space after commas

                        pkgs = [
                            p.strip()
                            for p in cleaned.split()
                            if p.strip() and not p.startswith("-")
                        ]  # Filter flags
                        packages.update(pkgs)
                    continue

                # convert other shell commands
                elif stripped.startswith("!"):
                    if not subprocess_imported:
                        new_cell_code.append("import subprocess\n")
                        subprocess_imported = True
                    cmd = stripped[1:]
                    new_line = _build_subprocess_line(cmd) + "\n"
                    new_cell_code.append(new_line)

                else:
                    new_cell_code.append(line)

            cell["source"] = new_cell_code

    return notebook_dict, packages


def add_inline_dependencies_to_content(packages: set[str], py_content: str) -> str:
    """
    Add PEP 723 inline script metadata block with dependencies.

    Marimo/Molab can automatically install packages when they're declared using PEP 723
    inline script metadata. The dependency list includes:
    - Packages extracted from !pip install commands in the original notebook
    - MUST_INSTALL_PACKAGES (core dependencies required for all notebooks)

    Args:
        packages: Set of package names to include (will be merged with MUST_INSTALL_PACKAGES)
        py_content: The Python file content as a string

    Returns:
        Python content with PEP 723 metadata block prepended

    NOTE: Without this, users would need to go through a step of manually installing packages before running
    the notebook (Marimo will try to install missing imports, which is not exactly nice for a smooth experience.
    Also, some libraries used under the hood are not directly imported and are not caught by Marimo).

    Format:
        # /// script
        # dependencies = [
        #     "package1",
        #     "package2",
        # ]
        # ///
    """
    packages = packages.copy()  # Don't mutate the input set
    packages.update(MUST_INSTALL_PACKAGES)
    if not packages:
        return py_content

    pkg_lines = "\n".join(f'#     "{pkg}",' for pkg in sorted(packages))
    deps_block = f"""# /// script
# dependencies = [
{pkg_lines}
# ]
# ///

"""

    return deps_block + py_content


def read_notebook(ipynb_path: Path) -> dict:
    """
    Read a Jupyter notebook file and return as a dictionary.

    Args:
        ipynb_path: Path to the .ipynb file

    Returns:
        Notebook data as a Python dictionary
    """
    return json.loads(ipynb_path.read_text(encoding="utf-8"))


def write_notebook(notebook_dict: dict, output_path: Path) -> None:
    """
    Write a notebook dictionary to a file.

    Args:
        notebook_dict: Notebook data as a Python dictionary
        output_path: Path where the notebook should be written
    """
    output_path.write_text(
        json.dumps(notebook_dict, indent=1, ensure_ascii=False), encoding="utf-8"
    )


def convert_notebook_to_marimo(temp_ipynb_path: Path) -> str:
    """
    Convert a Jupyter notebook to Marimo Python format using marimo CLI.

    Args:
        temp_ipynb_path: Path to the temporary preprocessed notebook

    Returns:
        Marimo Python file content as a string
    """
    result = subprocess.run(
        ["marimo", "convert", str(temp_ipynb_path)],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def write_python_file(content: str, output_path: Path) -> None:
    """
    Write Python content to a file.

    Args:
        content: Python file content as a string
        output_path: Path where the file should be written
    """
    output_path.write_text(content, encoding="utf-8")


def _build_subprocess_line(cmd: str) -> str:
    """
    Generate a subprocess.run() call string from a shell command.

    This helper converts various shell command patterns to their Python subprocess
    equivalents, handling special cases like piped input.

    Conversion rules:
    - Simple commands: `command arg` → `subprocess.run(['command', 'arg'], check=True)`
    - Yes piping: `yes | command` → `subprocess.run(['command'], input='y\\n', ...)`
    - No piping: `no | command` → `subprocess.run(['command'], input='n\\n', ...)`
    - Complex pipes: `cmd1 | cmd2` → `subprocess.run('cmd1 | cmd2', shell=True, ...)`

    Args:
        cmd: The shell command string (without the leading `!`)

    Returns:
        A string containing Python code for subprocess.run()
    """
    cmd = cmd.strip()

    # No pipe → simple list argv
    if "|" not in cmd:
        argv = shlex.split(cmd)
        return f"subprocess.run({argv!r}, check=True)"

    # Split pipe
    left, right = map(str.strip, cmd.split("|", 1))
    left_lower = left.lower()

    # yes | command  → feed "y\n"
    if left_lower == "yes":
        argv = shlex.split(right)
        return f"subprocess.run({argv!r}, input='y\\n', text=True, check=True)"

    # no | command  → feed "n\n"
    if left_lower == "no":
        argv = shlex.split(right)
        return f"subprocess.run({argv!r}, input='n\\n', text=True, check=True)"

    # generic pipe: shell=True fallback
    return f"subprocess.run({cmd!r}, shell=True, check=True)"


if __name__ == "__main__":
    for ipynb_file in EDUCATION_NOTEBOOKS_DIR.glob("*/*.ipynb"):
        # 1. Read notebook file
        notebook_dict = read_notebook(ipynb_file)
        # 2. Replace Colab imports
        notebook_dict = replace_colab_imports_in_notebook(notebook_dict)
        # 3. Process shell commands
        notebook_dict, packages = process_shell_commands_in_notebook(notebook_dict)
        # 4. Write temporary notebook
        temp_ipynb_file = ipynb_file.with_name(
            f"{TEMP_IPYNB_FILE_PREIFX}_{ipynb_file.name}"
        )
        write_notebook(notebook_dict, temp_ipynb_file)
        # 5. Convert to Marimo format
        py_content = convert_notebook_to_marimo(temp_ipynb_file)
        # 6. Add inline dependencies
        py_content_with_deps = add_inline_dependencies_to_content(packages, py_content)
        # 7. Write final Python file
        output_path = ipynb_file.with_suffix(".py")
        write_python_file(py_content_with_deps, output_path)
        # 8. Clean up temporary files
        temp_ipynb_file.unlink()
