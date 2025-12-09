import json
import re
import shlex
import subprocess
from pathlib import Path

EDUCATION_NOTEBOOKS_DIR = Path(__file__).parent.parent.parent / "education"
TEMP_IPYNB_FILE_PREIFX = "tmp"

MUST_INSTALL_PACKAGES = {"numpy", "pandas", "sqlalchemy"}


def convert_ipynb_to_py(original_ipynb: Path, cleaned_ipynb: Path) -> Path:
    """
    Convert a preprocessed Jupyter notebook to a Marimo Python file.

    This function uses Marimo's built-in converter on the cleaned notebook but preserves
    the original filename. This is necessary because the cleaned notebook is a temporary
    file with a prefixed name, but we want the output .py file to match the original
    notebook name for consistency with the repository structure.

    Args:
        original_ipynb: Path to the original notebook (used only for output naming)
        cleaned_ipynb: Path to the preprocessed temporary notebook (used for conversion)

    Returns:
        Path to the generated Marimo .py file (named after original_ipynb)
    """
    output_path = original_ipynb.with_suffix(".py")
    result = subprocess.run(
        ["marimo", "convert", str(cleaned_ipynb)],
        capture_output=True,
        text=True,
        check=True,
    )
    output_path.write_text(result.stdout, encoding="utf-8")
    return output_path


def add_inline_dependencies(packages: set[str], py_file: Path) -> None:
    """
    Add PEP 723 inline script metadata block with dependencies to a Marimo Python file.

    Marimo/Molab can automatically install packages when they're declared using PEP 723
    inline script metadata. This eliminates the need for manual pip install commands and
    ensures the notebook is self-contained and immediately runnable.

    The dependency list includes:
    - Packages extracted from !pip install commands in the original notebook
    - MUST_INSTALL_PACKAGES (core dependencies required for all notebooks)

    Args:
        packages: Set of package names to include (will be merged with MUST_INSTALL_PACKAGES)
        py_file: Path to the Marimo .py file to prepend metadata to

    Why: Without this, users would need to go through a step of manually installing packages before running
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
    packages.update(MUST_INSTALL_PACKAGES)
    if not packages:
        return

    content = py_file.read_text(encoding="utf-8")

    pkg_lines = "\n".join(f'#     "{pkg}",' for pkg in sorted(packages))
    deps_block = f"""# /// script
# dependencies = [
{pkg_lines}
# ]
# ///

"""

    py_file.write_text(deps_block + content, encoding="utf-8")


def replace_google_colab_imports(ipynb_file: Path) -> Path:
    """
    Remove Google Colab-specific imports and replace Colab API calls with standard Python.

    Google Colab provides special APIs like `google.colab.userdata` for accessing secrets
    that don't exist outside the Colab environment. This function:
    - Removes: `from google.colab import userdata` (and similar imports)
    - Replaces: `userdata.get(...)` → `os.getenv(...)`

    This makes notebooks compatible with any Python environment while maintaining the
    same interface for accessing environment variables/secrets.

    Args:
        ipynb_file: Path to the original Jupyter notebook

    Returns:
        Path to a temporary notebook file with Colab imports replaced

    Note: The original file is NOT modified; a temporary copy with prefix is created.
    """
    notebook = json.loads(Path(ipynb_file).read_text())

    for cell in notebook.get("cells", []):
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

    new_file = ipynb_file.with_name(f"{TEMP_IPYNB_FILE_PREIFX}_{ipynb_file.name}")
    with open(new_file, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=1, ensure_ascii=False)

    return new_file


def process_shell_commands(ipynb_file: Path) -> set[str]:
    """
    Convert Jupyter shell commands to Python subprocess calls and extract dependencies.

    Jupyter/Colab notebooks support shell commands with `!` syntax (e.g., `!pip install dlt`),
    but this is IPython-specific magic syntax that doesn't work in standard Python or Marimo.

    This function:
    1. Extracts package names from `!pip install` commands for dependency tracking
    2. Converts other `!command` shell commands to `subprocess.run()` calls
    3. Removes notebook-specific magic commands (e.g., `%%capture`)

    Args:
        ipynb_file: Path to the notebook (will be modified in-place)

    Returns:
        Set of package names extracted from pip install commands

    Note: This modifies the notebook file in-place.
    """
    notebook = json.loads(Path(ipynb_file).read_text())

    packages: set[str] = set()
    subprocess_imported: bool = False

    for cell in notebook.get("cells", []):
        if cell.get("cell_type") == "code":
            cell_code = cell.get("source", [])
            new_cell_code = []

            for line in cell_code:
                stripped = line.strip()

                # skip magic ommands
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

    with open(ipynb_file, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=1, ensure_ascii=False)

    return packages


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
    # Process all Jupyter notebooks in the education directory, converting them
    # from Colab-compatible format to Marimo-compatible Python files.
    #
    # Processing order is important:
    # 1. Replace Colab imports
    # 2. Process shell commands (extracts deps + converts to subprocess)
    # 3. Convert to Marimo (uses cleaned notebook)
    # 4. Add inline dependencies (prepends PEP 723 metadata to final file)
    # 5. Clean up temporary files
    for ipynb_file in EDUCATION_NOTEBOOKS_DIR.glob("*/*.ipynb"):
        # clean up google colab imports and process shell commands
        temp_ipynb_file = replace_google_colab_imports(ipynb_file)
        packages = process_shell_commands(temp_ipynb_file)

        # convert to marimo py files and add inline dependencies
        py_file = convert_ipynb_to_py(ipynb_file, temp_ipynb_file)
        add_inline_dependencies(packages, py_file)

        temp_ipynb_file.unlink()
