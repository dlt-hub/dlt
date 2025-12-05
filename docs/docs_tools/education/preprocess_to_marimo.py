from typing import Any, Optional
import json
import re
import shlex
import subprocess
from pathlib import Path
from docs_tools.education.constans import (
    TEMP_IPYNB_FILE_PREIFX,
    MUST_INSTALL_PACKAGES,
    EDUCATION_NOTEBOOKS_DIR,
)


def clean_whitespace(py_file: Path) -> None:
    """Remove trailing spaces and convert tabs to 4 spaces."""
    text = py_file.read_text(encoding="utf-8")
    text = re.sub(r"[ \t]+$", "", text, flags=re.MULTILINE)
    text = text.replace("\t", "    ")
    py_file.write_text(text, encoding="utf-8")


def format_with_black(py_file: Path) -> None:
    """Run black on the given list of files."""
    subprocess.run(["uv", "run", "black", f"{str(py_file)}"], check=True)


def convert_ipynb_to_py(original_ipynb: Path, cleaned_ipynb: Path) -> Path:
    """Run `marimo convert` on the cleaned `.ipynb` but
    write to the original .py filename."""

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
    """Add PEP 723 inline script metadata to a marimo Python file."""
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
    """Remove Google Colab imports from notebook cells and replace with os.getenv."""

    with open(ipynb_file, "r", encoding="utf-8") as f:
        notebook = json.load(f)

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
    """Extract pip packages AND convert shell commands to subprocess.run()."""

    with open(ipynb_file, "r", encoding="utf-8") as f:
        notebook = json.load(f)

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
        # clean up google colab imports and process shell commands
        temp_ipynb_file = replace_google_colab_imports(ipynb_file)
        packages = process_shell_commands(temp_ipynb_file)

        # convert to marimo py files and add inline dependencies
        py_file = convert_ipynb_to_py(ipynb_file, temp_ipynb_file)
        add_inline_dependencies(packages, py_file)

        # clean up py files
        clean_whitespace(py_file)
        #        format_with_black(py_file)

        temp_ipynb_file.unlink()
