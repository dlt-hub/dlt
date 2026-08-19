import pytest
from docs_tools.education.preprocess_to_molab import (
    replace_colab_imports_in_notebook,
    process_shell_commands_in_notebook,
    add_inline_dependencies_to_content,
)


def test_replace_colab_imports() -> None:
    """Ensure that collab specific imports are removed and converted where necessary."""
    notebook = {
        "cells": [
            {
                "cell_type": "code",
                "source": [
                    "from google.colab import userdata\n",
                    "api_key = userdata.get('API_KEY')\n",
                    "print(api_key)\n",
                ],
            },
        ]
    }
    result = replace_colab_imports_in_notebook(notebook)
    assert result == {
        "cells": [
            {
                "cell_type": "code",
                "source": [
                    "api_key = os.getenv('API_KEY')\n",
                    "print(api_key)\n",
                ],
            },
        ]
    }


def test_process_shell_commands_in_notebook() -> None:
    """Ensure that pip install commands are removed, shell commands converted."""
    notebook = {
        "cells": [
            {
                "cell_type": "code",
                "source": [
                    "!pip install dlt\n",
                    "!pip install dlt[bigquery,postgres]\n",
                    "!pip install requests==2.28.0\n",
                    "!pip install -q scikit-learn\n",
                ],
            },
            {
                "cell_type": "code",
                "source": [
                    "!ls -la\n",
                    "!pwd\n",
                    "!yes | dlt init source destination\n",
                    "!no | some_command --flag\n",
                    "!cat file.txt | grep pattern\n",
                    "%%capture\n",
                    "print('hello')\n",
                ],
            },
        ]
    }

    result, packages = process_shell_commands_in_notebook(notebook)
    assert packages == {
        "dlt",
        "dlt[bigquery,postgres]",
        "requests==2.28.0",
        "scikit-learn",
    }
    assert result == {
        "cells": [
            {"cell_type": "code", "source": []},
            {
                "cell_type": "code",
                "source": [
                    "import subprocess\n",
                    "subprocess.run(['ls', '-la'], check=True)\n",
                    "subprocess.run(['pwd'], check=True)\n",
                    "subprocess.run(['dlt', 'init', 'source', 'destination'], input='y\\n', text=True, check=True)\n",
                    "subprocess.run(['some_command', '--flag'], input='n\\n', text=True, check=True)\n",
                    "subprocess.run('cat file.txt | grep pattern', shell=True, check=True)\n",
                    "print('hello')\n",
                ],
            },
        ]
    }


def test_add_inline_dependencies_to_content() -> None:
    """Ensure that PEP 723 metadata block is correctly added and includes MUST_INSTALL_PACKAGES."""
    packages = {"requests", "dlt[bigquery,postgres]"}
    py_content = "import marimo\n"
    result = add_inline_dependencies_to_content(packages, py_content)
    expected = """# /// script
# dependencies = [
#     "dlt[bigquery,postgres]",
#     "numpy",
#     "pandas",
#     "requests",
#     "sqlalchemy",
# ]
# ///

import marimo
"""
    print(result)
    assert result == expected
