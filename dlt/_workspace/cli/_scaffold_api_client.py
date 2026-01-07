from typing import Optional, List, Dict
import tempfile
import requests
from pydantic import BaseModel

import os
from dlt.common.storages.file_storage import FileStorage

from dlt._workspace.cli.exceptions import ScaffoldSourceNotFound, ScaffoldApiError

# TODO point to deployed scaffold-api
SCAFFOLD_API_BASE_URL = "http://localhost:8000"


class ScaffoldFiles(BaseModel):
    """Response containing scaffold source files."""

    files: Dict[str, str]


def _get_scaffold_files(source_name: str) -> Dict[str, str]:
    """
    Fetch scaffold content for a given source from the Scaffold API.

    Args:
        source_name: Name of the source to fetch content for

    Returns:
        Dict with filename as key and content as value, e.g. {"source_name.md": content}

    Raises:
        ScaffoldSourceNotFound: If the source doesn't exist (404)
        ScaffoldApiError: If there's an error connecting to the API or an invalid JSON response or
        a general request exception
    """
    try:
        url = f"{SCAFFOLD_API_BASE_URL}/api/v1/scaffolds/{source_name}/files"
        response = requests.get(url, timeout=10)

        if response.status_code == 404:
            raise ScaffoldSourceNotFound(f"Source '{source_name}' not found", source_name)
        elif response.status_code == 200:
            data = response.json()
            return ScaffoldFiles(**data).files
        else:
            raise ScaffoldApiError(
                f"API returned status {response.status_code}: {response.text}", source_name
            )
    except ValueError as e:
        raise ScaffoldApiError(f"Invalid JSON response from scaffold-api: {str(e)}", source_name)
    except requests.RequestException as e:
        raise ScaffoldApiError(
            f"There was an error connecting to the scaffold-api: {str(e)}", source_name
        )


def get_scaffold_files_storage(source_name: str) -> Optional[FileStorage]:
    """
    Fetche scaffold content content for a given source and return it as a FileStorage object in a
    temporary directory.

    Args:
        source_name: Name of the source to fetch content for

    Returns:
        FileStorage containing the source files, or None if source doesn't exist

    Raises:
        ScaffoldSourceNotFound: If the source doesn't exist (404)
        ScaffoldApiError: If there's an error connecting to the API
    """
    scaffold_files_dict = _get_scaffold_files(source_name)

    temp_dir = tempfile.mkdtemp(prefix=f"scaffold_{source_name}_")

    storage = FileStorage(temp_dir, makedirs=True)

    for filename, content in scaffold_files_dict.items():
        storage.save(filename, content)

    return storage
