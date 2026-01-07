from typing import Optional, List, Dict
import tempfile
import requests

import os
from dlt.common.storages.file_storage import FileStorage

from dlt._workspace.cli.exceptions import VibeSourceNotFound, ScaffoldApiError

SCAFFOLD_API_BASE_URL = os.environ.get("SCAFFOLD_API_BASE_URL", "http://localhost:8000")

if not SCAFFOLD_API_BASE_URL:
    raise ValueError("SCAFFOLD_API_BASE_URL environment variable must be set")


def _get_vibe_files(source_name: str) -> dict[str, str]:
    """
    Fetch vibe content for a given source from the Scaffold API.

    Args:
        source_name: Name of the source to fetch content for

    Returns:
        Dict with filename as key and content as value, e.g. {"source_name.md": content}

    Raises:
        VibeSourceNotFound: If the source doesn't exist (404)
        ScaffoldApiError: If there's an error connecting to the API
    """
    try:
        url = f"{SCAFFOLD_API_BASE_URL}/api/v1/scaffolds/{source_name}/files"
        response = requests.get(url, timeout=10)

        if response.status_code == 404:
            raise VibeSourceNotFound(f"Source '{source_name}' not found", source_name)
        elif response.status_code == 200:
            data = response.json()
            return data.get("files", {})
        else:
            raise ScaffoldApiError(
                f"API returned status {response.status_code}: {response.text}", source_name
            )
    except requests.RequestException as e:
        raise ScaffoldApiError(
            f"There was an error connecting to the scaffold-api: {str(e)}", source_name
        )
    except ValueError as e:
        raise ScaffoldApiError(f"Invalid JSON response from scaffold-api: {str(e)}", source_name)


def get_vibe_files_storage(source_name: str) -> Optional[FileStorage]:
    """
    Fetch vibe content for a given source and return it as a FileStorage object in a temporary
    directory.

    Args:
        source_name: Name of the source to fetch content for

    Returns:
        FileStorage containing the source files, or None if source doesn't exist

    Raises:
        VibeSourceNotFound: If the source doesn't exist (404)
        ScaffoldApiError: If there's an error connecting to the API
    """
    files_dict = _get_vibe_files(source_name)
    if not files_dict:
        return None

    # Create a temporary directory for the FileStorage
    temp_dir = tempfile.mkdtemp(prefix=f"vibe_{source_name}_")

    # Create FileStorage pointing to the temp directory
    storage = FileStorage(temp_dir, makedirs=True)

    # Save each file from the dict to the storage
    for filename, content in files_dict.items():
        storage.save(filename, content)

    return storage
