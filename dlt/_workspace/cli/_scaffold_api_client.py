from typing import ClassVar, Optional, Dict
import tempfile
import requests

from dlt.common.configuration import with_config
from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec
from dlt.common.storages.file_storage import FileStorage

from dlt._workspace.cli.exceptions import ScaffoldSourceNotFound, ScaffoldApiError
from dlt._workspace.configuration import WorkspaceRuntimeConfiguration


def _parse_and_validate_response(response: requests.Response, source_name: str) -> Dict[str, str]:
    """
    Parse and validate the JSON response from the scaffold API.

    Args:
        response: The HTTP response object
        source_name: Name of the source (for error messages)

    Returns:
        Dict with filename as key and content as value

    Raises:
        ScaffoldApiError: If the response is invalid JSON or has unexpected structure
    """
    # TODO: once we do more than parsing one simple response, we should consider using a library
    # like attrs for this or some more generic parsing logic
    try:
        data = response.json()
    except ValueError as e:
        raise ScaffoldApiError(f"Invalid JSON response from scaffold-api: {str(e)}", source_name)

    # Validate response structure
    if not isinstance(data, dict) or "files" not in data:
        raise ScaffoldApiError(
            "Invalid response format: expected dict with 'files' key", source_name
        )

    files = data["files"]
    if not isinstance(files, dict):
        raise ScaffoldApiError("Invalid response format: 'files' must be a dict", source_name)

    return files


@with_config(spec=WorkspaceRuntimeConfiguration, sections=("runtime", "workspace"))
def _get_scaffold_files(source_name: str, scaffold_docs_api_url: str = None) -> Dict[str, str]:
    """
    Fetch scaffold content for a given source from the Scaffold API.

    Args:
        source_name: Name of the source to fetch content for
        docs_api_url: Base URL of the scaffold API (auto-injected from config)

    Returns:
        Dict with filename as key and content as value, e.g. {"source_name.md": content}

    Raises:
        ScaffoldSourceNotFound: If the source doesn't exist (404)
        ScaffoldApiError: If there's an error connecting to the API or an invalid JSON response or
        a general request exception
    """
    # Make HTTP request
    url = f"{scaffold_docs_api_url}/api/v1/scaffolds/{source_name}/files"
    try:
        response = requests.get(url, timeout=10)
    except requests.RequestException as e:
        raise ScaffoldApiError(
            f"There was an error connecting to the scaffold-api: {str(e)}", source_name
        )

    # Handle non-200 responses
    if response.status_code == 404:
        raise ScaffoldSourceNotFound(f"Source '{source_name}' not found", source_name)

    if response.status_code != 200:
        raise ScaffoldApiError(
            f"API returned status {response.status_code}: {response.text}", source_name
        )

    # Parse and validate response
    return _parse_and_validate_response(response, source_name)


def get_scaffold_files_storage(source_name: str) -> Optional[FileStorage]:
    """
    Fetch scaffold content content for a given source and return it as a FileStorage object in a
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
