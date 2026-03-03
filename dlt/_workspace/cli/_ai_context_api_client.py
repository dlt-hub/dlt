from typing import Any, Dict, List, Optional
import io
import tempfile
import zipfile
import requests

from dlt.common.configuration import with_config
from dlt.common.storages.file_storage import FileStorage

from dlt._workspace.cli.exceptions import ScaffoldSourceNotFound, ScaffoldApiError
from dlt._workspace.configuration import WorkspaceRuntimeConfiguration
from dlt._workspace.typing import TSourceItem


# Timeout for AI context API requests (20 seconds to handle larger files)
AI_CONTEXT_API_TIMEOUT = 20


@with_config(spec=WorkspaceRuntimeConfiguration, sections=("runtime", "workspace"))
def search_sources(query: str = "", ai_context_api_url: str = None) -> List[TSourceItem]:
    """Search for available sources by name and description.

    Args:
        query: Search string. Empty string returns all sources alphabetically.
        ai_context_api_url: Base URL of the AI context API (auto-injected from config)

    Returns:
        List of source items, each with source_name, description, and optional
        description_verbose and sample_urls fields.

    Raises:
        ScaffoldApiError: If there's an error connecting to the API
    """
    url = f"{ai_context_api_url}/api/v1/scaffolds/sources"
    params = {"q": query} if query else {}
    try:
        response = requests.get(url, params=params, timeout=AI_CONTEXT_API_TIMEOUT)
    except requests.RequestException as e:
        raise ScaffoldApiError(f"There was an error connecting to the AI context API: {str(e)}")

    if response.status_code != 200:
        raise ScaffoldApiError(f"API returned status {response.status_code}: {response.text}")

    data: Dict[str, Any] = response.json()
    results: List[TSourceItem] = data.get("results", [])
    return results


@with_config(spec=WorkspaceRuntimeConfiguration, sections=("runtime", "workspace"))
def get_ai_context_files_storage(
    source_name: str, ai_context_api_url: str = None
) -> Optional[FileStorage]:
    """Fetch AI context content for a given source and return it as a FileStorage.

    Downloads a ZIP archive from the AI context API and extracts it to a temp directory.

    Args:
        source_name: Name of the source to fetch content for
        ai_context_api_url: Base URL of the AI context API (auto-injected from config)

    Returns:
        FileStorage containing the source files

    Raises:
        ScaffoldSourceNotFound: If the source doesn't exist (404)
        ScaffoldApiError: If there's an error connecting to the API or extracting the ZIP
    """
    url = f"{ai_context_api_url}/api/v1/scaffolds/{source_name}/files"
    headers = {"Accept": "application/zip"}
    try:
        response = requests.get(url, headers=headers, timeout=AI_CONTEXT_API_TIMEOUT)
    except requests.RequestException as e:
        raise ScaffoldApiError(
            f"There was an error connecting to the AI context API: {str(e)}", source_name
        )

    if response.status_code == 404:
        raise ScaffoldSourceNotFound(f"Source '{source_name}' not found", source_name)

    if response.status_code != 200:
        raise ScaffoldApiError(
            f"API returned status {response.status_code}: {response.text}", source_name
        )

    temp_dir = tempfile.mkdtemp(prefix=f"ai_context_{source_name}_")
    try:
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            zip_file.extractall(temp_dir)
    except zipfile.BadZipFile as e:
        raise ScaffoldApiError(f"Invalid ZIP response from AI context API: {str(e)}", source_name)

    return FileStorage(temp_dir, makedirs=False)
