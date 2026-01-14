from typing import Optional, Dict
import io
import tempfile
import zipfile
import requests

from dlt.common.configuration import with_config
from dlt.common.storages.file_storage import FileStorage

from dlt._workspace.cli.exceptions import ScaffoldSourceNotFound, ScaffoldApiError
from dlt._workspace.configuration import WorkspaceRuntimeConfiguration


# Timeout for scaffold API requests (20 seconds to handle larger files)
SCAFFOLD_API_TIMEOUT = 20


@with_config(spec=WorkspaceRuntimeConfiguration, sections=("runtime", "workspace"))
def get_scaffold_files_storage(
    source_name: str, scaffold_docs_api_url: str = None
) -> Optional[FileStorage]:
    """
    Fetch scaffold content for a given source and return it as a FileStorage object in a
    temporary directory.

    Downloads a ZIP archive from the scaffold API and extracts it directly to a temp directory.

    Args:
        source_name: Name of the source to fetch content for
        scaffold_docs_api_url: Base URL of the scaffold API (auto-injected from config)

    Returns:
        FileStorage containing the source files

    Raises:
        ScaffoldSourceNotFound: If the source doesn't exist (404)
        ScaffoldApiError: If there's an error connecting to the API or extracting the ZIP
    """
    # Make HTTP request with Accept header requesting ZIP format for efficient transfer
    url = f"{scaffold_docs_api_url}/api/v1/scaffolds/{source_name}/files"
    headers = {"Accept": "application/zip"}
    try:
        response = requests.get(url, headers=headers, timeout=SCAFFOLD_API_TIMEOUT)
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

    # Create temp directory and extract ZIP directly
    temp_dir = tempfile.mkdtemp(prefix=f"scaffold_{source_name}_")
    try:
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            zip_file.extractall(temp_dir)
    except zipfile.BadZipFile as e:
        raise ScaffoldApiError(f"Invalid ZIP response from scaffold-api: {str(e)}", source_name)

    return FileStorage(temp_dir, makedirs=False)
