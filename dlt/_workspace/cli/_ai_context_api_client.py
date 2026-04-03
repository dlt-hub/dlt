from typing import Any, Dict, List
import requests

from dlt.common.configuration import with_config

from dlt._workspace.cli.exceptions import AiContextApiError
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
        AiContextApiError: If there's an error connecting to the API
    """
    url = f"{ai_context_api_url}/api/v1/scaffolds/sources"
    params = {"q": query} if query else {}
    try:
        response = requests.get(url, params=params, timeout=AI_CONTEXT_API_TIMEOUT)
    except requests.RequestException as e:
        raise AiContextApiError(f"There was an error connecting to the AI context API: {str(e)}")

    if response.status_code != 200:
        raise AiContextApiError(f"API returned status {response.status_code}: {response.text}")

    data: Dict[str, Any] = response.json()
    results: List[TSourceItem] = data.get("results", [])
    return results
