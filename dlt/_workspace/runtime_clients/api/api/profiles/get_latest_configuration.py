from http import HTTPStatus
from typing import Any, Optional, Union, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.configuration_response import ConfigurationResponse
from ...models.get_latest_configuration_response_400 import GetLatestConfigurationResponse400
from ...models.get_latest_configuration_response_401 import GetLatestConfigurationResponse401
from ...models.get_latest_configuration_response_403 import GetLatestConfigurationResponse403
from ...models.get_latest_configuration_response_404 import GetLatestConfigurationResponse404
from ...types import UNSET, Response


def _get_kwargs(
    workspace_id: UUID,
    profile_id_or_name: str,
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/v1/workspaces/{workspace_id}/profiles/{profile_id_or_name}/configurations/latest".format(
            workspace_id=workspace_id,
            profile_id_or_name=profile_id_or_name,
        ),
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[
    Union[
        ConfigurationResponse,
        GetLatestConfigurationResponse400,
        GetLatestConfigurationResponse401,
        GetLatestConfigurationResponse403,
        GetLatestConfigurationResponse404,
    ]
]:
    if response.status_code == 200:
        response_200 = ConfigurationResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = GetLatestConfigurationResponse400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = GetLatestConfigurationResponse401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = GetLatestConfigurationResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = GetLatestConfigurationResponse404.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[
    Union[
        ConfigurationResponse,
        GetLatestConfigurationResponse400,
        GetLatestConfigurationResponse401,
        GetLatestConfigurationResponse403,
        GetLatestConfigurationResponse404,
    ]
]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    workspace_id: UUID,
    profile_id_or_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[
    Union[
        ConfigurationResponse,
        GetLatestConfigurationResponse400,
        GetLatestConfigurationResponse401,
        GetLatestConfigurationResponse403,
        GetLatestConfigurationResponse404,
    ]
]:
    """GetLatestConfiguration


    Gets the latest and therefore active configuration for a profile. If non exists, a default
    configuration is created and returned.

    Requires READ permission. On the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ConfigurationResponse, GetLatestConfigurationResponse400, GetLatestConfigurationResponse401, GetLatestConfigurationResponse403, GetLatestConfigurationResponse404]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        profile_id_or_name=profile_id_or_name,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    workspace_id: UUID,
    profile_id_or_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[
    Union[
        ConfigurationResponse,
        GetLatestConfigurationResponse400,
        GetLatestConfigurationResponse401,
        GetLatestConfigurationResponse403,
        GetLatestConfigurationResponse404,
    ]
]:
    """GetLatestConfiguration


    Gets the latest and therefore active configuration for a profile. If non exists, a default
    configuration is created and returned.

    Requires READ permission. On the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ConfigurationResponse, GetLatestConfigurationResponse400, GetLatestConfigurationResponse401, GetLatestConfigurationResponse403, GetLatestConfigurationResponse404]
    """

    return sync_detailed(
        workspace_id=workspace_id,
        profile_id_or_name=profile_id_or_name,
        client=client,
    ).parsed


async def asyncio_detailed(
    workspace_id: UUID,
    profile_id_or_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[
    Union[
        ConfigurationResponse,
        GetLatestConfigurationResponse400,
        GetLatestConfigurationResponse401,
        GetLatestConfigurationResponse403,
        GetLatestConfigurationResponse404,
    ]
]:
    """GetLatestConfiguration


    Gets the latest and therefore active configuration for a profile. If non exists, a default
    configuration is created and returned.

    Requires READ permission. On the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ConfigurationResponse, GetLatestConfigurationResponse400, GetLatestConfigurationResponse401, GetLatestConfigurationResponse403, GetLatestConfigurationResponse404]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        profile_id_or_name=profile_id_or_name,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    workspace_id: UUID,
    profile_id_or_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[
    Union[
        ConfigurationResponse,
        GetLatestConfigurationResponse400,
        GetLatestConfigurationResponse401,
        GetLatestConfigurationResponse403,
        GetLatestConfigurationResponse404,
    ]
]:
    """GetLatestConfiguration


    Gets the latest and therefore active configuration for a profile. If non exists, a default
    configuration is created and returned.

    Requires READ permission. On the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ConfigurationResponse, GetLatestConfigurationResponse400, GetLatestConfigurationResponse401, GetLatestConfigurationResponse403, GetLatestConfigurationResponse404]
    """

    return (
        await asyncio_detailed(
            workspace_id=workspace_id,
            profile_id_or_name=profile_id_or_name,
            client=client,
        )
    ).parsed
