from http import HTTPStatus
from typing import Any, Optional, Union, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.configuration_response import ConfigurationResponse
from ...models.get_configuration_response_400 import GetConfigurationResponse400
from ...models.get_configuration_response_401 import GetConfigurationResponse401
from ...models.get_configuration_response_403 import GetConfigurationResponse403
from ...models.get_configuration_response_404 import GetConfigurationResponse404
from ...types import UNSET, Response


def _get_kwargs(
    workspace_id: UUID,
    profile_id_or_name: str,
    configuration_id_or_version: Union[UUID, int],
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/v1/workspaces/{workspace_id}/profiles/{profile_id_or_name}/configurations/{configuration_id_or_version}".format(
            workspace_id=workspace_id,
            profile_id_or_name=profile_id_or_name,
            configuration_id_or_version=configuration_id_or_version,
        ),
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[
    Union[
        ConfigurationResponse,
        GetConfigurationResponse400,
        GetConfigurationResponse401,
        GetConfigurationResponse403,
        GetConfigurationResponse404,
    ]
]:
    if response.status_code == 200:
        response_200 = ConfigurationResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = GetConfigurationResponse400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = GetConfigurationResponse401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = GetConfigurationResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = GetConfigurationResponse404.from_dict(response.json())

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
        GetConfigurationResponse400,
        GetConfigurationResponse401,
        GetConfigurationResponse403,
        GetConfigurationResponse404,
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
    configuration_id_or_version: Union[UUID, int],
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[
    Union[
        ConfigurationResponse,
        GetConfigurationResponse400,
        GetConfigurationResponse401,
        GetConfigurationResponse403,
        GetConfigurationResponse404,
    ]
]:
    """GetConfiguration


    Gets a configuration for a profile, either by ID or by version number.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):
        configuration_id_or_version (Union[UUID, int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ConfigurationResponse, GetConfigurationResponse400, GetConfigurationResponse401, GetConfigurationResponse403, GetConfigurationResponse404]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        profile_id_or_name=profile_id_or_name,
        configuration_id_or_version=configuration_id_or_version,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    workspace_id: UUID,
    profile_id_or_name: str,
    configuration_id_or_version: Union[UUID, int],
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[
    Union[
        ConfigurationResponse,
        GetConfigurationResponse400,
        GetConfigurationResponse401,
        GetConfigurationResponse403,
        GetConfigurationResponse404,
    ]
]:
    """GetConfiguration


    Gets a configuration for a profile, either by ID or by version number.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):
        configuration_id_or_version (Union[UUID, int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ConfigurationResponse, GetConfigurationResponse400, GetConfigurationResponse401, GetConfigurationResponse403, GetConfigurationResponse404]
    """

    return sync_detailed(
        workspace_id=workspace_id,
        profile_id_or_name=profile_id_or_name,
        configuration_id_or_version=configuration_id_or_version,
        client=client,
    ).parsed


async def asyncio_detailed(
    workspace_id: UUID,
    profile_id_or_name: str,
    configuration_id_or_version: Union[UUID, int],
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[
    Union[
        ConfigurationResponse,
        GetConfigurationResponse400,
        GetConfigurationResponse401,
        GetConfigurationResponse403,
        GetConfigurationResponse404,
    ]
]:
    """GetConfiguration


    Gets a configuration for a profile, either by ID or by version number.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):
        configuration_id_or_version (Union[UUID, int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ConfigurationResponse, GetConfigurationResponse400, GetConfigurationResponse401, GetConfigurationResponse403, GetConfigurationResponse404]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        profile_id_or_name=profile_id_or_name,
        configuration_id_or_version=configuration_id_or_version,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    workspace_id: UUID,
    profile_id_or_name: str,
    configuration_id_or_version: Union[UUID, int],
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[
    Union[
        ConfigurationResponse,
        GetConfigurationResponse400,
        GetConfigurationResponse401,
        GetConfigurationResponse403,
        GetConfigurationResponse404,
    ]
]:
    """GetConfiguration


    Gets a configuration for a profile, either by ID or by version number.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):
        configuration_id_or_version (Union[UUID, int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ConfigurationResponse, GetConfigurationResponse400, GetConfigurationResponse401, GetConfigurationResponse403, GetConfigurationResponse404]
    """

    return (
        await asyncio_detailed(
            workspace_id=workspace_id,
            profile_id_or_name=profile_id_or_name,
            configuration_id_or_version=configuration_id_or_version,
            client=client,
        )
    ).parsed
