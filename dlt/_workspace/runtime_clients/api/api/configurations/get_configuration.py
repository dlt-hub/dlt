from http import HTTPStatus
from typing import Any, Optional, Union, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.configuration_response import ConfigurationResponse
from ...models.error_response_400 import ErrorResponse400
from ...models.error_response_401 import ErrorResponse401
from ...models.error_response_403 import ErrorResponse403
from ...models.error_response_404 import ErrorResponse404
from ...types import UNSET, Response


def _get_kwargs(
    workspace_id: UUID,
    configuration_id_or_version: Union[UUID, int],
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/v1/workspaces/{workspace_id}/configurations/{configuration_id_or_version}".format(
            workspace_id=workspace_id,
            configuration_id_or_version=configuration_id_or_version,
        ),
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[
    Union[
        ConfigurationResponse,
        ErrorResponse400,
        ErrorResponse401,
        ErrorResponse403,
        ErrorResponse404,
    ]
]:
    if response.status_code == 200:
        response_200 = ConfigurationResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = ErrorResponse400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = ErrorResponse401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = ErrorResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = ErrorResponse404.from_dict(response.json())

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
        ErrorResponse400,
        ErrorResponse401,
        ErrorResponse403,
        ErrorResponse404,
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
    configuration_id_or_version: Union[UUID, int],
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[
    Union[
        ConfigurationResponse,
        ErrorResponse400,
        ErrorResponse401,
        ErrorResponse403,
        ErrorResponse404,
    ]
]:
    """GetConfiguration


    Gets a configuration for a workspace, either by ID or by version number.

    Requires READ permission. On the organization level.

    Args:
        workspace_id (UUID):
        configuration_id_or_version (Union[UUID, int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ConfigurationResponse, ErrorResponse400, ErrorResponse401, ErrorResponse403, ErrorResponse404]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        configuration_id_or_version=configuration_id_or_version,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    workspace_id: UUID,
    configuration_id_or_version: Union[UUID, int],
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[
    Union[
        ConfigurationResponse,
        ErrorResponse400,
        ErrorResponse401,
        ErrorResponse403,
        ErrorResponse404,
    ]
]:
    """GetConfiguration


    Gets a configuration for a workspace, either by ID or by version number.

    Requires READ permission. On the organization level.

    Args:
        workspace_id (UUID):
        configuration_id_or_version (Union[UUID, int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ConfigurationResponse, ErrorResponse400, ErrorResponse401, ErrorResponse403, ErrorResponse404]
    """

    return sync_detailed(
        workspace_id=workspace_id,
        configuration_id_or_version=configuration_id_or_version,
        client=client,
    ).parsed


async def asyncio_detailed(
    workspace_id: UUID,
    configuration_id_or_version: Union[UUID, int],
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[
    Union[
        ConfigurationResponse,
        ErrorResponse400,
        ErrorResponse401,
        ErrorResponse403,
        ErrorResponse404,
    ]
]:
    """GetConfiguration


    Gets a configuration for a workspace, either by ID or by version number.

    Requires READ permission. On the organization level.

    Args:
        workspace_id (UUID):
        configuration_id_or_version (Union[UUID, int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ConfigurationResponse, ErrorResponse400, ErrorResponse401, ErrorResponse403, ErrorResponse404]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        configuration_id_or_version=configuration_id_or_version,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    workspace_id: UUID,
    configuration_id_or_version: Union[UUID, int],
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[
    Union[
        ConfigurationResponse,
        ErrorResponse400,
        ErrorResponse401,
        ErrorResponse403,
        ErrorResponse404,
    ]
]:
    """GetConfiguration


    Gets a configuration for a workspace, either by ID or by version number.

    Requires READ permission. On the organization level.

    Args:
        workspace_id (UUID):
        configuration_id_or_version (Union[UUID, int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ConfigurationResponse, ErrorResponse400, ErrorResponse401, ErrorResponse403, ErrorResponse404]
    """

    return (
        await asyncio_detailed(
            workspace_id=workspace_id,
            configuration_id_or_version=configuration_id_or_version,
            client=client,
        )
    ).parsed
