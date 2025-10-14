from http import HTTPStatus
from typing import Any, Optional, Union, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.configuration_response import ConfigurationResponse
from ...models.create_configuration_request import CreateConfigurationRequest
from ...models.create_configuration_response_400 import CreateConfigurationResponse400
from ...models.create_configuration_response_401 import CreateConfigurationResponse401
from ...models.create_configuration_response_403 import CreateConfigurationResponse403
from ...models.create_configuration_response_404 import CreateConfigurationResponse404
from ...types import UNSET, Response


def _get_kwargs(
    workspace_id: UUID,
    profile_id_or_name: str,
    *,
    body: CreateConfigurationRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/v1/workspaces/{workspace_id}/profiles/{profile_id_or_name}/configurations".format(
            workspace_id=workspace_id,
            profile_id_or_name=profile_id_or_name,
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[
    Union[
        ConfigurationResponse,
        CreateConfigurationResponse400,
        CreateConfigurationResponse401,
        CreateConfigurationResponse403,
        CreateConfigurationResponse404,
    ]
]:
    if response.status_code == 201:
        response_201 = ConfigurationResponse.from_dict(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = CreateConfigurationResponse400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = CreateConfigurationResponse401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = CreateConfigurationResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = CreateConfigurationResponse404.from_dict(response.json())

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
        CreateConfigurationResponse400,
        CreateConfigurationResponse401,
        CreateConfigurationResponse403,
        CreateConfigurationResponse404,
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
    body: CreateConfigurationRequest,
) -> Response[
    Union[
        ConfigurationResponse,
        CreateConfigurationResponse400,
        CreateConfigurationResponse401,
        CreateConfigurationResponse403,
        CreateConfigurationResponse404,
    ]
]:
    """CreateConfiguration


    Creates a new configuration for a profile. A configuration has a version number, a config and a
    secrets. The
    config and secrets attributes are optional but should be a valid config and secrets if provided.

    PLEASE NOTE: Secrets are not yet encrypted at rest on this endpoint. Avoid sending production
    secrets!

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):
        body (CreateConfigurationRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ConfigurationResponse, CreateConfigurationResponse400, CreateConfigurationResponse401, CreateConfigurationResponse403, CreateConfigurationResponse404]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        profile_id_or_name=profile_id_or_name,
        body=body,
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
    body: CreateConfigurationRequest,
) -> Optional[
    Union[
        ConfigurationResponse,
        CreateConfigurationResponse400,
        CreateConfigurationResponse401,
        CreateConfigurationResponse403,
        CreateConfigurationResponse404,
    ]
]:
    """CreateConfiguration


    Creates a new configuration for a profile. A configuration has a version number, a config and a
    secrets. The
    config and secrets attributes are optional but should be a valid config and secrets if provided.

    PLEASE NOTE: Secrets are not yet encrypted at rest on this endpoint. Avoid sending production
    secrets!

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):
        body (CreateConfigurationRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ConfigurationResponse, CreateConfigurationResponse400, CreateConfigurationResponse401, CreateConfigurationResponse403, CreateConfigurationResponse404]
    """

    return sync_detailed(
        workspace_id=workspace_id,
        profile_id_or_name=profile_id_or_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    workspace_id: UUID,
    profile_id_or_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    body: CreateConfigurationRequest,
) -> Response[
    Union[
        ConfigurationResponse,
        CreateConfigurationResponse400,
        CreateConfigurationResponse401,
        CreateConfigurationResponse403,
        CreateConfigurationResponse404,
    ]
]:
    """CreateConfiguration


    Creates a new configuration for a profile. A configuration has a version number, a config and a
    secrets. The
    config and secrets attributes are optional but should be a valid config and secrets if provided.

    PLEASE NOTE: Secrets are not yet encrypted at rest on this endpoint. Avoid sending production
    secrets!

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):
        body (CreateConfigurationRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ConfigurationResponse, CreateConfigurationResponse400, CreateConfigurationResponse401, CreateConfigurationResponse403, CreateConfigurationResponse404]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        profile_id_or_name=profile_id_or_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    workspace_id: UUID,
    profile_id_or_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    body: CreateConfigurationRequest,
) -> Optional[
    Union[
        ConfigurationResponse,
        CreateConfigurationResponse400,
        CreateConfigurationResponse401,
        CreateConfigurationResponse403,
        CreateConfigurationResponse404,
    ]
]:
    """CreateConfiguration


    Creates a new configuration for a profile. A configuration has a version number, a config and a
    secrets. The
    config and secrets attributes are optional but should be a valid config and secrets if provided.

    PLEASE NOTE: Secrets are not yet encrypted at rest on this endpoint. Avoid sending production
    secrets!

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):
        body (CreateConfigurationRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ConfigurationResponse, CreateConfigurationResponse400, CreateConfigurationResponse401, CreateConfigurationResponse403, CreateConfigurationResponse404]
    """

    return (
        await asyncio_detailed(
            workspace_id=workspace_id,
            profile_id_or_name=profile_id_or_name,
            client=client,
            body=body,
        )
    ).parsed
