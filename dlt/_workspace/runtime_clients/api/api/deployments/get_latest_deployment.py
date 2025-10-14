from http import HTTPStatus
from typing import Any, Optional, Union, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.deployment_response import DeploymentResponse
from ...models.get_latest_deployment_response_400 import GetLatestDeploymentResponse400
from ...models.get_latest_deployment_response_401 import GetLatestDeploymentResponse401
from ...models.get_latest_deployment_response_403 import GetLatestDeploymentResponse403
from ...models.get_latest_deployment_response_404 import GetLatestDeploymentResponse404
from ...types import UNSET, Response


def _get_kwargs(
    workspace_id: UUID,
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/v1/workspaces/{workspace_id}/deployments/latest".format(
            workspace_id=workspace_id,
        ),
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[
    Union[
        DeploymentResponse,
        GetLatestDeploymentResponse400,
        GetLatestDeploymentResponse401,
        GetLatestDeploymentResponse403,
        GetLatestDeploymentResponse404,
    ]
]:
    if response.status_code == 200:
        response_200 = DeploymentResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = GetLatestDeploymentResponse400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = GetLatestDeploymentResponse401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = GetLatestDeploymentResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = GetLatestDeploymentResponse404.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[
    Union[
        DeploymentResponse,
        GetLatestDeploymentResponse400,
        GetLatestDeploymentResponse401,
        GetLatestDeploymentResponse403,
        GetLatestDeploymentResponse404,
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
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[
    Union[
        DeploymentResponse,
        GetLatestDeploymentResponse400,
        GetLatestDeploymentResponse401,
        GetLatestDeploymentResponse403,
        GetLatestDeploymentResponse404,
    ]
]:
    """GetLatestDeployment


    Gets the latest and therefore active deployment for a workspace.

    Requires READ permission. On the organization level.

    Args:
        workspace_id (UUID):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[DeploymentResponse, GetLatestDeploymentResponse400, GetLatestDeploymentResponse401, GetLatestDeploymentResponse403, GetLatestDeploymentResponse404]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    workspace_id: UUID,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[
    Union[
        DeploymentResponse,
        GetLatestDeploymentResponse400,
        GetLatestDeploymentResponse401,
        GetLatestDeploymentResponse403,
        GetLatestDeploymentResponse404,
    ]
]:
    """GetLatestDeployment


    Gets the latest and therefore active deployment for a workspace.

    Requires READ permission. On the organization level.

    Args:
        workspace_id (UUID):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[DeploymentResponse, GetLatestDeploymentResponse400, GetLatestDeploymentResponse401, GetLatestDeploymentResponse403, GetLatestDeploymentResponse404]
    """

    return sync_detailed(
        workspace_id=workspace_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    workspace_id: UUID,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[
    Union[
        DeploymentResponse,
        GetLatestDeploymentResponse400,
        GetLatestDeploymentResponse401,
        GetLatestDeploymentResponse403,
        GetLatestDeploymentResponse404,
    ]
]:
    """GetLatestDeployment


    Gets the latest and therefore active deployment for a workspace.

    Requires READ permission. On the organization level.

    Args:
        workspace_id (UUID):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[DeploymentResponse, GetLatestDeploymentResponse400, GetLatestDeploymentResponse401, GetLatestDeploymentResponse403, GetLatestDeploymentResponse404]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    workspace_id: UUID,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[
    Union[
        DeploymentResponse,
        GetLatestDeploymentResponse400,
        GetLatestDeploymentResponse401,
        GetLatestDeploymentResponse403,
        GetLatestDeploymentResponse404,
    ]
]:
    """GetLatestDeployment


    Gets the latest and therefore active deployment for a workspace.

    Requires READ permission. On the organization level.

    Args:
        workspace_id (UUID):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[DeploymentResponse, GetLatestDeploymentResponse400, GetLatestDeploymentResponse401, GetLatestDeploymentResponse403, GetLatestDeploymentResponse404]
    """

    return (
        await asyncio_detailed(
            workspace_id=workspace_id,
            client=client,
        )
    ).parsed
