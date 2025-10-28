from http import HTTPStatus
from typing import Any, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.create_deployment_body import CreateDeploymentBody
from ...models.deployment_response import DeploymentResponse
from ...models.error_response_400 import ErrorResponse400
from ...models.error_response_401 import ErrorResponse401
from ...models.error_response_403 import ErrorResponse403
from ...models.error_response_404 import ErrorResponse404
from ...types import UNSET, Response


def _get_kwargs(
    workspace_id: UUID,
    *,
    body: CreateDeploymentBody,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/v1/workspaces/{workspace_id}/deployments".format(
            workspace_id=workspace_id,
        ),
    }

    _kwargs["files"] = body.to_multipart()

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> (
    DeploymentResponse
    | ErrorResponse400
    | ErrorResponse401
    | ErrorResponse403
    | ErrorResponse404
    | None
):
    if response.status_code == 201:
        response_201 = DeploymentResponse.from_dict(response.json())

        return response_201

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
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[
    DeploymentResponse | ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404
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
    client: AuthenticatedClient | Client,
    body: CreateDeploymentBody,
) -> Response[
    DeploymentResponse | ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404
]:
    """CreateDeployment


    Creates a new deployment for a workspace. This deployment will become the new active deployment.
    Requires upload of a tarball of files. Max tarball size is 1MB.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        body (CreateDeploymentBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[DeploymentResponse | ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    workspace_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: CreateDeploymentBody,
) -> (
    DeploymentResponse
    | ErrorResponse400
    | ErrorResponse401
    | ErrorResponse403
    | ErrorResponse404
    | None
):
    """CreateDeployment


    Creates a new deployment for a workspace. This deployment will become the new active deployment.
    Requires upload of a tarball of files. Max tarball size is 1MB.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        body (CreateDeploymentBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        DeploymentResponse | ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404
    """

    return sync_detailed(
        workspace_id=workspace_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    workspace_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: CreateDeploymentBody,
) -> Response[
    DeploymentResponse | ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404
]:
    """CreateDeployment


    Creates a new deployment for a workspace. This deployment will become the new active deployment.
    Requires upload of a tarball of files. Max tarball size is 1MB.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        body (CreateDeploymentBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[DeploymentResponse | ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    workspace_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: CreateDeploymentBody,
) -> (
    DeploymentResponse
    | ErrorResponse400
    | ErrorResponse401
    | ErrorResponse403
    | ErrorResponse404
    | None
):
    """CreateDeployment


    Creates a new deployment for a workspace. This deployment will become the new active deployment.
    Requires upload of a tarball of files. Max tarball size is 1MB.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        body (CreateDeploymentBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        DeploymentResponse | ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404
    """

    return (
        await asyncio_detailed(
            workspace_id=workspace_id,
            client=client,
            body=body,
        )
    ).parsed
