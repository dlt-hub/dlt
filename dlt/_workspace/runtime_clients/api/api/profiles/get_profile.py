from http import HTTPStatus
from typing import Any, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_response_400 import ErrorResponse400
from ...models.error_response_401 import ErrorResponse401
from ...models.error_response_403 import ErrorResponse403
from ...models.error_response_404 import ErrorResponse404
from ...models.profile_response import ProfileResponse
from ...types import UNSET, Response


def _get_kwargs(
    workspace_id: UUID,
    profile_id_or_name: str,
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/v1/workspaces/{workspace_id}/profiles/{profile_id_or_name}".format(
            workspace_id=workspace_id,
            profile_id_or_name=profile_id_or_name,
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> (
    ErrorResponse400
    | ErrorResponse401
    | ErrorResponse403
    | ErrorResponse404
    | ProfileResponse
    | None
):
    if response.status_code == 200:
        response_200 = ProfileResponse.from_dict(response.json())

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
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[
    ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | ProfileResponse
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
    client: AuthenticatedClient | Client,
) -> Response[
    ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | ProfileResponse
]:
    """GetProfile


    Gets a profile for a workspace, either by ID or by profile name.

    Requires READ permission. On the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | ProfileResponse]
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
    client: AuthenticatedClient | Client,
) -> (
    ErrorResponse400
    | ErrorResponse401
    | ErrorResponse403
    | ErrorResponse404
    | ProfileResponse
    | None
):
    """GetProfile


    Gets a profile for a workspace, either by ID or by profile name.

    Requires READ permission. On the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | ProfileResponse
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
    client: AuthenticatedClient | Client,
) -> Response[
    ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | ProfileResponse
]:
    """GetProfile


    Gets a profile for a workspace, either by ID or by profile name.

    Requires READ permission. On the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | ProfileResponse]
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
    client: AuthenticatedClient | Client,
) -> (
    ErrorResponse400
    | ErrorResponse401
    | ErrorResponse403
    | ErrorResponse404
    | ProfileResponse
    | None
):
    """GetProfile


    Gets a profile for a workspace, either by ID or by profile name.

    Requires READ permission. On the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | ProfileResponse
    """

    return (
        await asyncio_detailed(
            workspace_id=workspace_id,
            profile_id_or_name=profile_id_or_name,
            client=client,
        )
    ).parsed
