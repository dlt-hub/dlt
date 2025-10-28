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
from ...models.list_profile_versions_response_200 import ListProfileVersionsResponse200
from ...types import UNSET, Response, Unset


def _get_kwargs(
    workspace_id: UUID,
    profile_id_or_name: str,
    *,
    limit: int | Unset = 100,
    offset: int | Unset = 0,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["limit"] = limit

    params["offset"] = offset

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/v1/workspaces/{workspace_id}/profiles/{profile_id_or_name}/versions".format(
            workspace_id=workspace_id,
            profile_id_or_name=profile_id_or_name,
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> (
    ErrorResponse400
    | ErrorResponse401
    | ErrorResponse403
    | ErrorResponse404
    | ListProfileVersionsResponse200
    | None
):
    if response.status_code == 200:
        response_200 = ListProfileVersionsResponse200.from_dict(response.json())

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
    ErrorResponse400
    | ErrorResponse401
    | ErrorResponse403
    | ErrorResponse404
    | ListProfileVersionsResponse200
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
    limit: int | Unset = 100,
    offset: int | Unset = 0,
) -> Response[
    ErrorResponse400
    | ErrorResponse401
    | ErrorResponse403
    | ErrorResponse404
    | ListProfileVersionsResponse200
]:
    """ListProfileVersions


    Gets the profile_version history for a profile. Returns a paginated list of profile_versions ordered
    by version descending, latest first.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):
        limit (int | Unset):  Default: 100.
        offset (int | Unset):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | ListProfileVersionsResponse200]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        profile_id_or_name=profile_id_or_name,
        limit=limit,
        offset=offset,
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
    limit: int | Unset = 100,
    offset: int | Unset = 0,
) -> (
    ErrorResponse400
    | ErrorResponse401
    | ErrorResponse403
    | ErrorResponse404
    | ListProfileVersionsResponse200
    | None
):
    """ListProfileVersions


    Gets the profile_version history for a profile. Returns a paginated list of profile_versions ordered
    by version descending, latest first.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):
        limit (int | Unset):  Default: 100.
        offset (int | Unset):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | ListProfileVersionsResponse200
    """

    return sync_detailed(
        workspace_id=workspace_id,
        profile_id_or_name=profile_id_or_name,
        client=client,
        limit=limit,
        offset=offset,
    ).parsed


async def asyncio_detailed(
    workspace_id: UUID,
    profile_id_or_name: str,
    *,
    client: AuthenticatedClient | Client,
    limit: int | Unset = 100,
    offset: int | Unset = 0,
) -> Response[
    ErrorResponse400
    | ErrorResponse401
    | ErrorResponse403
    | ErrorResponse404
    | ListProfileVersionsResponse200
]:
    """ListProfileVersions


    Gets the profile_version history for a profile. Returns a paginated list of profile_versions ordered
    by version descending, latest first.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):
        limit (int | Unset):  Default: 100.
        offset (int | Unset):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | ListProfileVersionsResponse200]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        profile_id_or_name=profile_id_or_name,
        limit=limit,
        offset=offset,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    workspace_id: UUID,
    profile_id_or_name: str,
    *,
    client: AuthenticatedClient | Client,
    limit: int | Unset = 100,
    offset: int | Unset = 0,
) -> (
    ErrorResponse400
    | ErrorResponse401
    | ErrorResponse403
    | ErrorResponse404
    | ListProfileVersionsResponse200
    | None
):
    """ListProfileVersions


    Gets the profile_version history for a profile. Returns a paginated list of profile_versions ordered
    by version descending, latest first.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        profile_id_or_name (str):
        limit (int | Unset):  Default: 100.
        offset (int | Unset):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | ListProfileVersionsResponse200
    """

    return (
        await asyncio_detailed(
            workspace_id=workspace_id,
            profile_id_or_name=profile_id_or_name,
            client=client,
            limit=limit,
            offset=offset,
        )
    ).parsed
