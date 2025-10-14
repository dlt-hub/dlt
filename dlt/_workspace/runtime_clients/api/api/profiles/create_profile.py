from http import HTTPStatus
from typing import Any, Optional, Union, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.create_profile_request import CreateProfileRequest
from ...models.create_profile_response_400 import CreateProfileResponse400
from ...models.create_profile_response_401 import CreateProfileResponse401
from ...models.create_profile_response_403 import CreateProfileResponse403
from ...models.create_profile_response_404 import CreateProfileResponse404
from ...models.profile_response import ProfileResponse
from ...types import UNSET, Response


def _get_kwargs(
    workspace_id: UUID,
    *,
    body: CreateProfileRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/v1/workspaces/{workspace_id}/profiles".format(
            workspace_id=workspace_id,
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
        CreateProfileResponse400,
        CreateProfileResponse401,
        CreateProfileResponse403,
        CreateProfileResponse404,
        ProfileResponse,
    ]
]:
    if response.status_code == 201:
        response_201 = ProfileResponse.from_dict(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = CreateProfileResponse400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = CreateProfileResponse401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = CreateProfileResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = CreateProfileResponse404.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[
    Union[
        CreateProfileResponse400,
        CreateProfileResponse401,
        CreateProfileResponse403,
        CreateProfileResponse404,
        ProfileResponse,
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
    body: CreateProfileRequest,
) -> Response[
    Union[
        CreateProfileResponse400,
        CreateProfileResponse401,
        CreateProfileResponse403,
        CreateProfileResponse404,
        ProfileResponse,
    ]
]:
    """CreateProfile


    Creates a new profile for a workspace. A profile name in a workspace must be unique. A profile has a
    versioned configuration, the
    highest version is the active configuration. When running a script, the profile can be specified to
    use a specific configuration.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        body (CreateProfileRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[CreateProfileResponse400, CreateProfileResponse401, CreateProfileResponse403, CreateProfileResponse404, ProfileResponse]]
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
    client: Union[AuthenticatedClient, Client],
    body: CreateProfileRequest,
) -> Optional[
    Union[
        CreateProfileResponse400,
        CreateProfileResponse401,
        CreateProfileResponse403,
        CreateProfileResponse404,
        ProfileResponse,
    ]
]:
    """CreateProfile


    Creates a new profile for a workspace. A profile name in a workspace must be unique. A profile has a
    versioned configuration, the
    highest version is the active configuration. When running a script, the profile can be specified to
    use a specific configuration.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        body (CreateProfileRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[CreateProfileResponse400, CreateProfileResponse401, CreateProfileResponse403, CreateProfileResponse404, ProfileResponse]
    """

    return sync_detailed(
        workspace_id=workspace_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    workspace_id: UUID,
    *,
    client: Union[AuthenticatedClient, Client],
    body: CreateProfileRequest,
) -> Response[
    Union[
        CreateProfileResponse400,
        CreateProfileResponse401,
        CreateProfileResponse403,
        CreateProfileResponse404,
        ProfileResponse,
    ]
]:
    """CreateProfile


    Creates a new profile for a workspace. A profile name in a workspace must be unique. A profile has a
    versioned configuration, the
    highest version is the active configuration. When running a script, the profile can be specified to
    use a specific configuration.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        body (CreateProfileRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[CreateProfileResponse400, CreateProfileResponse401, CreateProfileResponse403, CreateProfileResponse404, ProfileResponse]]
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
    client: Union[AuthenticatedClient, Client],
    body: CreateProfileRequest,
) -> Optional[
    Union[
        CreateProfileResponse400,
        CreateProfileResponse401,
        CreateProfileResponse403,
        CreateProfileResponse404,
        ProfileResponse,
    ]
]:
    """CreateProfile


    Creates a new profile for a workspace. A profile name in a workspace must be unique. A profile has a
    versioned configuration, the
    highest version is the active configuration. When running a script, the profile can be specified to
    use a specific configuration.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        body (CreateProfileRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[CreateProfileResponse400, CreateProfileResponse401, CreateProfileResponse403, CreateProfileResponse404, ProfileResponse]
    """

    return (
        await asyncio_detailed(
            workspace_id=workspace_id,
            client=client,
            body=body,
        )
    ).parsed
