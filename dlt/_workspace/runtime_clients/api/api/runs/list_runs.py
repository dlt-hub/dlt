from http import HTTPStatus
from typing import Any, Optional, Union, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_response_400 import ErrorResponse400
from ...models.error_response_401 import ErrorResponse401
from ...models.error_response_403 import ErrorResponse403
from ...models.error_response_404 import ErrorResponse404
from ...models.list_runs_response_200 import ListRunsResponse200
from ...models.run_status import RunStatus
from ...types import UNSET, Response, Unset


def _get_kwargs(
    workspace_id: UUID,
    *,
    script_id: Union[None, UUID, Unset] = UNSET,
    status: Union[None, RunStatus, Unset] = UNSET,
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    json_script_id: Union[None, Unset, str]
    if isinstance(script_id, Unset):
        json_script_id = UNSET
    elif isinstance(script_id, UUID):
        json_script_id = str(script_id)
    else:
        json_script_id = script_id
    params["script_id"] = json_script_id

    json_status: Union[None, Unset, str]
    if isinstance(status, Unset):
        json_status = UNSET
    elif isinstance(status, RunStatus):
        json_status = status.value
    else:
        json_status = status
    params["status"] = json_status

    params["limit"] = limit

    params["offset"] = offset

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/v1/workspaces/{workspace_id}/runs".format(
            workspace_id=workspace_id,
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[
    Union[
        ErrorResponse400, ErrorResponse401, ErrorResponse403, ErrorResponse404, ListRunsResponse200
    ]
]:
    if response.status_code == 200:
        response_200 = ListRunsResponse200.from_dict(response.json())

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
        ErrorResponse400, ErrorResponse401, ErrorResponse403, ErrorResponse404, ListRunsResponse200
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
    script_id: Union[None, UUID, Unset] = UNSET,
    status: Union[None, RunStatus, Unset] = UNSET,
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Response[
    Union[
        ErrorResponse400, ErrorResponse401, ErrorResponse403, ErrorResponse404, ListRunsResponse200
    ]
]:
    """ListRuns


    Gets all runs for a workspace. Returns a paginated list of runs ordered by date_added descending.
    Can be filtered by script ID and status.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        script_id (Union[None, UUID, Unset]):
        status (Union[None, RunStatus, Unset]):
        limit (Union[Unset, int]):  Default: 100.
        offset (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse400, ErrorResponse401, ErrorResponse403, ErrorResponse404, ListRunsResponse200]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        script_id=script_id,
        status=status,
        limit=limit,
        offset=offset,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    workspace_id: UUID,
    *,
    client: Union[AuthenticatedClient, Client],
    script_id: Union[None, UUID, Unset] = UNSET,
    status: Union[None, RunStatus, Unset] = UNSET,
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Optional[
    Union[
        ErrorResponse400, ErrorResponse401, ErrorResponse403, ErrorResponse404, ListRunsResponse200
    ]
]:
    """ListRuns


    Gets all runs for a workspace. Returns a paginated list of runs ordered by date_added descending.
    Can be filtered by script ID and status.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        script_id (Union[None, UUID, Unset]):
        status (Union[None, RunStatus, Unset]):
        limit (Union[Unset, int]):  Default: 100.
        offset (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse400, ErrorResponse401, ErrorResponse403, ErrorResponse404, ListRunsResponse200]
    """

    return sync_detailed(
        workspace_id=workspace_id,
        client=client,
        script_id=script_id,
        status=status,
        limit=limit,
        offset=offset,
    ).parsed


async def asyncio_detailed(
    workspace_id: UUID,
    *,
    client: Union[AuthenticatedClient, Client],
    script_id: Union[None, UUID, Unset] = UNSET,
    status: Union[None, RunStatus, Unset] = UNSET,
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Response[
    Union[
        ErrorResponse400, ErrorResponse401, ErrorResponse403, ErrorResponse404, ListRunsResponse200
    ]
]:
    """ListRuns


    Gets all runs for a workspace. Returns a paginated list of runs ordered by date_added descending.
    Can be filtered by script ID and status.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        script_id (Union[None, UUID, Unset]):
        status (Union[None, RunStatus, Unset]):
        limit (Union[Unset, int]):  Default: 100.
        offset (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse400, ErrorResponse401, ErrorResponse403, ErrorResponse404, ListRunsResponse200]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        script_id=script_id,
        status=status,
        limit=limit,
        offset=offset,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    workspace_id: UUID,
    *,
    client: Union[AuthenticatedClient, Client],
    script_id: Union[None, UUID, Unset] = UNSET,
    status: Union[None, RunStatus, Unset] = UNSET,
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Optional[
    Union[
        ErrorResponse400, ErrorResponse401, ErrorResponse403, ErrorResponse404, ListRunsResponse200
    ]
]:
    """ListRuns


    Gets all runs for a workspace. Returns a paginated list of runs ordered by date_added descending.
    Can be filtered by script ID and status.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        script_id (Union[None, UUID, Unset]):
        status (Union[None, RunStatus, Unset]):
        limit (Union[Unset, int]):  Default: 100.
        offset (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse400, ErrorResponse401, ErrorResponse403, ErrorResponse404, ListRunsResponse200]
    """

    return (
        await asyncio_detailed(
            workspace_id=workspace_id,
            client=client,
            script_id=script_id,
            status=status,
            limit=limit,
            offset=offset,
        )
    ).parsed
