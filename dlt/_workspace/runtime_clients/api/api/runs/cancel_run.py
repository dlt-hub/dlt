# Python internals
from http import HTTPStatus
from typing import Any, Optional, Union
from uuid import UUID

# Other libraries
import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.cancel_run_response_400 import CancelRunResponse400
from ...models.cancel_run_response_401 import CancelRunResponse401
from ...models.cancel_run_response_403 import CancelRunResponse403
from ...models.cancel_run_response_404 import CancelRunResponse404
from ...models.detailed_run_response import DetailedRunResponse
from ...types import Response


def _get_kwargs(
    workspace_id: UUID,
    run_id: UUID,
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/v1/workspaces/{workspace_id}/runs/{run_id}/cancel".format(
            workspace_id=workspace_id,
            run_id=run_id,
        ),
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[
    Union[
        CancelRunResponse400,
        CancelRunResponse401,
        CancelRunResponse403,
        CancelRunResponse404,
        DetailedRunResponse,
    ]
]:
    if response.status_code == 201:
        response_201 = DetailedRunResponse.from_dict(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = CancelRunResponse400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = CancelRunResponse401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = CancelRunResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = CancelRunResponse404.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[
    Union[
        CancelRunResponse400,
        CancelRunResponse401,
        CancelRunResponse403,
        CancelRunResponse404,
        DetailedRunResponse,
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
    run_id: UUID,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[
    Union[
        CancelRunResponse400,
        CancelRunResponse401,
        CancelRunResponse403,
        CancelRunResponse404,
        DetailedRunResponse,
    ]
]:
    """CancelRun


    Cancels a run for a workspace by ID.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        run_id (UUID):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[CancelRunResponse400, CancelRunResponse401, CancelRunResponse403, CancelRunResponse404, DetailedRunResponse]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        run_id=run_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    workspace_id: UUID,
    run_id: UUID,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[
    Union[
        CancelRunResponse400,
        CancelRunResponse401,
        CancelRunResponse403,
        CancelRunResponse404,
        DetailedRunResponse,
    ]
]:
    """CancelRun


    Cancels a run for a workspace by ID.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        run_id (UUID):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[CancelRunResponse400, CancelRunResponse401, CancelRunResponse403, CancelRunResponse404, DetailedRunResponse]
    """

    return sync_detailed(
        workspace_id=workspace_id,
        run_id=run_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    workspace_id: UUID,
    run_id: UUID,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[
    Union[
        CancelRunResponse400,
        CancelRunResponse401,
        CancelRunResponse403,
        CancelRunResponse404,
        DetailedRunResponse,
    ]
]:
    """CancelRun


    Cancels a run for a workspace by ID.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        run_id (UUID):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[CancelRunResponse400, CancelRunResponse401, CancelRunResponse403, CancelRunResponse404, DetailedRunResponse]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        run_id=run_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    workspace_id: UUID,
    run_id: UUID,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[
    Union[
        CancelRunResponse400,
        CancelRunResponse401,
        CancelRunResponse403,
        CancelRunResponse404,
        DetailedRunResponse,
    ]
]:
    """CancelRun


    Cancels a run for a workspace by ID.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        run_id (UUID):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[CancelRunResponse400, CancelRunResponse401, CancelRunResponse403, CancelRunResponse404, DetailedRunResponse]
    """

    return (
        await asyncio_detailed(
            workspace_id=workspace_id,
            run_id=run_id,
            client=client,
        )
    ).parsed
