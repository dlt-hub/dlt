from http import HTTPStatus
from typing import Any, Optional, Union, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.ping_response import PingResponse
from ...types import UNSET, Response


def _get_kwargs() -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/v1/ping",
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[PingResponse]:
    if response.status_code == 200:
        response_200 = PingResponse.from_dict(response.json())

        return response_200

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[PingResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[PingResponse]:
    """Ping


    Pings the server to check if it is alive. Returns a pong response.

    This endpoint is public and does not require authentication.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[PingResponse]
    """

    kwargs = _get_kwargs()

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[PingResponse]:
    """Ping


    Pings the server to check if it is alive. Returns a pong response.

    This endpoint is public and does not require authentication.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        PingResponse
    """

    return sync_detailed(
        client=client,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[PingResponse]:
    """Ping


    Pings the server to check if it is alive. Returns a pong response.

    This endpoint is public and does not require authentication.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[PingResponse]
    """

    kwargs = _get_kwargs()

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[PingResponse]:
    """Ping


    Pings the server to check if it is alive. Returns a pong response.

    This endpoint is public and does not require authentication.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        PingResponse
    """

    return (
        await asyncio_detailed(
            client=client,
        )
    ).parsed
