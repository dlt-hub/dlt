from http import HTTPStatus
from typing import Any, Optional, Union, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.me_response import MeResponse
from ...models.me_response_401 import MeResponse401
from ...models.me_response_403 import MeResponse403
from ...models.me_response_404 import MeResponse404
from ...types import UNSET, Response


def _get_kwargs() -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/v1/me",
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[MeResponse, MeResponse401, MeResponse403, MeResponse404]]:
    if response.status_code == 200:
        response_200 = MeResponse.from_dict(response.json())

        return response_200

    if response.status_code == 401:
        response_401 = MeResponse401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = MeResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = MeResponse404.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[MeResponse, MeResponse401, MeResponse403, MeResponse404]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[Union[MeResponse, MeResponse401, MeResponse403, MeResponse404]]:
    """Me


    Get the current users info including default organization and workspace, creates default
    organization and workspace if needed.

    Requires Authorization Header

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[MeResponse, MeResponse401, MeResponse403, MeResponse404]]
    """

    kwargs = _get_kwargs()

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[MeResponse, MeResponse401, MeResponse403, MeResponse404]]:
    """Me


    Get the current users info including default organization and workspace, creates default
    organization and workspace if needed.

    Requires Authorization Header

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[MeResponse, MeResponse401, MeResponse403, MeResponse404]
    """

    return sync_detailed(
        client=client,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[Union[MeResponse, MeResponse401, MeResponse403, MeResponse404]]:
    """Me


    Get the current users info including default organization and workspace, creates default
    organization and workspace if needed.

    Requires Authorization Header

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[MeResponse, MeResponse401, MeResponse403, MeResponse404]]
    """

    kwargs = _get_kwargs()

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[MeResponse, MeResponse401, MeResponse403, MeResponse404]]:
    """Me


    Get the current users info including default organization and workspace, creates default
    organization and workspace if needed.

    Requires Authorization Header

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[MeResponse, MeResponse401, MeResponse403, MeResponse404]
    """

    return (
        await asyncio_detailed(
            client=client,
        )
    ).parsed
