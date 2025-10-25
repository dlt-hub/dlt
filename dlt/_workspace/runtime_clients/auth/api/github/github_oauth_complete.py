from http import HTTPStatus
from typing import Any, Optional, Union, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.github_device_flow_login_request import GithubDeviceFlowLoginRequest
from ...models.github_oauth_complete_response_400 import GithubOauthCompleteResponse400
from ...models.login_response import LoginResponse
from ...types import UNSET, Response


def _get_kwargs(
    *,
    body: GithubDeviceFlowLoginRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/v1/github/device-flow/token",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[GithubOauthCompleteResponse400, LoginResponse]]:
    if response.status_code == 201:
        response_201 = LoginResponse.from_dict(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = GithubOauthCompleteResponse400.from_dict(response.json())

        return response_400

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[GithubOauthCompleteResponse400, LoginResponse]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    body: GithubDeviceFlowLoginRequest,
) -> Response[Union[GithubOauthCompleteResponse400, LoginResponse]]:
    """GithubOauthComplete

    Args:
        body (GithubDeviceFlowLoginRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[GithubOauthCompleteResponse400, LoginResponse]]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
    body: GithubDeviceFlowLoginRequest,
) -> Optional[Union[GithubOauthCompleteResponse400, LoginResponse]]:
    """GithubOauthComplete

    Args:
        body (GithubDeviceFlowLoginRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[GithubOauthCompleteResponse400, LoginResponse]
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    body: GithubDeviceFlowLoginRequest,
) -> Response[Union[GithubOauthCompleteResponse400, LoginResponse]]:
    """GithubOauthComplete

    Args:
        body (GithubDeviceFlowLoginRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[GithubOauthCompleteResponse400, LoginResponse]]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
    body: GithubDeviceFlowLoginRequest,
) -> Optional[Union[GithubOauthCompleteResponse400, LoginResponse]]:
    """GithubOauthComplete

    Args:
        body (GithubDeviceFlowLoginRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[GithubOauthCompleteResponse400, LoginResponse]
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
