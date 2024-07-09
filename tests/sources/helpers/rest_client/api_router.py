import re
from typing import NamedTuple, Callable, Pattern, List, Union, TYPE_CHECKING, Dict, Any

import requests_mock

from dlt.common import json

if TYPE_CHECKING:
    RequestCallback = Callable[
        [requests_mock.Request, requests_mock.Context], Union[str, Dict[str, Any], List[Any]]
    ]
    ResponseSerializer = Callable[[requests_mock.Request, requests_mock.Context], str]
else:
    RequestCallback = Callable
    ResponseSerializer = Callable


class Route(NamedTuple):
    method: str
    pattern: Pattern[str]
    callback: ResponseSerializer


class APIRouter:
    def __init__(self, base_url: str):
        self.routes: List[Route] = []
        self.base_url = base_url

    def _add_route(self, method: str, pattern: str, func: RequestCallback) -> RequestCallback:
        compiled_pattern = re.compile(f"{self.base_url}{pattern}")

        def serialize_response(request, context):
            result = func(request, context)

            if isinstance(result, dict) or isinstance(result, list):
                return json.dumps(result)

            return result

        self.routes.append(Route(method, compiled_pattern, serialize_response))
        return serialize_response

    def get(self, pattern: str) -> Callable[[RequestCallback], RequestCallback]:
        def decorator(func: RequestCallback) -> RequestCallback:
            return self._add_route("GET", pattern, func)

        return decorator

    def post(self, pattern: str) -> Callable[[RequestCallback], RequestCallback]:
        def decorator(func: RequestCallback) -> RequestCallback:
            return self._add_route("POST", pattern, func)

        return decorator

    def register_routes(self, mocker: requests_mock.Mocker) -> None:
        for route in self.routes:
            mocker.register_uri(
                route.method,
                route.pattern,
                text=route.callback,
            )
