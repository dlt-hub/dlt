from typing import (
    List,
    Dict,
    Union,
    Literal,
    Callable,
    Any,
)
from requests import Response


HTTPMethodBasic = Literal["GET", "POST"]
HTTPMethodExtended = Literal["PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]
HTTPMethod = Union[HTTPMethodBasic, HTTPMethodExtended]
HookFunction = Callable[[Response, Any, Any], None]
HookEvent = Union[HookFunction, List[HookFunction]]
Hooks = Dict[str, HookEvent]
