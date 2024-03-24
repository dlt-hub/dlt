from typing import (
    Union,
    Literal,
)


HTTPMethodBasic = Literal["GET", "POST"]
HTTPMethodExtended = Literal["PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]
HTTPMethod = Union[HTTPMethodBasic, HTTPMethodExtended]
