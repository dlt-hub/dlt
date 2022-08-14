import functools
import hexbytes
from typing import Any

from dlt.common import Decimal
from dlt.common.arithmetics import Context, default_context, decimal
from dlt.common.typing import StrAny

# default scale of platform contracts
WEI_SCALE = 18
# log(2^256) + 1
EVM_DECIMAL_PRECISION = 78
# value of one at wei scale
WEI_SCALE_POW = 10**18


# class WeiDecimal(Decimal):
#     ctx = default_context(decimal.getcontext().copy(), EVM_DECIMAL_PRECISION)
#     def __new__(cls, *args: Any, **kwargs: Any) -> "Wei":
#         c = default_context(decimal.getcontext().copy(), EVM_DECIMAL_PRECISION)
#         d = super(WeiDecimal, cls).__new__(cls, *args, **kwargs, context=c)
#         d.c = c
#         # d.c = default_context(decimal.getcontext().copy(), EVM_DECIMAL_PRECISION)
#         return d

#     def __getattribute__(self, __name: str) -> Any:
#         rv = super().__getattribute__(__name)
#         if callable(rv) and not __name.startswith("_"):
#             if "context" in rv.func_code.co_varnames:
#                 return functools.partialmethod(rv, context=self.c)
#         return rv

#     def __repr__(self) -> str:
#         return super().__repr__().replace("Decimal", "Wei")


class Wei(Decimal):

    ctx = default_context(decimal.getcontext().copy(), EVM_DECIMAL_PRECISION)

    # def __slots__hints__(self) -> None:
    #     self._scale: int = 0

    # def __new__(cls, value: int, scale: int = 0) -> "Wei":
    #     self = super(Wei, cls).__new__(cls, value)
    #     self._scale = scale
    #     return self

    # def __init__(self, value: int, scale: int = 0) -> None:
    #     self._c = default_context(decimal.getcontext().copy(), EVM_DECIMAL_PRECISION)
    #     self.value = value
    #     self.scale = scale


    # def to_decimal():
    #     pass

    # def __get__(self, obj, type=None) -> object:
    #     print("GET!")
    #     if self.normalize(self.ctx) > 100:
    #         return "STR"
    #     else:
    #         return self

    def __new__(cls, value: int, scale: int = 0) -> "Wei":
        d: "Wei" = None
        if scale == 0:
            d = super(Wei, cls).__new__(cls, value)
        else:
            d = super(Wei, cls).__new__(cls, Decimal(value, context=cls.ctx) / 10**scale)

        return d

    # def from_uint256(value: int, scale: int = 0):
    #     pass

    # # def __repr__(self) -> str:
    # #     return f"{self.scale},{self.value}"
