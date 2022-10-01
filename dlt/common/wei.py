from typing import Union

from dlt.common.typing import TVariantRV, SupportsVariant
from dlt.common.arithmetics import default_context, decimal, Decimal

# default scale of EVM based blockchain
WEI_SCALE = 18
# log(2^256) + 1
EVM_DECIMAL_PRECISION = 78
# value of one at wei scale
WEI_SCALE_POW = 10**18


class Wei(Decimal,SupportsVariant[Decimal]):

    ctx = default_context(decimal.getcontext().copy(), EVM_DECIMAL_PRECISION)

    @classmethod
    def from_int256(cls, value: int, decimals: int = 0) -> "Wei":
        d: "Wei" = None
        with decimal.localcontext(Wei.ctx):
            if decimals == 0:
                d = cls(value)
            else:
                d = cls(Decimal(value) / 10**decimals)

        return d

    def __call__(self) -> Union["Wei", TVariantRV]:
        # TODO: this should look into DestinationCapabilitiesContext to get maximum Decimal value.
        # this is BigQuery BIGDECIMAL max
        if self > 578960446186580977117854925043439539266 or self < -578960446186580977117854925043439539267:
            return  ("str", str(self))
        else:
            return self


    def __repr__(self) -> str:
        return f"Wei('{str(self)}')"
