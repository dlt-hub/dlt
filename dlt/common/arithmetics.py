import decimal # noqa: I251
from contextlib import contextmanager
from typing import Iterator
from decimal import ROUND_HALF_UP, Decimal, DefaultContext, DivisionByZero, InvalidOperation, localcontext, Context, ConversionSyntax  # noqa: I251


DefaultContext.rounding = ROUND_HALF_UP
# use small caps for exponent
DefaultContext.capitals = 0
# prevent NaN to be returned
DefaultContext.traps[InvalidOperation] = True
# prevent Inf to be returned
DefaultContext.traps[DivisionByZero] = True
decimal.setcontext(DefaultContext)

DEFAULT_NUMERIC_PRECISION = 38
DEFAULT_NUMERIC_SCALE = 9

NUMERIC_DEFAULT_QUANTIZER = Decimal("1." + "0" * DEFAULT_NUMERIC_SCALE)


@contextmanager
def numeric_default_context() -> Iterator[Context]:
    with localcontext() as c:
        c.prec=DEFAULT_NUMERIC_PRECISION
        yield c


def numeric_default_quantize(v: Decimal) -> Decimal:
    if v == 0:
        return v
    return v.quantize(NUMERIC_DEFAULT_QUANTIZER)
