import decimal # noqa: I251
from contextlib import contextmanager
from typing import Iterator
from decimal import ROUND_HALF_UP, Decimal, Inexact, DivisionByZero, DefaultContext, InvalidOperation, localcontext, Context, Subnormal, ConversionSyntax  # noqa: I251


DEFAULT_NUMERIC_PRECISION = 38
DEFAULT_NUMERIC_SCALE = 9
NUMERIC_DEFAULT_QUANTIZER = Decimal("1." + "0" * DEFAULT_NUMERIC_SCALE)


def default_context(c: Context, precision: int) -> Context:
    c.rounding = ROUND_HALF_UP
    # prevent NaN to be returned
    c.traps[InvalidOperation] = True
    # prevent Inf to be returned
    c.traps[DivisionByZero] = True
    # force exact operations - prevents unknown rounding
    c.traps[Inexact] = True
    c.traps[Subnormal] = True
    # use 128 bit precision which is default in most databases (DEFAULT_NUMERIC_PRECISION)
    c.prec = precision

    return c


@contextmanager
def numeric_default_context(precision: int = DEFAULT_NUMERIC_PRECISION) -> Iterator[Context]:
    with localcontext() as c:
        yield default_context(c, precision)


def numeric_default_quantize(v: Decimal) -> Decimal:
    if v == 0:
        return v
    c = decimal.getcontext().copy()
    c.traps[Inexact] = False
    return v.quantize(NUMERIC_DEFAULT_QUANTIZER, context=c)
