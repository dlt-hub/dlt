from dlt.common.arithmetics import localcontext
from dlt.common.wei import Wei, Decimal


def test_init() -> None:
    assert Wei(1) == 1
    assert Wei(10**18, scale=18) == 1
    assert type(Wei(1)) is Wei


def test_wei_int() -> None:
    assert Wei(1) == 1
    assert Wei(2) > 1
    x = Wei(10)
    print(x)
    d = {"a": Wei(20)}
    print(d["a"].normalize())

    class WeiContainer:
        W = Wei(40)
        def __init__(self) -> None:
            self.w = Wei(20)

    print(WeiContainer().w)
    print(WeiContainer().W)
    print(WeiContainer().w.normalize())
    # assert Wei(10)**18 == 10**18
