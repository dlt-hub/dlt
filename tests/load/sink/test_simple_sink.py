import dlt
from dlt.common.typing import TDataItems


def test_datasink() -> None:
    @dlt.sink()
    def test_sink(items: TDataItems) -> None:
        print("CALL")
        print(items)

    p = dlt.pipeline("sink_test", destination=test_sink)

    p.run([{"a": "b "}], table_name="items")
    assert False
