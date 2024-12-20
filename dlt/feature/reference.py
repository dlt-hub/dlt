from typing import Protocol


class SupportsSimpleFeature(Protocol):
    def calculate(self, a: int, b: int) -> int: ...
