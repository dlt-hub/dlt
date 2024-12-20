from typing import TYPE_CHECKING

from dlt.feature.impl import SimpleFeature
from dlt.feature.reference import SupportsSimpleFeature


# extend interface
class SupportsExtendedFeature(SupportsSimpleFeature):
    # add new method
    def more_calculation(self, a: int, b: int) -> int: ...


# extend implementation
class ExtendedFeature(SupportsExtendedFeature, SimpleFeature):
    # override a method
    def calculate(self, a: int, b: int) -> int:
        return a * b

    # add a new method
    def more_calculation(self, a: int, b: int) -> int:
        return a**b
