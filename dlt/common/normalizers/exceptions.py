from dlt.common.exceptions import DltException


class NormalizerException(DltException):
    pass


class InvalidJsonNormalizer(NormalizerException):
    def __init__(self, required_normalizer: str, present_normalizer: str) -> None:
        self.required_normalizer = required_normalizer
        self.present_normalizer = present_normalizer
        super().__init__(
            f"Operation requires {required_normalizer} normalizer while"
            f" {present_normalizer} normalizer is present"
        )
