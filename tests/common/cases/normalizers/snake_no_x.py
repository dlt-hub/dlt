from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCaseNamingConvention


class NamingConvention(SnakeCaseNamingConvention):
    def normalize_identifier(self, identifier: str) -> str:
        identifier = super().normalize_identifier(identifier)
        if identifier.endswith("x"):
            print(identifier[:-1] + "_")
            return identifier[:-1] + "_"
        return identifier
