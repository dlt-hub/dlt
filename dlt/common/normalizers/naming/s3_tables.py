from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCaseNamingConvention


S3_TABLES_MAX_IDENTIFIER_LENGTH = 255


class NamingConvention(SnakeCaseNamingConvention):
    """Naming convention for S3 Tables.

    Built to comply with the rules listed here: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets-naming.html#naming-rules-table.

    Extends `snake_case` naming convention with additions:
    - enforce max identifier length of 255 characters
    - remove leading underscores from table identifiers

    Note that this S3 Tables rule is currently not checked/enforced by the naming convention:
    - "Namespace names must not start with the reserved prefix `aws`."
    """

    def __init__(self, max_length: int = None) -> None:
        super().__init__(max_length)
        if self.max_length and self.max_length > S3_TABLES_MAX_IDENTIFIER_LENGTH:
            raise ValueError(
                "`max_length` for `s3_tables` naming convention may not exceed"
                f" {S3_TABLES_MAX_IDENTIFIER_LENGTH}"
            )
        elif self.max_length is None:
            self.max_length = S3_TABLES_MAX_IDENTIFIER_LENGTH

    def normalize_table_identifier(self, identifier: str) -> str:
        norm_identifier = super().normalize_table_identifier(identifier)
        norm_identifier = self._remove_leading_underscores(norm_identifier)
        return norm_identifier
