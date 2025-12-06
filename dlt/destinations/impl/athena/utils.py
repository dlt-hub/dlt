S3_TABLES_CATALOG_PREFIX = "s3tablescatalog/"


def is_s3_tables_catalog(catalog_name: str) -> bool:
    return catalog_name.startswith(S3_TABLES_CATALOG_PREFIX)
