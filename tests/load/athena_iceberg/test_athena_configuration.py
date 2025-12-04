from dlt.common.typing import StrAny
from dlt.destinations.impl.athena.configuration import DEFAULT_AWS_DATA_CATALOG

from tests.load.utils import S3_TABLES_CATALOG, cm_yield_client


def test_catalog_name() -> None:
    # use defaults for both aws_data_catalog and staging_aws_data_catalog
    config: StrAny = {}
    with cm_yield_client("athena", "dummy_dataset", config) as client:
        assert client.sql_client.catalog_name(quote=False) == DEFAULT_AWS_DATA_CATALOG
        with client.sql_client.with_staging_dataset():
            assert client.sql_client.catalog_name(quote=False) == DEFAULT_AWS_DATA_CATALOG

    # set aws_data_catalog to S3 Tables Catalog
    config = {"aws_data_catalog": S3_TABLES_CATALOG}
    with cm_yield_client("athena", "dummy_dataset", config) as client:
        assert client.sql_client.catalog_name(quote=False) == S3_TABLES_CATALOG
        with client.sql_client.with_staging_dataset():
            # staging catalog cannot be S3 Tables Catalog, should fallback to default
            assert client.sql_client.catalog_name(quote=False) == DEFAULT_AWS_DATA_CATALOG

    # set aws_data_catalog to non-S3 Tables Catalog
    config = {"aws_data_catalog": "dummycatalog"}
    with cm_yield_client("athena", "dummy_dataset", config) as client:
        assert client.sql_client.catalog_name(quote=False) == "dummycatalog"
        with client.sql_client.with_staging_dataset():
            assert client.sql_client.catalog_name(quote=False) == "dummycatalog"

    # set staging_aws_data_catalog to non-S3 Tables Catalog
    config = {"staging_aws_data_catalog": "dummycatalog"}
    with cm_yield_client("athena", "dummy_dataset", config) as client:
        assert client.sql_client.catalog_name(quote=False) == DEFAULT_AWS_DATA_CATALOG
        with client.sql_client.with_staging_dataset():
            assert client.sql_client.catalog_name(quote=False) == "dummycatalog"
