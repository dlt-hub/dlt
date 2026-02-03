from typing import cast

from dlt.common.typing import StrAny
from dlt.destinations.impl.athena.configuration import (
    AthenaClientConfiguration,
    DEFAULT_AWS_DATA_CATALOG,
)

from tests.load.utils import S3_TABLES_CATALOG, cm_yield_client


def test_s3_tables_naming_convention_setting() -> None:
    # naming convention should be adjusted to `s3_tables` when using S3 Tables Catalog
    config = {"aws_data_catalog": S3_TABLES_CATALOG}
    with cm_yield_client("athena", "dummy_dataset", config) as client:
        assert client.capabilities.naming_convention == "s3_tables"


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


def test_query_result_bucket_optional() -> None:
    # query_result_bucket is Optional[str] so it should be resolved even when None.
    # this enables users with Athena managed query results to omit it.
    # see: https://github.com/dlt-hub/dlt/issues/3565
    hints = AthenaClientConfiguration.get_resolvable_fields()
    # None must be accepted as resolved for optional fields
    assert AthenaClientConfiguration.is_field_resolved(None, hints["query_result_bucket"])
    # query_result_bucket maps to pyathena's s3_staging_dir in to_connector_params().
    # when None, pyathena omits OutputLocation from the StartQueryExecution request,
    # which is required for workgroups with managed query results.
    with cm_yield_client("athena", "dummy_dataset") as client:
        athena_config = cast(AthenaClientConfiguration, client.config)
        athena_config.query_result_bucket = None
        connector_params = athena_config.to_connector_params()
        assert connector_params["s3_staging_dir"] is None
