from prometheus_client import CollectorRegistry

from dlt.common.typing import StrAny
from dlt.dbt_runner.configuration import gen_configuration_variant
from dlt.dbt_runner import runner

from tests.utils import clean_test_storage


def setup_runner(dest_schema_prefix: str, override_values: StrAny = None) -> None:
    clean_test_storage()
    C = gen_configuration_variant(explicit_values=override_values)
    # set unique dest schema prefix by default
    C.dest_schema_prefix = dest_schema_prefix
    C.package_run_params = ["--fail-fast", "--full-refresh"]
    # override values including the defaults above
    if override_values:
        for k,v in override_values.items():
            setattr(C, k, v)
    runner.configure(C, CollectorRegistry(auto_describe=True))
