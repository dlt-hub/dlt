import os

import pytest

from dlt.common.configuration.container import Container
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.normalizers.utils import explicit_normalizers, import_normalizers
from dlt.common.normalizers.json.relational import DataItemNormalizer as RelationalNormalizer
from dlt.common.normalizers.naming import snake_case, direct
from dlt.common.normalizers.naming.exceptions import InvalidNamingModule, UnknownNamingModule

from tests.common.normalizers.custom_normalizers import (
    DataItemNormalizer as CustomRelationalNormalizer,
)
from tests.utils import preserve_environ


def test_default_normalizers() -> None:
    config = explicit_normalizers()
    assert config["names"] is None
    assert config["json"] is None

    # pass explicit
    config = explicit_normalizers("direct", {"module": "custom"})
    assert config["names"] == "direct"
    assert config["json"] == {"module": "custom"}

    # use environ
    os.environ["SCHEMA__NAMING"] = "direct"
    os.environ["SCHEMA__JSON_NORMALIZER"] = '{"module": "custom"}'
    config = explicit_normalizers()
    assert config["names"] == "direct"
    assert config["json"] == {"module": "custom"}


def test_default_normalizers_with_caps() -> None:
    # gets the naming convention from capabilities
    destination_caps = DestinationCapabilitiesContext.generic_capabilities()
    destination_caps.naming_convention = "direct"
    with Container().injectable_context(destination_caps):
        config = explicit_normalizers()
        assert config["names"] == "direct"


def test_import_normalizers() -> None:
    config, naming, json_normalizer = import_normalizers(explicit_normalizers())
    assert isinstance(naming, snake_case.NamingConvention)
    # no maximum length: we do not know the destination capabilities
    assert naming.max_length is None
    assert json_normalizer is RelationalNormalizer
    assert config["names"] == "snake_case"
    assert config["json"] == {"module": "dlt.common.normalizers.json.relational"}

    os.environ["SCHEMA__NAMING"] = "direct"
    os.environ["SCHEMA__JSON_NORMALIZER"] = (
        '{"module": "tests.common.normalizers.custom_normalizers"}'
    )
    config, naming, json_normalizer = import_normalizers(explicit_normalizers())
    assert config["names"] == "direct"
    assert config["json"] == {"module": "tests.common.normalizers.custom_normalizers"}
    assert isinstance(naming, direct.NamingConvention)
    assert naming.max_length is None
    assert json_normalizer is CustomRelationalNormalizer


@pytest.mark.parametrize("sections", ("", "SOURCES__", "SOURCES__TEST_SCHEMA__"))
def test_config_sections(sections: str) -> None:
    os.environ[f"{sections}SCHEMA__NAMING"] = "direct"
    os.environ[f"{sections}SCHEMA__JSON_NORMALIZER"] = (
        '{"module": "tests.common.normalizers.custom_normalizers"}'
    )
    config, _, _ = import_normalizers(explicit_normalizers(schema_name="test_schema"))
    assert config["names"] == "direct"
    assert config["json"] == {"module": "tests.common.normalizers.custom_normalizers"}


def test_import_normalizers_with_caps() -> None:
    # gets the naming convention from capabilities
    destination_caps = DestinationCapabilitiesContext.generic_capabilities()
    destination_caps.naming_convention = "direct"
    destination_caps.max_identifier_length = 127
    with Container().injectable_context(destination_caps):
        _, naming, _ = import_normalizers(explicit_normalizers())
        assert isinstance(naming, direct.NamingConvention)
        assert naming.max_length == 127


def test_import_invalid_naming_module() -> None:
    with pytest.raises(UnknownNamingModule) as py_ex:
        import_normalizers(explicit_normalizers("unknown"))
    assert py_ex.value.naming_module == "unknown"
    with pytest.raises(UnknownNamingModule) as py_ex:
        import_normalizers(explicit_normalizers("dlt.common.tests"))
    assert py_ex.value.naming_module == "dlt.common.tests"
    with pytest.raises(InvalidNamingModule) as py_ex2:
        import_normalizers(explicit_normalizers("dlt.pipeline.helpers"))
    assert py_ex2.value.naming_module == "dlt.pipeline"
    assert py_ex2.value.naming_class == "helpers"
