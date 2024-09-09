import os
import pytest

from dlt.common.configuration.container import Container
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.normalizers.typing import TNormalizersConfig
from dlt.common.normalizers.json.relational import DataItemNormalizer as RelationalNormalizer
from dlt.common.normalizers.naming import snake_case, direct
from dlt.common.normalizers.naming.exceptions import (
    InvalidNamingType,
    NamingTypeNotFound,
    UnknownNamingModule,
)
from tests.common.normalizers.custom_normalizers import (
    DataItemNormalizer as CustomRelationalNormalizer,
)
from dlt.common.schema.normalizers import (
    DEFAULT_NAMING_NAMESPACE,
    explicit_normalizers,
    import_normalizers,
    naming_from_reference,
    serialize_reference,
)

from tests.utils import preserve_environ


def test_explicit_normalizers() -> None:
    config = explicit_normalizers()
    assert config["names"] is None
    assert config["json"] is None

    # pass explicit
    config = explicit_normalizers("direct", {"module": "custom"})
    assert config["names"] == "direct"
    assert config["json"] == {"module": "custom"}

    # pass modules and types, make sure normalizer config is serialized
    config = explicit_normalizers(direct)
    assert config["names"] == f"{DEFAULT_NAMING_NAMESPACE}.direct.NamingConvention"
    config = explicit_normalizers(direct.NamingConvention)
    assert config["names"] == f"{DEFAULT_NAMING_NAMESPACE}.direct.NamingConvention"

    # use environ
    os.environ["SCHEMA__NAMING"] = "direct"
    os.environ["SCHEMA__JSON_NORMALIZER"] = '{"module": "custom"}'
    config = explicit_normalizers()
    assert config["names"] == "direct"
    assert config["json"] == {"module": "custom"}


def test_explicit_normalizers_caps_ignored() -> None:
    # gets the naming convention from capabilities
    destination_caps = DestinationCapabilitiesContext.generic_capabilities()
    destination_caps.naming_convention = "direct"
    with Container().injectable_context(destination_caps):
        config = explicit_normalizers()
        assert config["names"] is None


def test_serialize_reference() -> None:
    assert serialize_reference(None) is None
    assert serialize_reference("module") == "module"
    assert (
        serialize_reference(snake_case) == f"{DEFAULT_NAMING_NAMESPACE}.snake_case.NamingConvention"
    )
    assert (
        serialize_reference(snake_case.NamingConvention)
        == f"{DEFAULT_NAMING_NAMESPACE}.snake_case.NamingConvention"
    )
    # test a wrong module and type
    with pytest.raises(NamingTypeNotFound):
        serialize_reference(pytest)
    with pytest.raises(ValueError):
        serialize_reference(Container)  # type: ignore[arg-type]


def test_naming_from_reference() -> None:
    assert naming_from_reference("snake_case").name() == "snake_case"
    assert naming_from_reference("snake_case.NamingConvention").name() == "snake_case"

    # now not visible
    with pytest.raises(UnknownNamingModule):
        naming_from_reference("custom_normalizers")

    # temporarily add current file dir to paths and import module that clash with dlt predefined (no path)
    import sys

    try:
        from tests.common.normalizers import custom_normalizers

        sys.path.insert(0, os.path.dirname(custom_normalizers.__file__))
        assert naming_from_reference("custom_normalizers").name() == "custom_normalizers"
        assert (
            naming_from_reference("custom_normalizers.NamingConvention").name()
            == "custom_normalizers"
        )
        assert (
            naming_from_reference("custom_normalizers.ColumnNamingConvention").name()
            == "custom_normalizers"
        )
    finally:
        sys.path.pop(0)

    # non standard location
    assert (
        naming_from_reference("dlt.destinations.impl.weaviate.naming").name()
        == "dlt.destinations.impl.weaviate.naming"
    )

    # import module
    assert naming_from_reference(snake_case).name() == "snake_case"
    assert naming_from_reference(snake_case.NamingConvention).name() == "snake_case"

    with pytest.raises(ValueError):
        naming_from_reference(snake_case.NamingConvention())  # type: ignore[arg-type]

    # with max length
    naming = naming_from_reference(snake_case.NamingConvention, 120)
    assert naming.max_length == 120


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


def test_import_normalizers_with_defaults() -> None:
    explicit = explicit_normalizers()
    default_: TNormalizersConfig = {
        "names": "dlt.destinations.impl.weaviate.naming",
        "json": {"module": "tests.common.normalizers.custom_normalizers"},
    }
    config, naming, json_normalizer = import_normalizers(explicit, default_)

    assert config["names"] == "dlt.destinations.impl.weaviate.naming"
    assert config["json"] == {"module": "tests.common.normalizers.custom_normalizers"}
    assert naming.name() == "dlt.destinations.impl.weaviate.naming"
    assert json_normalizer is CustomRelationalNormalizer

    # correctly overrides
    explicit["names"] = "sql_cs_v1"
    explicit["json"] = {"module": "dlt.common.normalizers.json.relational"}
    config, naming, json_normalizer = import_normalizers(explicit, default_)
    assert config["names"] == "sql_cs_v1"
    assert config["json"] == {"module": "dlt.common.normalizers.json.relational"}
    assert naming.name() == "sql_cs_v1"
    assert json_normalizer is RelationalNormalizer


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

        _, naming, _ = import_normalizers(explicit_normalizers(snake_case))
        assert isinstance(naming, snake_case.NamingConvention)
        assert naming.max_length == 127

    # max table nesting generates relational normalizer
    default_: TNormalizersConfig = {
        "names": "dlt.destinations.impl.weaviate.naming",
        "json": {"module": "tests.common.normalizers.custom_normalizers"},
    }
    destination_caps.max_table_nesting = 0
    with Container().injectable_context(destination_caps):
        config, _, relational = import_normalizers(explicit_normalizers())
        assert config["json"]["config"]["max_nesting"] == 0
        assert relational is RelationalNormalizer

        # wrong normalizer
        config, _, relational = import_normalizers(explicit_normalizers(), default_)
        assert "config" not in config["json"]


def test_import_invalid_naming_module() -> None:
    with pytest.raises(UnknownNamingModule) as py_ex:
        import_normalizers(explicit_normalizers("unknown"))
    assert py_ex.value.naming_module == "unknown"
    with pytest.raises(UnknownNamingModule) as py_ex:
        import_normalizers(explicit_normalizers("dlt.common.tests"))
    assert py_ex.value.naming_module == "dlt.common.tests"
    with pytest.raises(InvalidNamingType) as py_ex2:
        import_normalizers(explicit_normalizers("dlt.pipeline.helpers"))
    assert py_ex2.value.naming_module == "dlt.pipeline"
    assert py_ex2.value.naming_class == "helpers"
