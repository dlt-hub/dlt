import os
from importlib import import_module
from types import ModuleType
from typing import Any, Dict, Optional, Type, Tuple, cast

import dlt
from dlt.common import logger
from dlt.common import known_env
from dlt.common.configuration.inject import with_config
from dlt.common.configuration.specs import known_sections
from dlt.common.schema.configuration import SchemaConfiguration
from dlt.common.normalizers.exceptions import InvalidJsonNormalizer
from dlt.common.normalizers.json import SupportsDataItemNormalizer, DataItemNormalizer
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.normalizers.naming.exceptions import (
    NamingTypeNotFound,
    UnknownNamingModule,
    InvalidNamingType,
)
from dlt.common.normalizers.typing import (
    TJSONNormalizer,
    TNormalizersConfig,
    TNamingConventionReferenceArg,
)
from dlt.common.typing import is_subclass
from dlt.common.utils import get_full_obj_class_name

DEFAULT_NAMING_NAMESPACE = os.environ.get(
    known_env.DLT_DEFAULT_NAMING_NAMESPACE, "dlt.common.normalizers.naming"
)
DEFAULT_NAMING_MODULE = os.environ.get(known_env.DLT_DEFAULT_NAMING_MODULE, "snake_case")


def _section_for_schema(kwargs: Dict[str, Any]) -> Tuple[str, ...]:
    """Uses the schema name to generate dynamic section normalizer settings"""
    if schema_name := kwargs.get("schema_name"):
        return (known_sections.SOURCES, schema_name)
    else:
        return (known_sections.SOURCES,)


@with_config(spec=SchemaConfiguration, sections=_section_for_schema)  # type: ignore[call-overload]
def configured_normalizers(
    naming: TNamingConventionReferenceArg = dlt.config.value,
    json_normalizer: TJSONNormalizer = dlt.config.value,
    allow_identifier_change_on_table_with_data: bool = None,
    use_break_path_on_normalize: Optional[bool] = None,
    schema_name: Optional[str] = None,
) -> TNormalizersConfig:
    """Gets explicitly onfigured normalizers without any defaults or capabilities injection. If `naming`
    is a module or a type it will get converted into string form via import.

    If `schema_name` is present, a section ("sources", schema_name, "schema") is used to inject the config
    via callable sections=_section_for_schema
    """

    norm_conf: TNormalizersConfig = {"names": serialize_reference(naming), "json": json_normalizer}
    if allow_identifier_change_on_table_with_data is not None:
        norm_conf["allow_identifier_change_on_table_with_data"] = (
            allow_identifier_change_on_table_with_data
        )
    if use_break_path_on_normalize is not None:
        norm_conf["use_break_path_on_normalize"] = use_break_path_on_normalize
    return norm_conf


@with_config
def import_normalizers(
    explicit_normalizers: TNormalizersConfig,
    default_normalizers: TNormalizersConfig = None,
) -> Tuple[TNormalizersConfig, NamingConvention, Type[DataItemNormalizer[Any]]]:
    """Imports the normalizers specified in `normalizers_config` or taken from defaults. Returns the updated config and imported modules.

    `destination_capabilities` are used to get naming convention, max length of the identifier and max nesting level.
    """
    # use container to get destination capabilities, do not use config injection to resolve circular dependencies
    from dlt.common.destination.capabilities import DestinationCapabilitiesContext
    from dlt.common.configuration.container import Container

    destination_capabilities = Container().get(DestinationCapabilitiesContext)
    if default_normalizers is None:
        default_normalizers = {}
    # add defaults to normalizer_config
    naming: Optional[TNamingConventionReferenceArg] = explicit_normalizers.get("names")
    if naming is None:
        if destination_capabilities:
            naming = destination_capabilities.naming_convention
        if naming is None:
            naming = default_normalizers.get("names") or DEFAULT_NAMING_MODULE
    # get max identifier length
    if destination_capabilities:
        max_length = min(
            destination_capabilities.max_identifier_length,
            destination_capabilities.max_column_identifier_length,
        )
    else:
        max_length = None
    naming_convention = naming_from_reference(naming, max_length)
    explicit_normalizers["names"] = serialize_reference(naming)

    item_normalizer = explicit_normalizers.get("json") or default_normalizers.get("json") or {}
    item_normalizer.setdefault("module", "dlt.common.normalizers.json.relational")
    # if max_table_nesting is set, we need to set the max_table_nesting in the json_normalizer
    if destination_capabilities and destination_capabilities.max_table_nesting is not None:
        # TODO: this is a hack, we need a better method to do this
        from dlt.common.normalizers.json.relational import DataItemNormalizer

        try:
            DataItemNormalizer.ensure_this_normalizer(item_normalizer)
            item_normalizer.setdefault("config", {})
            item_normalizer["config"]["max_nesting"] = destination_capabilities.max_table_nesting  # type: ignore[index]
        except InvalidJsonNormalizer:
            # not a right normalizer
            logger.warning(f"JSON Normalizer {item_normalizer} does not support max_nesting")
            pass
    json_module = cast(SupportsDataItemNormalizer, import_module(item_normalizer["module"]))
    explicit_normalizers["json"] = item_normalizer
    return (
        explicit_normalizers,
        naming_convention,
        json_module.DataItemNormalizer,
    )


def naming_from_reference(
    names: TNamingConventionReferenceArg,
    max_length: Optional[int] = None,
) -> NamingConvention:
    """Resolves naming convention from reference in `names` and applies max length if specified

    Reference may be: (1) shorthand name pointing to `dlt.common.normalizers.naming` namespace
    (2) a type name which is a module containing `NamingConvention` attribute (3) a type of class deriving from NamingConvention
    """

    def _import_naming(module: str) -> ModuleType:
        if "." in module:
            # TODO: bump schema engine version and migrate schema. also change the name in  TNormalizersConfig from names to naming
            if module == "dlt.common.normalizers.names.snake_case":
                module = f"{DEFAULT_NAMING_NAMESPACE}.{DEFAULT_NAMING_MODULE}"
            # this is full module name
            naming_module = import_module(module)
        else:
            # from known location
            try:
                naming_module = import_module(f"{DEFAULT_NAMING_NAMESPACE}.{module}")
            except ImportError:
                # also import local module
                naming_module = import_module(module)
        return naming_module

    def _get_type(naming_module: ModuleType, cls: str) -> Type[NamingConvention]:
        class_: Type[NamingConvention] = getattr(naming_module, cls, None)
        if class_ is None:
            raise NamingTypeNotFound(naming_module.__name__, cls)
        if is_subclass(class_, NamingConvention):
            return class_
        raise InvalidNamingType(naming_module.__name__, cls)

    if is_subclass(names, NamingConvention):
        class_: Type[NamingConvention] = names  # type: ignore[assignment]
    elif isinstance(names, ModuleType):
        class_ = _get_type(names, "NamingConvention")
    elif isinstance(names, str):
        try:
            class_ = _get_type(_import_naming(names), "NamingConvention")
        except ImportError:
            parts = names.rsplit(".", 1)
            # we have no more options to try
            if len(parts) <= 1:
                raise UnknownNamingModule(names)
            try:
                class_ = _get_type(_import_naming(parts[0]), parts[1])
            except UnknownNamingModule:
                raise
            except ImportError:
                raise UnknownNamingModule(names)
    else:
        raise ValueError(names)

    return class_(max_length)


def serialize_reference(naming: Optional[TNamingConventionReferenceArg]) -> Optional[str]:
    """Serializes generic `naming` reference to importable string."""
    if naming is None:
        return naming
    if isinstance(naming, str):
        return naming
    # import reference and use naming to get valid path to type
    return get_full_obj_class_name(naming_from_reference(naming))
