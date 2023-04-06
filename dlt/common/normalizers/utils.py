from importlib import import_module
from typing import Any, Type, Tuple, cast

import dlt
from dlt.common.configuration.inject import with_config
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.normalizers.configuration import NormalizersConfiguration
from dlt.common.normalizers.json import SupportsDataItemNormalizer, DataItemNormalizer
from dlt.common.normalizers.naming import NamingConvention, SupportsNamingConvention
from dlt.common.normalizers.naming.exceptions import UnknownNamingModule, InvalidNamingModule
from dlt.common.normalizers.typing import TJSONNormalizer, TNormalizersConfig

DEFAULT_NAMING_MODULE = "dlt.common.normalizers.naming.snake_case"


@with_config(spec=NormalizersConfiguration)
def default_normalizers(
    naming: str = dlt.config.value ,
    json_normalizer: TJSONNormalizer = dlt.config.value
) -> TNormalizersConfig:
    return {"names": naming, "json": json_normalizer}


@with_config
def import_normalizers(
    normalizers_config: TNormalizersConfig,
    destination_capabilities: DestinationCapabilitiesContext = None
) -> Tuple[NamingConvention, Type[DataItemNormalizer[Any]]]:
    names = normalizers_config["names"]
    try:
        if "." in names:
            # TODO: bump schema engine version and migrate schema. also change the name in  TNormalizersConfig from names to naming
            if names == "dlt.common.normalizers.names.snake_case":
                names = DEFAULT_NAMING_MODULE
            # this is full module name
            naming_module = cast(SupportsNamingConvention, import_module(names))
        else:
            # from known location
            naming_module = cast(SupportsNamingConvention, import_module(f"dlt.common.normalizers.naming.{names}"))
    except ImportError:
        raise UnknownNamingModule(names)
    if not hasattr(naming_module, "NamingConvention"):
        raise InvalidNamingModule(names)
    # get max identifier length
    if destination_capabilities:
        max_length = min(destination_capabilities.max_identifier_length, destination_capabilities.max_column_identifier_length)
    else:
        max_length = None
    json_module = cast(SupportsDataItemNormalizer, import_module(normalizers_config["json"]["module"]))

    return naming_module.NamingConvention(max_length), json_module.DataItemNormalizer
