import inspect
from importlib import import_module
from typing import Any, Type, Tuple, Union, cast, List

import dlt
from dlt.common.configuration.inject import with_config
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.normalizers.configuration import NormalizersConfiguration
from dlt.common.normalizers.json import SupportsDataItemNormalizer, DataItemNormalizer
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.normalizers.naming.exceptions import UnknownNamingModule, InvalidNamingModule
from dlt.common.normalizers.typing import TJSONNormalizer, TNormalizersConfig
from dlt.common.utils import uniq_id_base64, many_uniq_ids_base64

DEFAULT_NAMING_MODULE = "dlt.common.normalizers.naming.snake_case"
DLT_ID_LENGTH_BYTES = 10


@with_config(spec=NormalizersConfiguration)
def explicit_normalizers(
    naming: Union[str, NamingConvention] = dlt.config.value,
    json_normalizer: TJSONNormalizer = dlt.config.value,
    allow_identifier_change_on_table_with_data: bool = None,
) -> TNormalizersConfig:
    """Gets explicitly configured normalizers - via config or destination caps. May return None as naming or normalizer"""
    norm_conf: TNormalizersConfig = {"names": naming, "json": json_normalizer}
    if allow_identifier_change_on_table_with_data is not None:
        norm_conf["allow_identifier_change_on_table_with_data"] = (
            allow_identifier_change_on_table_with_data
        )
    return norm_conf


@with_config
def import_normalizers(
    normalizers_config: TNormalizersConfig,
    destination_capabilities: DestinationCapabilitiesContext = None,
) -> Tuple[TNormalizersConfig, NamingConvention, Type[DataItemNormalizer[Any]]]:
    """Imports the normalizers specified in `normalizers_config` or taken from defaults. Returns the updated config and imported modules.

    `destination_capabilities` are used to get max length of the identifier.
    """
    # add defaults to normalizer_config
    normalizers_config["names"] = names = normalizers_config["names"] or "snake_case"
    normalizers_config["json"] = item_normalizer = normalizers_config.get("json") or {
        "module": "dlt.common.normalizers.json.relational"
    }
    json_module = cast(SupportsDataItemNormalizer, import_module(item_normalizer["module"]))

    return (
        normalizers_config,
        naming_from_reference(names, destination_capabilities),
        json_module.DataItemNormalizer,
    )


def naming_from_reference(
    names: Union[str, NamingConvention],
    destination_capabilities: DestinationCapabilitiesContext = None,
) -> NamingConvention:
    """Resolves naming convention from reference in `names` and applies max length from `destination_capabilities`

    Reference may be: (1) actual instance of NamingConvention (2) shorthand name pointing to `dlt.common.normalizers.naming` namespace
    (3) a type name which is a module containing `NamingConvention` attribute (4) a type of class deriving from NamingConvention
    """

    # try:
    #     if "." in names:
    #         # TODO: bump schema engine version and migrate schema. also change the name in  TNormalizersConfig from names to naming
    #         if names == "dlt.common.normalizers.names.snake_case":
    #             names = DEFAULT_NAMING_MODULE
    #         # this is full module name
    #         naming_module = cast(SupportsNamingConvention, import_module(names))
    #     else:
    #         # from known location
    #         naming_module = cast(
    #             SupportsNamingConvention, import_module(f"dlt.common.normalizers.naming.{names}")
    #         )
    # except ImportError:
    #     raise UnknownNamingModule(names)
    # if not hasattr(naming_module, "NamingConvention"):
    #     raise InvalidNamingModule(names)
    # # get max identifier length
    # if destination_capabilities:
    #     max_length = min(
    #         destination_capabilities.max_identifier_length,
    #         destination_capabilities.max_column_identifier_length,
    #     )
    # else:
    #     max_length = None

    def _import_naming(module: str, cls: str) -> Type[NamingConvention]:
        if "." in module or cls != "NamingConvention":
            # TODO: bump schema engine version and migrate schema. also change the name in  TNormalizersConfig from names to naming
            if module == "dlt.common.normalizers.names.snake_case":
                module = DEFAULT_NAMING_MODULE
            # this is full module name
            naming_module = import_module(module)
        else:
            # from known location
            naming_module = import_module(f"dlt.common.normalizers.naming.{module}")
        class_ = getattr(naming_module, cls, None)
        if class_ is None:
            raise UnknownNamingModule(module + "." + cls)
        if inspect.isclass(class_) and issubclass(class_, NamingConvention):
            return class_
        raise InvalidNamingModule(module, cls)

    if not isinstance(names, NamingConvention):
        try:
            class_ = _import_naming(names, "NamingConvention")
        except ImportError:
            parts = names.rsplit(".", 1)
            # we have no more options to try
            if len(parts) <= 1:
                raise UnknownNamingModule(names)
            try:
                class_ = _import_naming(*parts)
            except UnknownNamingModule:
                raise
            except ImportError:
                raise UnknownNamingModule(names)

        # get max identifier length
        if destination_capabilities:
            max_length = min(
                destination_capabilities.max_identifier_length,
                destination_capabilities.max_column_identifier_length,
            )
        else:
            max_length = None
        names = class_(max_length)
    return names


def generate_dlt_ids(n_ids: int) -> List[str]:
    return many_uniq_ids_base64(n_ids, DLT_ID_LENGTH_BYTES)


def generate_dlt_id() -> str:
    return uniq_id_base64(DLT_ID_LENGTH_BYTES)
