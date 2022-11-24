from typing import Any, Iterable, Iterator, NamedTuple, Tuple
import tomlkit
from tomlkit.items import Table as TOMLTable

from dlt.common.configuration.resolve import extract_inner_hint
from dlt.common.configuration.specs.base_configuration import BaseConfiguration, is_base_configuration_hint
from dlt.common.typing import AnyType, is_final_type, is_optional_type


class WritableConfigValue(NamedTuple):
    name: Any
    hint: AnyType
    # default_value: Any
    namespaces: Tuple[str, ...]


def write_value(toml_table: TOMLTable, name: str, hint: AnyType, default_value: Any = None) -> None:
    if is_final_type(hint) or is_optional_type(hint):
        # do not dump final fields
        return
    # get the inner hint to generate cool examples
    hint = extract_inner_hint(hint)

    if is_base_configuration_hint(hint):
        inner_table = tomlkit.table(False)
        toml_table[name] = inner_table
        write_spec(inner_table, hint())
    else:

        if default_value is None:
            toml_table[name] = name
            toml_table[name].comment("please set me up!")
        else:
            toml_table[name] = default_value


def write_spec(toml_table: TOMLTable, config: BaseConfiguration) -> None:
    for name, hint in config.get_resolvable_fields().items():
        default_value = getattr(config, name, None)
        write_value(toml_table, name, hint, default_value)


def write_values(toml: tomlkit.TOMLDocument, values: Iterable[WritableConfigValue]) -> None:
    for value in values:
        toml_table: TOMLTable = toml  # type: ignore
        for namespace in value.namespaces:
            if namespace not in toml_table:
                inner_table = tomlkit.table(True)
                toml_table[namespace] = inner_table
                toml_table = inner_table
            else:
                toml_table = toml_table[namespace]  # type: ignore

        write_value(toml_table, value.name, value.hint)
