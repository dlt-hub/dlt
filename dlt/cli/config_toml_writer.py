from typing import Any, NamedTuple, Tuple, Iterable, Mapping
import tomlkit
from tomlkit.items import Table as TOMLTable
from tomlkit.container import Container as TOMLContainer
from collections.abc import Sequence as C_Sequence

from dlt.common.configuration.specs.base_configuration import is_hint_not_resolvable
from dlt.common.configuration.const import TYPE_EXAMPLES
from dlt.common.configuration.specs import (
    BaseConfiguration,
    is_base_configuration_inner_hint,
    extract_inner_hint,
)
from dlt.common.data_types import py_type_to_sc_type
from dlt.common.typing import AnyType, is_optional_type, is_subclass


class WritableConfigValue(NamedTuple):
    name: Any
    hint: AnyType
    default_value: Any
    sections: Tuple[str, ...]


def generate_typed_example(name: str, hint: AnyType) -> Any:
    inner_hint = extract_inner_hint(hint)
    try:
        sc_type = py_type_to_sc_type(inner_hint)
        if sc_type in TYPE_EXAMPLES.keys():
            return TYPE_EXAMPLES[sc_type]
        if sc_type == "bigint":
            return 0
        if sc_type == "double":
            return 1.0
        if sc_type == "bool":
            return True
        if sc_type == "json":
            return "json"
        if sc_type in ("wei", "decimal"):
            return "1.0"
        raise TypeError(sc_type)
    except TypeError:
        return name


def write_value(
    toml_table: TOMLTable,
    name: str,
    hint: AnyType,
    overwrite_existing: bool,
    default_value: Any = None,
    is_default_of_interest: bool = False,
) -> None:
    # skip if table contains the name already
    if name in toml_table and not overwrite_existing:
        return
    # do not dump nor resolvable and optional fields if they are not of special interest
    if (
        is_hint_not_resolvable(hint) or is_optional_type(hint) or default_value is not None
    ) and not is_default_of_interest:
        return
    # get the inner hint to generate cool examples
    hint = extract_inner_hint(hint)
    if is_base_configuration_inner_hint(hint):
        inner_table = tomlkit.table(is_super_table=True)
        write_spec(inner_table, hint(), default_value, overwrite_existing)
        if len(inner_table) > 0:
            toml_table[name] = inner_table
    else:
        if default_value is None:
            example_value = generate_typed_example(name, hint)
            # complex types do not get default values for now (#2531)
            if example_value == "json":
                return
            toml_table[name] = example_value
            # tomlkit not supporting comments on boolean
            if not isinstance(example_value, bool):
                toml_table[name].comment("fill this in!")
        else:
            toml_table[name] = default_value


def write_spec(
    toml_table: TOMLTable,
    config: BaseConfiguration,
    initial_value: Mapping[str, Any],
    overwrite_existing: bool,
) -> None:
    for name, hint in config.get_resolvable_fields().items():
        # use initial value
        initial_ = initial_value.get(name) if initial_value else None
        # use default value stored in config
        default_value = getattr(config, name, None)

        # check if field is of particular interest and should be included if it has default
        is_default_of_interest = name in config.__config_gen_annotations__

        # if initial is different from default, it is of interest as well
        if initial_ is not None:
            is_default_of_interest = is_default_of_interest or (initial_ != default_value)

        write_value(
            toml_table,
            name,
            hint,
            overwrite_existing,
            default_value=initial_ or default_value,
            is_default_of_interest=is_default_of_interest,
        )


def write_values(
    toml: TOMLContainer, values: Iterable[WritableConfigValue], overwrite_existing: bool
) -> None:
    # TODO: decouple writers from a particular object model ie. TOML
    for value in values:
        toml_table: TOMLTable = toml  # type: ignore
        for section in value.sections:
            if section not in toml_table:
                inner_table = tomlkit.table(is_super_table=True)
                toml_table[section] = inner_table
                toml_table = inner_table
            else:
                toml_table = toml_table[section]  # type: ignore

        write_value(
            toml_table,
            value.name,
            value.hint,
            overwrite_existing,
            default_value=value.default_value,
            is_default_of_interest=True,
        )
