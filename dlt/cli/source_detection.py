import ast
import inspect
from typing import Dict, Tuple, Set, List

from dlt.common.configuration import is_secret_hint
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.reflection.utils import creates_func_def_name_node
from dlt.common.typing import is_optional_type

from dlt.sources import SourceReference
from dlt.cli.config_toml_writer import WritableConfigValue
from dlt.cli.exceptions import CliCommandInnerException
from dlt.reflection.script_visitor import PipelineScriptVisitor


def find_call_arguments_to_replace(
    visitor: PipelineScriptVisitor, replace_nodes: List[Tuple[str, str]], init_script_name: str
) -> List[Tuple[ast.AST, ast.AST]]:
    # the input tuple (call argument name, replacement value)
    # the returned tuple (node, replacement value, node type)
    transformed_nodes: List[Tuple[ast.AST, ast.AST]] = []
    replaced_args: Set[str] = set()
    known_calls: Dict[str, List[inspect.BoundArguments]] = visitor.known_calls
    for arg_name, calls in known_calls.items():
        for args in calls:
            for t_arg_name, t_value in replace_nodes:
                dn_node: ast.AST = args.arguments.get(t_arg_name)
                if dn_node is not None:
                    if not isinstance(dn_node, ast.Constant) or not isinstance(dn_node.value, str):
                        raise CliCommandInnerException(
                            "init",
                            f"The pipeline script {init_script_name} must pass the {t_arg_name} as string to '{arg_name}' function in line {dn_node.lineno}",  # type: ignore[attr-defined]
                        )
                    else:
                        transformed_nodes.append((dn_node, ast.Constant(value=t_value, kind=None)))
                        replaced_args.add(t_arg_name)

    # there was at least one replacement
    for t_arg_name, _ in replace_nodes:
        if t_arg_name not in replaced_args:
            raise CliCommandInnerException(
                "init",
                f"The pipeline script {init_script_name} is not explicitly passing the"
                f" '{t_arg_name}' argument to 'pipeline' or 'run' function. In init script the"
                " default and configured values are not accepted.",
            )
    return transformed_nodes


def find_source_calls_to_replace(
    visitor: PipelineScriptVisitor, pipeline_name: str
) -> List[Tuple[ast.AST, ast.AST]]:
    transformed_nodes: List[Tuple[ast.AST, ast.AST]] = []
    for source_def in visitor.known_sources_resources.values():
        # append function name to be replaced
        transformed_nodes.append(
            (
                creates_func_def_name_node(source_def, visitor.source_lines),
                ast.Name(id=pipeline_name + "_" + source_def.name),
            )
        )

    for calls in visitor.known_sources_resources_calls.values():
        for call in calls:
            transformed_nodes.append(
                (call.func, ast.Name(id=pipeline_name + "_" + ast.unparse(call.func)))
            )

    return transformed_nodes


def detect_source_configs(
    sources: Dict[str, SourceReference], module_prefix: str, section: Tuple[str, ...]
) -> Tuple[
    Dict[str, WritableConfigValue], Dict[str, WritableConfigValue], Dict[str, SourceReference]
]:
    """Creates sample secret and configs for `sources` belonging to `module_prefix`. Assumes that
    all sources belong to a single section so only source name is used to create sample layouts"""
    # all detected secrets with sections
    required_secrets: Dict[str, WritableConfigValue] = {}
    # all detected configs with sections
    required_config: Dict[str, WritableConfigValue] = {}
    # all sources checked, indexed by source name
    checked_sources: Dict[str, SourceReference] = {}

    for _, source_info in sources.items():
        # accept only sources declared in the `init` or `pipeline` modules
        if source_info.ref.startswith(module_prefix):
            checked_sources[source_info.name] = source_info
            extract_secrets_and_configs_from_source_reference(
                source_info, required_secrets, required_config, section
            )

    return required_secrets, required_config, checked_sources


def extract_secrets_and_configs_from_source_reference(
    source_info: SourceReference,
    required_secrets: Dict[str, WritableConfigValue],
    required_config: Dict[str, WritableConfigValue],
    section: Tuple[str, ...],
) -> None:
    source_config = source_info.SPEC() if source_info.SPEC else BaseConfiguration()
    spec_fields = source_config.get_resolvable_fields()
    for field_name, field_type in spec_fields.items():
        val_store = None
        # all secrets must go to secrets.toml
        if is_secret_hint(field_type):
            val_store = required_secrets
        # all configs that are required and do not have a default value must go to config.toml
        elif not is_optional_type(field_type) and getattr(source_config, field_name) is None:
            val_store = required_config

        if val_store is not None:
            # we are sure that all sources come from single file so we can put them in single section
            val_store[source_info.name + ":" + field_name] = WritableConfigValue(
                field_name, field_type, None, section
            )
