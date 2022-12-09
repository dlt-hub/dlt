import inspect
import os
import ast
import click
import shutil
from astunparse import unparse
from types import ModuleType
from typing import Dict, List, Set, Tuple
from importlib.metadata import version as pkg_version

from dlt.common.configuration import is_secret_hint, DOT_DLT, make_dot_dlt_path
from dlt.common.configuration.providers import CONFIG_TOML, SECRETS_TOML, ConfigTomlProvider, SecretsTomlProvider
from dlt.version import DLT_PKG_NAME, __version__
from dlt.common.normalizers.names.snake_case import normalize_schema_name
from dlt.common.destination import DestinationReference
from dlt.common.reflection.utils import creates_func_def_name_node, rewrite_python_script
from dlt.common.schema.exceptions import InvalidSchemaName
from dlt.common.storages.file_storage import FileStorage
from dlt.common.typing import is_optional_type

from dlt.extract.decorators import _SOURCES
import dlt.reflection.names as n
from dlt.reflection.script_inspector import inspect_pipeline_script
from dlt.reflection.script_visitor import PipelineScriptVisitor

import dlt.cli.echo as fmt
from dlt.cli import utils
from dlt.cli.config_toml_writer import WritableConfigValue, write_values
from dlt.cli.exceptions import CliCommandException

DLT_INIT_DOCS_URL = "https://dlthub.com/docs/command-line-interface#dlt-init"


def _find_call_arguments_to_replace(visitor: PipelineScriptVisitor, replace_nodes: List[Tuple[str, str]], init_script_name: str) -> List[Tuple[ast.AST, ast.AST]]:
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
                        raise CliCommandException("init", f"The pipeline script {init_script_name} must pass the {t_arg_name} as string to '{arg_name}' function in line {dn_node.lineno}")
                    else:
                        transformed_nodes.append((dn_node, ast.Constant(value=t_value, kind=None)))
                        replaced_args.add(t_arg_name)

    # there was at least one replacement
    for t_arg_name, _ in replace_nodes:
        if t_arg_name not in replaced_args:
            raise CliCommandException("init", f"The pipeline script {init_script_name} is not explicitly passing the '{t_arg_name}' argument to 'pipeline' or 'run' function. In init script the default and configured values are not accepted.")
    return transformed_nodes


def _find_source_calls_to_replace(visitor: PipelineScriptVisitor, pipeline_name: str) -> List[Tuple[ast.AST, ast.AST]]:
    transformed_nodes: List[Tuple[ast.AST, ast.AST]] = []
    for source_def in visitor.known_sources_resources.values():
        # append function name to be replaced
        transformed_nodes.append((creates_func_def_name_node(source_def, visitor.source_lines), ast.Name(id=pipeline_name + "_" + source_def.name)))

    for calls in visitor.known_sources_resources_calls.values():
        for call in calls:
            transformed_nodes.append((call.func, ast.Name(id=pipeline_name + "_" + unparse(call.func))))

    return transformed_nodes


def _detect_required_configs(visitor: PipelineScriptVisitor) -> Tuple[Dict[str, WritableConfigValue], Dict[str, WritableConfigValue]]:
    # all detected secrets with namespaces
    required_secrets: Dict[str, WritableConfigValue] = {}
    # all detected configs with namespaces
    required_config: Dict[str, WritableConfigValue] = {}

    # skip sources without spec. those are not imported and most probably are inner functions. also skip the sources that are not called
    # also skip the sources that are called from functions, the parent of call object to the source must be None (no outer function)
    known_imported_sources = {name: _SOURCES[name] for name in visitor.known_sources_resources
        if name in _SOURCES and name in visitor.known_sources_resources_calls and any(call.parent is None for call in visitor.known_sources_resources_calls[name])}  # type: ignore

    for source_name, source_info in known_imported_sources.items():
        source_config = source_info.SPEC()
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
                # we are sure that all resources come from single file so we can put them in single namespace
                # namespaces = () if len(known_imported_sources) == 1 else ("sources", source_name)
                val_store[source_name + ":" + field_name] = WritableConfigValue(field_name, field_type, ())

    return required_secrets, required_config


def _get_template_files(command_module: ModuleType, use_generic_template: bool) -> Tuple[str, List[str]]:
    template_files: List[str] = command_module.TEMPLATE_FILES
    pipeline_script: str = command_module.PIPELINE_SCRIPT
    if use_generic_template:
        pipeline_script, py = os.path.splitext(pipeline_script)
        pipeline_script = f"{pipeline_script}_generic{py}"
    return pipeline_script, template_files


def init_command(pipeline_name: str, destination_name: str, use_generic_template: bool, branch: str = None) -> None:
    # try to import the destination and get config spec
    destination_reference = DestinationReference.from_name(destination_name)
    destination_spec = destination_reference.spec()

    click.echo("Looking up the init scripts...")
    clone_storage = utils.clone_command_repo("init", branch)
    # clone_storage = FileStorage("/home/rudolfix/src/python-dlt-init-template")
    command_module = utils.load_command_module(clone_storage.storage_path)
    pipeline_script, template_files = _get_template_files(command_module, use_generic_template)

    # get init script variant or the default
    init_script_name = os.path.join("variants", pipeline_name + ".py")
    is_variant = clone_storage.has_file(init_script_name)
    if is_variant:
        dest_pipeline_script = pipeline_name + ".py"
        click.echo(f"Using a verified pipeline {fmt.bold(dest_pipeline_script)}")
        if use_generic_template:
            fmt.warning("--generic parameter is meaningless if verified pipeline is used")
    else:
        # use default
        init_script_name = pipeline_script

    # normalize source name
    norm_source_name = normalize_schema_name(pipeline_name)
    if norm_source_name != pipeline_name:
        raise InvalidSchemaName(pipeline_name, norm_source_name)
    dest_pipeline_script = norm_source_name + ".py"

    # prepare destination storage
    dest_storage = FileStorage(os.path.abspath("."))
    if not dest_storage.has_folder(DOT_DLT):
        dest_storage.create_folder(DOT_DLT)

    # check if directory is empty
    toml_files = [make_dot_dlt_path(CONFIG_TOML), make_dot_dlt_path(SECRETS_TOML)]
    created_files = template_files + [dest_pipeline_script] + toml_files
    existing_files = dest_storage.list_folder_files(".", to_root=False) + dest_storage.list_folder_files(DOT_DLT, to_root=True)
    will_overwrite = set(created_files).intersection(existing_files)
    if will_overwrite:
        if not click.confirm(f"The following files in current folder will be replaced: {will_overwrite}. Do you want to continue?", default=False):
            raise FileExistsError("Would overwrite following files:", will_overwrite)

    # read module source and parse it
    visitor = utils.parse_init_script("init", clone_storage.load(init_script_name), init_script_name)
    if visitor.is_destination_imported:
        raise CliCommandException("init", f"The pipeline script {init_script_name} import a destination from dlt.destinations. You should specify destinations by name when calling dlt.pipeline or dlt.run in init scripts.")
    if n.PIPELINE not in visitor.known_calls:
        raise CliCommandException("init", f"The pipeline script {init_script_name} does not seem to initialize pipeline with dlt.pipeline. Please initialize pipeline explicitly in init scripts.")

    # find all arguments in all calls to replace
    transformed_nodes = _find_call_arguments_to_replace(
        visitor,
        [("destination", destination_name), ("pipeline_name", pipeline_name), ("dataset_name", pipeline_name + "_data")],
        init_script_name
    )

    # inspect the script
    inspect_pipeline_script(clone_storage.storage_path, clone_storage.to_relative_path(init_script_name))

    if len(_SOURCES) == 0:
        raise CliCommandException("init", f"The pipeline script {init_script_name} is not creating or importing any sources or resources")

    for source_q_name, source_config in _SOURCES.items():
        if source_q_name not in visitor.known_sources_resources:
            raise CliCommandException("init", f"The pipeline script {init_script_name} imports a source/resource {source_config.f.__name__} from module {source_config.module.__name__}. In init scripts you must declare all sources and resources in single file.")

    # rename sources and resources
    if not is_variant:
        transformed_nodes.extend(_find_source_calls_to_replace(visitor, pipeline_name))

    # detect all the required secrets and configs that should go into tomls files
    required_secrets, required_config = _detect_required_configs(visitor)
    # add destination spec to required secrets
    credentials_type = destination_spec().get_resolvable_fields()["credentials"]
    required_secrets["destinations:" + destination_name] = WritableConfigValue("credentials", credentials_type, ("destination", destination_name))

    # modify the script
    script_lines = rewrite_python_script(visitor.source_lines, transformed_nodes)
    dest_script_source = "".join(script_lines)
    # validate by parsing
    ast.parse(source=dest_script_source)

    # welcome message
    click.echo()
    click.echo("Your new pipeline %s is ready to be customized!" % fmt.bold(pipeline_name))
    click.echo("* Review and change how dlt loads your data in %s" % fmt.bold(dest_pipeline_script))
    click.echo("* Add credentials to %s and other secrets in %s" % (fmt.bold(destination_name), fmt.bold(toml_files[1])))

    # add dlt to dependencies
    requirements_txt: str = None
    # figure out the build system
    if dest_storage.has_file(utils.PYPROJECT_TOML):
        click.echo()
        click.echo("Your python dependencies are kept in %s. Please add the dependency for %s as follows:" % (fmt.bold(utils.PYPROJECT_TOML), fmt.bold(DLT_PKG_NAME)))
        click.echo(fmt.bold("%s [%s] >= %s" % (DLT_PKG_NAME, destination_name, __version__)))
        click.echo("If you are using poetry you may issue the following command:")
        click.echo(fmt.bold("poetry add %s -E %s" % (DLT_PKG_NAME, destination_name)))
        click.echo("If the dependency is already added, make sure you add the extra %s to it" % fmt.bold(destination_name))
    else:
        req_dep = f"{DLT_PKG_NAME}[{destination_name}]"
        req_dep_line = f"{req_dep} >= {pkg_version(DLT_PKG_NAME)}\n"
        if dest_storage.has_file(utils.REQUIREMENTS_TXT):
            click.echo("Your python dependencies are kept in %s. Please add the dependency for %s as follows:" % (fmt.bold(utils.REQUIREMENTS_TXT), fmt.bold(DLT_PKG_NAME)))
            click.echo(req_dep_line)
            click.echo("To install dlt with the %s extra using pip:" % fmt.bold(destination_name))
            click.echo(f"pip3 install {req_dep}")
        else:
            requirements_txt = req_dep_line
            click.echo("* %s was created. Install it with:\npip3 install -r %s" % (fmt.bold(utils.REQUIREMENTS_TXT), utils.REQUIREMENTS_TXT))
    click.echo("* Read %s for more information" % fmt.bold("https://dlthub.com/docs/walkthroughs/create-a-pipeline"))

    # copy files at the very end
    for file_name in template_files + toml_files:
        shutil.copy(clone_storage.make_full_path(file_name), dest_storage.make_full_path(file_name))

    # create script
    dest_storage.save(dest_pipeline_script, dest_script_source)
    # generate tomls with comments
    secrets_prov = SecretsTomlProvider()
    write_values(secrets_prov._toml, required_secrets.values())
    config_prov = ConfigTomlProvider()
    write_values(config_prov._toml, required_config.values())
    # write toml files
    secrets_prov._write_toml()
    config_prov._write_toml()

    if requirements_txt is not None:
        dest_storage.save(utils.REQUIREMENTS_TXT, requirements_txt)
