import inspect
import os
import ast
import click
import shutil
import tempfile
import sys
from types import ModuleType
from typing import Dict, List, Tuple
from importlib.metadata import version as pkg_version
from importlib import import_module

from dlt.common.git import clone_repo
from dlt.common.configuration.providers.toml import ConfigTomlProvider, SecretsTomlProvider
from dlt.common.configuration.specs.base_configuration import is_secret_hint
from dlt.common.configuration.accessors import DLT_SECRETS_VALUE, DLT_CONFIG_VALUE
from dlt.common.exceptions import DltException
from dlt.common.logger import DLT_PKG_NAME
from dlt.common.normalizers.names.snake_case import normalize_schema_name
from dlt.common.destination import DestinationReference
from dlt.common.reflection.utils import set_ast_parents
from dlt.common.schema.exceptions import InvalidSchemaName
from dlt.common.storages.file_storage import FileStorage
from dlt.common.typing import AnyType, is_optional_type

from dlt.extract.decorators import _SOURCES
import dlt.reflection.names as n
from dlt.reflection.script_inspector import inspect_pipeline_script
from dlt.reflection.script_visitor import PipelineScriptVisitor

import dlt.cli.echo as fmt
from dlt.cli.config_toml_writer import WritableConfigValue, write_values


REQUIREMENTS_TXT = "requirements.txt"
PYPROJECT_TOML = "pyproject.toml"


def _clone_init_repo(branch: str) -> Tuple[FileStorage, List[str], str]:
    # return tuple is (file storage for cloned repo, list of template files to copy, the default pipeline template script)
    # template_dir = "~/src/python-dlt-init-template"
    template_dir = tempfile.mkdtemp()
    clone_repo("https://github.com/scale-vector/python-dlt-init-template.git", template_dir, branch=branch)

    clone_storage = FileStorage(template_dir)

    assert clone_storage.has_file("pipeline.py")

    # import the settings from the clone
    try:
        template_dir, template_module_name = os.path.split(template_dir)
        sys.path.append(template_dir)
        template_module = import_module(template_module_name)
        return clone_storage, template_module.TEMPLATE_FILES, template_module.PIPELINE_SCRIPT
    finally:
        sys.path.remove(template_dir)


def _parse_init_script(script_source: str, init_script_name: str) -> PipelineScriptVisitor:
    # parse the script first
    tree = ast.parse(source=script_source)
    set_ast_parents(tree)
    visitor = PipelineScriptVisitor(script_source)
    visitor.visit(tree)
    if len(visitor.mod_aliases) == 0:
        raise CliCommandException("init", f"The pipeline script {init_script_name} does not import dlt or has bizarre import structure")
    if visitor.is_destination_imported:
        raise CliCommandException("init", f"The pipeline script {init_script_name} import a destination from dlt.destinations. You should specify destinations by name when calling dlt.pipeline or dlt.run in init scripts.")
    if n.PIPELINE not in visitor.known_calls:
        raise CliCommandException("init", f"The pipeline script {init_script_name} does not seem to initialize pipeline with dlt.pipeline. Please initialize pipeline explicitly in init scripts.")
    if n.RUN not in visitor.known_calls:
        raise CliCommandException("init", f"The pipeline script {init_script_name} does not seem to run the pipeline.")

    return visitor


def _find_argument_nodes_to_replace(visitor: PipelineScriptVisitor, replace_nodes: List[Tuple[str, str]], init_script_name: str) -> List[Tuple[ast.Constant, str, str]]:
    # the input tuple (call argument name, replacement value)
    # the returned tuple (node, replacement value, node type)
    transformed_nodes: List[Tuple[ast.Constant, str, str]] = []
    known_calls: Dict[str, List[inspect.BoundArguments]] = visitor.known_calls
    for arg_name, calls in known_calls.items():
        for args in calls:
            for t_arg_name, t_value in replace_nodes:
                if t_arg_name in args.arguments:
                    dn_node: ast.AST = args.arguments[t_arg_name]
                    if dn_node is not None:
                        if not isinstance(dn_node, ast.Constant) or not isinstance(dn_node.value, str):
                            raise CliCommandException("init", f"The pipeline script {init_script_name} must pass the {t_arg_name} as string to '{arg_name}' function in line {dn_node.lineno}")
                        else:
                            transformed_nodes.append((dn_node, t_value, t_arg_name))

    # there was at least one replacement
    for t_arg_name, _ in replace_nodes:
        if len(list(filter(lambda tn: tn[2] == t_arg_name, transformed_nodes))) == 0:
            raise CliCommandException("init", f"The pipeline script {init_script_name} is not explicitly passing the '{t_arg_name}' argument to 'pipeline' or 'run' function. In init script the default and configured values are not accepted.")
    return transformed_nodes


def _detect_required_configs(visitor: PipelineScriptVisitor, script_module: ModuleType, init_script_name: str) -> Tuple[Dict[str, WritableConfigValue], Dict[str, WritableConfigValue]]:
    # all detected secrets with namespaces
    required_secrets: Dict[str, WritableConfigValue] = {}
    # all detected configs with namespaces
    required_config: Dict[str, WritableConfigValue] = {}

    # skip sources without spec. those are not imported and most probably are inner functions. also skip the sources that are not called
    # also skip the sources that are called from functions, the parent of call object to the source must be None (no outer function)
    known_imported_sources = {name: _SOURCES[name] for name in visitor.known_sources
        if name in _SOURCES and name in visitor.known_source_calls and any(call.parent is None for call in visitor.known_source_calls[name])}  # type: ignore

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


def _rewrite_script(script_source: str, transformed_nodes: List[Tuple[ast.Constant, str, str]]) -> str:
    module_source_lines: List[str] = ast._splitlines_no_ff(script_source)  # type: ignore
    script_lines: List[str] = []
    last_line = -1
    last_offset = -1
    # sort transformed nodes by line and offset
    for node, t_value, _ in sorted(transformed_nodes, key=lambda n: (n[0].lineno, n[0].col_offset)):
        # do we have a line changed
        if last_line != node.lineno - 1:
            # add remainder from the previous line
            if last_offset >= 0:
                script_lines.append(module_source_lines[last_line][last_offset:])
            # add all new lines from previous line to current
            script_lines.extend(module_source_lines[last_line+1:node.lineno-1])
            # add trailing characters until node in current line starts
            script_lines.append(module_source_lines[node.lineno-1][:node.col_offset])
        elif last_offset >= 0:
            # no line change, add the characters from the end of previous node to the current
            script_lines.append(module_source_lines[last_line][last_offset:node.col_offset])

        # replace node value
        script_lines.append(f'"{t_value}"')
        last_line = node.end_lineno - 1
        last_offset = node.end_col_offset

    # add all that was missing
    if last_offset >= 0:
        script_lines.append(module_source_lines[last_line][last_offset:])
    script_lines.extend(module_source_lines[last_line+1:])

    dest_script = "".join(script_lines)
    # validate by parsing
    ast.parse(source=dest_script)
    return dest_script


def init_command(pipeline_name: str, destination_name: str, branch: str) -> None:
    # try to import the destination and get config spec
    destination_reference = DestinationReference.from_name(destination_name)
    destination_spec = destination_reference.spec()

    click.echo("Cloning the init scripts...")
    clone_storage, TEMPLATE_FILES, PIPELINE_SCRIPT = _clone_init_repo(branch)

    # get init script variant or the default
    init_script_name = os.path.join("variants", pipeline_name + ".py")
    if clone_storage.has_file(init_script_name):
        # use variant
        dest_pipeline_script = pipeline_name + ".py"
        click.echo(f"Using a init script variant {fmt.bold(dest_pipeline_script)}")
    else:
        # use default
        init_script_name = PIPELINE_SCRIPT

    # normalize source name
    norm_source_name = normalize_schema_name(pipeline_name)
    if norm_source_name != pipeline_name:
        raise InvalidSchemaName(pipeline_name, norm_source_name)
    dest_pipeline_script = norm_source_name + ".py"

    # prepare destination storage
    dest_storage = FileStorage(os.path.abspath(os.path.join(".")))
    if not dest_storage.has_folder(".dlt"):
        dest_storage.create_folder(".dlt")

    # check if directory is empty
    toml_files = [".dlt/config.toml", ".dlt/secrets.toml"]
    created_files = TEMPLATE_FILES + [dest_pipeline_script] + toml_files
    existing_files = dest_storage.list_folder_files(".", to_root=False) + dest_storage.list_folder_files(".dlt", to_root=True)
    will_overwrite = set(created_files).intersection(existing_files)
    if will_overwrite:
        if not click.confirm(f"The following files in current folder will be replaced: {will_overwrite}. Do you want to continue?", default=False):
            raise FileExistsError("Would overwrite following files:", will_overwrite)

    # read module source and parse it
    visitor = _parse_init_script(clone_storage.load(init_script_name), init_script_name)

    # find all arguments in all calls to replace
    transformed_nodes = _find_argument_nodes_to_replace(
        visitor,
        [("destination", destination_name), ("pipeline_name", pipeline_name), ("dataset_name", pipeline_name + "_data")],
        init_script_name
    )

    # inspect the script
    script_module = inspect_pipeline_script(clone_storage.make_full_path(init_script_name))

    if len(_SOURCES) == 0:
        raise CliCommandException("init", f"The pipeline script {init_script_name} is not creating or importing any sources or resources")

    for source_q_name, source_config in _SOURCES.items():
        if source_q_name not in visitor.known_sources:
            print(visitor.known_sources)
            raise CliCommandException("init", f"The pipeline script {init_script_name} imports a source/resource {source_config.f.__name__} from module {source_config.module.__name__}. In init scripts you must declare all sources and resources in single file.")

    # detect all the required secrets and configs that should go into tomls files
    required_secrets, required_config = _detect_required_configs(visitor, script_module, init_script_name)
    # add destination spec to required secrets
    credentials_type = destination_spec().get_resolvable_fields()["credentials"]
    required_secrets["destinations:" + destination_name] = WritableConfigValue("credentials", credentials_type, ("destination", destination_name))

    # modify the script
    dest_script_source = _rewrite_script(visitor.source, transformed_nodes)

    # welcome message
    click.echo()
    click.echo("Your new pipeline %s is ready to be customized!" % fmt.bold(pipeline_name))
    click.echo("* Review and change how dlt loads your data in %s" % fmt.bold(dest_pipeline_script))
    click.echo("* Add credentials to %s and other secrets in %s" % (fmt.bold(destination_name), fmt.bold(toml_files[1])))
    click.echo("* Configure your pipeline in %s" % fmt.bold(toml_files[1]))
    click.echo("* See %s for further information" % fmt.bold("README.md"))
    click.echo()

    # add dlt to dependencies
    dlt_version = pkg_version(DLT_PKG_NAME)
    requirements_txt: str = None
    # figure out the build system
    if dest_storage.has_file(PYPROJECT_TOML):
       click.echo("Your python dependencies are kept in %s. Please add the dependency for %s as follows:" % (fmt.bold(PYPROJECT_TOML), fmt.bold(DLT_PKG_NAME)))
       click.echo(fmt.bold("%s [%s] >= %s" % (DLT_PKG_NAME, destination_name, dlt_version)))
       click.echo("If you are using poetry you may issue the following command:")
       click.echo(fmt.bold("poetry add %s -E %s" % (DLT_PKG_NAME, destination_name)))
       click.echo("If the dependency is already added, make sure you add the extra %s to it" % fmt.bold(destination_name))
    else:
        req_dep_line = f"{DLT_PKG_NAME}[{destination_name}] >= {pkg_version(DLT_PKG_NAME)}\n"
        if dest_storage.has_file(REQUIREMENTS_TXT):
            click.echo("Your python dependencies are kept in %s. Please add the dependency for %s as follows:" % (fmt.bold(REQUIREMENTS_TXT), fmt.bold(DLT_PKG_NAME)))
            click.echo(req_dep_line)
            click.echo("To install dlt with the %s extra using pip:" % fmt.bold(destination_name))
        else:
            requirements_txt = req_dep_line
            click.echo("* %s created. Install it with:\npip3 install -r %s" % (fmt.bold(REQUIREMENTS_TXT), REQUIREMENTS_TXT))

    # copy files at the very end
    for file_name in TEMPLATE_FILES + toml_files:
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
        dest_storage.save(REQUIREMENTS_TXT, requirements_txt)


class CliCommandException(DltException):
    def __init__(self, cmd: str, msg: str, inner_exc: Exception = None) -> None:
        self.cmd = cmd
        self.inner_exc = inner_exc
        super().__init__(msg)
