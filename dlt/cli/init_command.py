import os
import ast
import shutil
from types import ModuleType
from typing import Dict, List, Sequence, Tuple
from importlib.metadata import version as pkg_version
from dlt.cli.telemetry_command import telemetry_status_command

from dlt.common import git
from dlt.common.configuration.paths import get_dlt_project_dir, make_dlt_project_path
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.providers import CONFIG_TOML, SECRETS_TOML, ConfigTomlProvider, SecretsTomlProvider
from dlt.common.normalizers import default_normalizers, import_normalizers
from dlt.common.pipeline import get_dlt_repos_dir
from dlt.version import DLT_PKG_NAME, __version__
from dlt.common.destination.reference import DestinationReference
from dlt.common.reflection.utils import rewrite_python_script
from dlt.common.schema.exceptions import InvalidSchemaName
from dlt.common.storages.file_storage import FileStorage

from dlt.extract.decorators import _SOURCES
import dlt.reflection.names as n
from dlt.reflection.script_inspector import inspect_pipeline_script, load_script_module

from dlt.cli import echo as fmt, pipeline_files as files_ops, source_detection
from dlt.cli import utils
from dlt.cli.config_toml_writer import WritableConfigValue, write_values
from dlt.cli.pipeline_files import PipelineFiles, TPipelineFileEntry, TPipelineFileIndex
from dlt.cli.exceptions import CliCommandException

DLT_INIT_DOCS_URL = "https://dlthub.com/docs/reference/command-line-interface#dlt-init"
DEFAULT_PIPELINES_REPO = "https://github.com/dlt-hub/pipelines.git"
INIT_MODULE_NAME = "init"
PIPELINES_MODULE_NAME = "pipelines"


def _get_template_files(command_module: ModuleType, use_generic_template: bool) -> Tuple[str, List[str]]:
    template_files: List[str] = command_module.TEMPLATE_FILES
    pipeline_script: str = command_module.PIPELINE_SCRIPT
    if use_generic_template:
        pipeline_script, py = os.path.splitext(pipeline_script)
        pipeline_script = f"{pipeline_script}_generic{py}"
    return pipeline_script, template_files


def _select_pipeline_files(
    pipeline_name: str,
    remote_modified: Dict[str, TPipelineFileEntry],
    remote_deleted: Dict[str, TPipelineFileEntry],
    conflict_modified: Sequence[str],
    conflict_deleted: Sequence[str]
) -> Tuple[str, Dict[str, TPipelineFileEntry], Dict[str, TPipelineFileEntry]]:
    # some files were changed and cannot be updated (or are created without index)
    fmt.echo("Existing files for %s pipeline were changed and cannot be automatically updated" % fmt.bold(pipeline_name))
    if conflict_modified:
        fmt.echo("Following files are MODIFIED locally and CONFLICT with incoming changes: %s" % fmt.bold(", ".join(conflict_modified)))
    if conflict_deleted:
        fmt.echo("Following files are DELETED locally and CONFLICT with incoming changes: %s" % fmt.bold(", ".join(conflict_deleted)))
    can_update_files = set(remote_modified.keys()) - set(conflict_modified)
    can_delete_files = set(remote_deleted.keys()) - set(conflict_deleted)
    if len(can_update_files) > 0 or len(can_delete_files) > 0:
        if len(can_update_files) > 0:
            fmt.echo("Following files can be automatically UPDATED: %s" % fmt.bold(", ".join(can_update_files)))
        if len(can_delete_files) > 0:
            fmt.echo("Following files can be automatically DELETED: %s" % fmt.bold(", ".join(can_delete_files)))
        prompt = "Should incoming changes be Skipped, Applied (local changes will be lost) or Merged (%s UPDATED | %s DELETED | all local changes remain)?" % (fmt.bold(",".join(can_update_files)), fmt.bold(",".join(can_delete_files)))
        choices = "sam"
    else:
        prompt = "Should incoming changes be Skipped or Applied?"
        choices = "sa"
    # Skip / Apply / Merge
    resolution = fmt.prompt(prompt, choices, default="s")
    if resolution == "s":
        # do not copy nor delete any files
        fmt.echo("Skipping all incoming changes. No local files were modified.")
        remote_modified.clear()
        remote_deleted.clear()
    elif resolution == "m":
        # update what we can
        fmt.echo("Merging the incoming changes. No files with local changes were modified.")
        remote_modified = {n:e for n, e in remote_modified.items() if n in can_update_files}
        remote_deleted = {n:e for n, e in remote_deleted.items() if n in can_delete_files}
    else:
        # fully overwrite, leave all files to be copied
        fmt.echo("Applying all incoming changes to local files.")

    return resolution, remote_modified, remote_deleted


def _get_dependency_system(dest_storage: FileStorage) -> str:
    if dest_storage.has_file(utils.PYPROJECT_TOML):
        return utils.PYPROJECT_TOML
    elif dest_storage.has_file(utils.REQUIREMENTS_TXT):
        return utils.REQUIREMENTS_TXT
    else:
        return None


def _list_pipelines(repo_location: str, branch: str = None) -> Dict[str, PipelineFiles]:
    clone_storage = git.get_fresh_repo_files(repo_location, get_dlt_repos_dir(), branch=branch)
    pipelines_storage = FileStorage(clone_storage.make_full_path(PIPELINES_MODULE_NAME))

    pipelines: Dict[str, PipelineFiles] = {}
    for pipeline_name in files_ops.get_pipeline_names(pipelines_storage):
        try:
            pipelines[pipeline_name] = files_ops.get_pipeline_files(pipelines_storage, pipeline_name)
        except Exception as ex:
            fmt.warning(f"Pipeline {pipeline_name} not available: {ex}")

    return pipelines


def _welcome_message(pipeline_name: str, destination_name: str, pipeline_files: PipelineFiles, dependency_system: str, is_new_pipeline: bool) -> None:
    fmt.echo()
    if pipeline_files.is_template:
        fmt.echo("Your new pipeline %s is ready to be customized!" % fmt.bold(pipeline_name))
        fmt.echo("* Review and change how dlt loads your data in %s" % fmt.bold(pipeline_files.dest_pipeline_script))
    else:
        if is_new_pipeline:
            fmt.echo("Pipeline %s was added to your project!" % fmt.bold(pipeline_name))
            fmt.echo("* See the usage examples and code snippets to copy from %s" % fmt.bold(pipeline_files.dest_pipeline_script))
        else:
            fmt.echo("Pipeline %s was updated to the newest version!" % fmt.bold(pipeline_name))

    if is_new_pipeline:
        fmt.echo("* Add credentials for %s and other secrets in %s" % (fmt.bold(destination_name), fmt.bold(make_dlt_project_path(SECRETS_TOML))))

    if dependency_system:
        fmt.echo("* Add the required dependencies to %s:" % fmt.bold(dependency_system))
        for dep in pipeline_files.requirements:
            fmt.echo("  " + fmt.bold(dep))
        fmt.echo("  If the dlt dependency is already added, make sure you install the extra for %s to it" % fmt.bold(destination_name))
        if dependency_system == utils.REQUIREMENTS_TXT:
            qs = "' '"
            fmt.echo("  To install with pip: %s" % fmt.bold(f"pip3 install '{qs.join(pipeline_files.requirements)}'"))
        elif dependency_system == utils.PYPROJECT_TOML:
            fmt.echo("  If you are using poetry you may issue the following command:")
            fmt.echo(fmt.bold("  poetry add %s -E %s" % (DLT_PKG_NAME, destination_name)))
        fmt.echo()
    else:
        fmt.echo("* %s was created. Install it with:\npip3 install -r %s" % (fmt.bold(utils.REQUIREMENTS_TXT), utils.REQUIREMENTS_TXT))

    if is_new_pipeline:
        fmt.echo("* Read %s for more information" % fmt.bold("https://dlthub.com/docs/walkthroughs/create-a-pipeline"))
    else:
        fmt.echo("* Read %s for more information" % fmt.bold("https://dlthub.com/docs/walkthroughs/add-a-pipeline"))


def list_pipelines_command(repo_location: str, branch: str = None) -> None:
    fmt.echo("Looking up the init scripts in %s..." % fmt.bold(repo_location))
    for pipeline_name, pipeline_files in _list_pipelines(repo_location, branch).items():
        fmt.echo("%s: %s" % (fmt.bold(pipeline_name), pipeline_files.doc))


def init_command(pipeline_name: str, destination_name: str, use_generic_template: bool, repo_location: str, branch: str = None) -> None:
    # try to import the destination and get config spec
    destination_reference = DestinationReference.from_name(destination_name)
    destination_spec = destination_reference.spec()

    fmt.echo("Looking up the init scripts in %s..." % fmt.bold(repo_location))
    clone_storage = git.get_fresh_repo_files(repo_location, get_dlt_repos_dir(), branch=branch)
    # copy init files from here
    init_storage = FileStorage(clone_storage.make_full_path(INIT_MODULE_NAME))
    # copy pipeline files from here
    pipelines_storage = FileStorage(clone_storage.make_full_path(PIPELINES_MODULE_NAME))
    # load init module and get init files and script
    init_module = load_script_module(clone_storage.storage_path, INIT_MODULE_NAME)
    pipeline_script, template_files = _get_template_files(init_module, use_generic_template)
    # prepare destination storage
    dest_storage = FileStorage(os.path.abspath("."))
    if not dest_storage.has_folder(get_dlt_project_dir()):
        dest_storage.create_folder(get_dlt_project_dir())
    # get local index of pipeline files
    local_index = files_ops.load_pipeline_local_index(pipeline_name)
    # folder deleted at dest - full refresh
    if not dest_storage.has_folder(pipeline_name):
        local_index["files"] = {}
    # is update or new pipeline
    is_new_pipeline = len(local_index["files"]) == 0

    # look for existing pipeline
    pipeline_files: PipelineFiles = None
    remote_index: TPipelineFileIndex = None
    if pipelines_storage.has_folder(pipeline_name):
        # get pipeline files
        pipeline_files = files_ops.get_pipeline_files(pipelines_storage, pipeline_name)
        # get file index from remote pipeline files being copied
        remote_index = files_ops.get_remote_pipeline_index(pipeline_files.storage.storage_path, pipeline_files.files)
        # diff local and remote index to get modified and deleted files
        remote_new, remote_modified, remote_deleted = files_ops.gen_index_diff(local_index, remote_index)
        # find files that are modified locally
        conflict_modified, conflict_deleted = files_ops.find_conflict_files(local_index, remote_new, remote_modified, remote_deleted, dest_storage)
        # add new to modified
        remote_modified.update(remote_new)
        if conflict_modified or conflict_deleted:
            # select pipeline files that can be copied/updated
            _, remote_modified, remote_deleted = _select_pipeline_files(
                pipeline_name,
                remote_modified,
                remote_deleted,
                conflict_modified,
                conflict_deleted
            )
        if not remote_deleted and not remote_modified:
            fmt.echo("No files to update, exiting")
            return

        if remote_index["is_dirty"]:
            fmt.warning(f"The pipelines repository is dirty. {pipeline_name} pipeline files may not update correctly in the future.")
        # add template files
        pipeline_files.files.extend(template_files)

    else:
        # normalize source name
        naming, _ = import_normalizers(default_normalizers())
        norm_source_name = naming.normalize_identifier(pipeline_name)
        if norm_source_name != pipeline_name:
            raise InvalidSchemaName(pipeline_name, norm_source_name)
        dest_pipeline_script = norm_source_name + ".py"
        pipeline_files = PipelineFiles(True, init_storage, pipeline_script, dest_pipeline_script, template_files, [], "")
        if dest_storage.has_file(dest_pipeline_script):
            fmt.warning("Pipeline script %s already exist, exiting" % dest_pipeline_script)
            return

    # add .dlt/*.toml files to be copied
    pipeline_files.files.extend([make_dlt_project_path(CONFIG_TOML), make_dlt_project_path(SECRETS_TOML)])

    # add dlt extras line to requirements
    req_dep = f"{DLT_PKG_NAME}[{destination_name}]"
    req_dep_line = f"{req_dep}>={pkg_version(DLT_PKG_NAME)}"
    pipeline_files.requirements.insert(0, req_dep_line)

    # read module source and parse it
    visitor = utils.parse_init_script("init", pipeline_files.storage.load(pipeline_files.pipeline_script), pipeline_files.pipeline_script)
    if visitor.is_destination_imported:
        raise CliCommandException("init", f"The pipeline script {pipeline_files.pipeline_script} import a destination from dlt.destinations. You should specify destinations by name when calling dlt.pipeline or dlt.run in init scripts.")
    if n.PIPELINE not in visitor.known_calls:
        raise CliCommandException("init", f"The pipeline script {pipeline_files.pipeline_script} does not seem to initialize pipeline with dlt.pipeline. Please initialize pipeline explicitly in init scripts.")

    # find all arguments in all calls to replace
    transformed_nodes = source_detection.find_call_arguments_to_replace(
        visitor,
        [("destination", destination_name), ("pipeline_name", pipeline_name), ("dataset_name", pipeline_name + "_data")],
        pipeline_files.pipeline_script
    )

    # inspect the script
    inspect_pipeline_script(
        pipeline_files.storage.storage_path,
        pipeline_files.storage.to_relative_path(pipeline_files.pipeline_script),
        ignore_missing_imports=True
    )

    # detect all the required secrets and configs that should go into tomls files
    if pipeline_files.is_template:
        # replace destination, pipeline_name and dataset_name in templates
        transformed_nodes = source_detection.find_call_arguments_to_replace(
            visitor,
            [("destination", destination_name), ("pipeline_name", pipeline_name), ("dataset_name", pipeline_name + "_data")],
            pipeline_files.pipeline_script
        )
        # template sources are always in module starting with "pipeline"
        # for templates, place config and secrets into top level section
        required_secrets, required_config, checked_sources = source_detection.detect_source_configs(_SOURCES, "pipeline", ())
        # template has a strict rules where sources are placed
        for source_q_name, source_config in checked_sources.items():
            if source_q_name not in visitor.known_sources_resources:
                raise CliCommandException("init", f"The pipeline script {pipeline_files.pipeline_script} imports a source/resource {source_config.f.__name__} from module {source_config.module.__name__}. In init scripts you must declare all sources and resources in single file.")
        # rename sources and resources
        transformed_nodes.extend(source_detection.find_source_calls_to_replace(visitor, pipeline_name))
    else:
        # replace only destination for existing pipelines
        transformed_nodes = source_detection.find_call_arguments_to_replace(visitor, [("destination", destination_name)], pipeline_files.pipeline_script)
        # pipeline sources are in module with name starting from {pipeline_name}
        # for verified pipelines place in the specific source section
        required_secrets, required_config, checked_sources = source_detection.detect_source_configs(_SOURCES, pipeline_name, (known_sections.SOURCES, pipeline_name))

    if len(checked_sources) == 0:
        raise CliCommandException("init", f"The pipeline script {pipeline_files.pipeline_script} is not creating or importing any sources or resources")

    # add destination spec to required secrets
    credentials_type = destination_spec().get_resolvable_fields()["credentials"]
    required_secrets["destinations:" + destination_name] = WritableConfigValue("credentials", credentials_type, None, ("destination", destination_name))
    # add the global telemetry to required config
    required_config["runtime.dlthub_telemetry"] = WritableConfigValue("dlthub_telemetry", bool, utils.get_telemetry_status(), ("runtime", ))

    # modify the script
    script_lines = rewrite_python_script(visitor.source_lines, transformed_nodes)
    dest_script_source = "".join(script_lines)
    # validate by parsing
    ast.parse(source=dest_script_source)

    # ask for confirmation
    if is_new_pipeline:
        if pipeline_files.is_template:
            fmt.echo("An existing pipeline %s was not found. Creating a new pipeline with name %s." % (fmt.bold(pipeline_name), fmt.bold(pipeline_name)))
        else:
            fmt.echo("Cloning and configuring an existing pipeline %s (%s)" % (fmt.bold(pipeline_name), pipeline_files.doc))
            if use_generic_template:
                fmt.warning("--generic parameter is meaningless if verified pipeline is used")
        if not fmt.confirm("Do you want to proceed?", default=True):
            raise CliCommandException("init", "Aborted")

    dependency_system = _get_dependency_system(dest_storage)
    _welcome_message(pipeline_name, destination_name, pipeline_files, dependency_system, is_new_pipeline)

    # copy files at the very end
    for file_name in pipeline_files.files:
        dest_path = dest_storage.make_full_path(file_name)
        # get files from init section first
        if init_storage.has_file(file_name):
            if dest_storage.has_file(dest_path):
                # do not overwrite any init files
                continue
            src_path = init_storage.make_full_path(file_name)
        else:
            # only those that were modified should be copied from pipeline
            if file_name in remote_modified:
                src_path = pipeline_files.storage.make_full_path(file_name)
            else:
                continue
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        shutil.copy2(src_path, dest_path)

    if remote_index:
        # delete files
        for file_name in remote_deleted:
            if dest_storage.has_file(file_name):
                dest_storage.delete(file_name)
        files_ops.save_pipeline_local_index(pipeline_name, remote_index, remote_modified, remote_deleted)
    # create script
    if not dest_storage.has_file(pipeline_files.dest_pipeline_script):
        dest_storage.save(pipeline_files.dest_pipeline_script, dest_script_source)

    # generate tomls with comments
    secrets_prov = SecretsTomlProvider()
    # print(secrets_prov._toml)
    write_values(secrets_prov._toml, required_secrets.values(), overwrite_existing=False)
    config_prov = ConfigTomlProvider()
    write_values(config_prov._toml, required_config.values(), overwrite_existing=False)
    # write toml files
    secrets_prov.write_toml()
    config_prov.write_toml()

    # telemetry_status_command()

    # if there's no dependency system write the requirements file
    if dependency_system is None:
        requirements_txt = "\n".join(pipeline_files.requirements)
        dest_storage.save(utils.REQUIREMENTS_TXT, requirements_txt)
