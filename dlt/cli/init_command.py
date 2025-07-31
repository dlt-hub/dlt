import os
import ast
import shutil
from typing import Dict, Sequence, Tuple, Optional
from pathlib import Path


import dlt.destinations
from dlt.common import git
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.providers import (
    SECRETS_TOML,
    ConfigTomlProvider,
    SecretsTomlProvider,
)
from dlt.common.pipeline import get_dlt_repos_dir
from dlt.version import DLT_PKG_NAME, __version__
from dlt.common.destination import Destination
from dlt.common.reflection.utils import rewrite_python_script
from dlt.common.runtime import run_context
from dlt.common.schema.utils import is_valid_schema_name
from dlt.common.schema.exceptions import InvalidSchemaName
from dlt.common.storages.file_storage import FileStorage

from dlt.sources import SourceReference

import dlt.reflection.names as n
from dlt.reflection.script_inspector import import_pipeline_script

from dlt.cli import echo as fmt, pipeline_files as files_ops, source_detection, utils

# keep it for backward compat
from dlt.cli import DEFAULT_VERIFIED_SOURCES_REPO
from dlt.cli.config_toml_writer import WritableConfigValue, write_values
from dlt.cli.pipeline_files import (
    TEMPLATE_FILES,
    SOURCES_MODULE_NAME,
    SINGLE_FILE_TEMPLATE_MODULE_NAME,
    SourceConfiguration,
    TVerifiedSourceFileEntry,
    TVerifiedSourceFileIndex,
)
from dlt.cli.exceptions import CliCommandInnerException
from dlt.cli.ai_command import SUPPORTED_IDES, TSupportedIde


DLT_INIT_DOCS_URL = "https://dlthub.com/docs/reference/command-line-interface#dlt-init"


def list_sources_command(repo_location: str, branch: str = None) -> None:
    fmt.echo("---")
    fmt.echo("Available dlt core sources:")
    fmt.echo("---")
    core_sources = _list_core_sources()
    for source_name, source_configuration in core_sources.items():
        msg = "%s: %s" % (fmt.bold(source_name), source_configuration.doc)
        fmt.echo(msg)

    fmt.echo("---")
    fmt.echo("Available dlt single file templates:")
    fmt.echo("---")
    template_sources = _list_template_sources()
    for source_name, source_configuration in template_sources.items():
        msg = "%s: %s" % (fmt.bold(source_name), source_configuration.doc)
        fmt.echo(msg)

    fmt.echo("---")
    fmt.echo("Available verified sources:")
    fmt.echo("---")
    for source_name, source_configuration in _list_verified_sources(repo_location, branch).items():
        reqs = source_configuration.requirements
        dlt_req_string = str(reqs.dlt_requirement_base)
        msg = "%s: " % (fmt.bold(source_name))
        if source_name in core_sources.keys():
            msg += "(Deprecated since dlt 1.0.0 in favor of core source of the same name) "
        msg += source_configuration.doc
        if not reqs.is_installed_dlt_compatible():
            msg += fmt.warning_style(" [needs update: %s]" % (dlt_req_string))

        fmt.echo(msg)


def list_destinations_command() -> None:
    fmt.echo("---")
    fmt.echo("Available dlt core destinations:")
    fmt.echo("---")
    core_destinations = _list_core_destinations()
    for destination_name in core_destinations:
        msg = "%s" % fmt.bold(destination_name)
        fmt.echo(msg)


def init_command(
    source_name: str,
    destination_type: str,
    repo_location: str,
    branch: str = None,
    eject_source: bool = False,
    dry_run: bool = False,
    add_example_pipeline_script: bool = True,
) -> Tuple[Dict[str, str], files_ops.TSourceType]:
    run_ctx = run_context.active()
    destination_storage_path = run_ctx.run_dir
    settings_dir = run_ctx.settings_dir
    sources_dir = run_ctx.get_run_entity("sources")

    is_dlthub_source, display_source_name, _ = _get_source_display_name(source_name)
    copied_files, source_type, selected_ide = init_pipeline_at_destination(
        source_name,
        destination_type,
        repo_location,
        branch,
        eject_source,
        dry_run,
        add_example_pipeline_script,
        destination_storage_path,
        settings_dir,
        sources_dir,
    )
    if is_dlthub_source and copied_files is not None and selected_ide:
        from dlt.cli import DEFAULT_VIBE_SOURCES_REPO, DEFAULT_VERIFIED_SOURCES_REPO
        from dlt.cli.ai_command import ai_setup_command, vibe_source_setup

        fmt.echo()
        fmt.echo()
        ai_setup_command(
            selected_ide,
            DEFAULT_VERIFIED_SOURCES_REPO,
            branch=branch,
            hide_warnings=True,
        )
        fmt.echo()
        fmt.echo()
        # swap default repo location
        if repo_location == DEFAULT_VERIFIED_SOURCES_REPO:
            repo_location = DEFAULT_VIBE_SOURCES_REPO
        vibe_source_setup(display_source_name, repo_location, branch=branch)

    return copied_files, source_type


def init_pipeline_at_destination(
    source_name: str,
    destination_type: str,
    repo_location: str,
    branch: str = None,
    eject_source: bool = False,
    dry_run: bool = False,
    add_example_pipeline_script: bool = True,
    destination_storage_path: str = None,
    settings_dir: str = None,
    sources_dir: str = None,
    target_dependency_system: str = None,
) -> Tuple[Dict[str, str], files_ops.TSourceType, Optional[TSupportedIde]]:
    """
    Initializes a pipeline at the specified destination by setting up the required files, configurations, and dependencies.

    This function handles the discovery of the source type (template, core, or verified), prepares the destination storage,
    resolves conflicts for existing files, and generates necessary configuration files (e.g., `config.toml`, `secrets.toml`).
    It also validates compatibility with the installed dlt version and optionally creates an example pipeline script,
    that loads data from the source to a specifiable destination.

    Args:
        - source_name (str): The name of the source to initialize.
        - destination_type (str): The type of destination (e.g., "bigquery", "redshift").
        - repo_location (str): The location of the verified sources repository.
        - branch (str, optional): The branch of the repository to use. Defaults to None.
        - eject_source (bool, optional): Whether to eject the source code. Defaults to False.
        - dry_run (bool, optional): If True, no files are modified, and the changes are returned as a preview. Defaults to False.
        - skip_example_pipeline_script (bool, optional): If True, skips creating the example pipeline script. Defaults to False.
        - destination_storage_path (str, optional): Path to the destination storage. Defaults to None.
        - settings_dir (str, optional): Path to the settings directory. Defaults to None.
        - sources_dir (str, optional): Path to the sources directory. Defaults to None.
        - target_dependency_system (str, optional): Additional context used to adjust the welcome message. Valid options are
            `requirements.txt` or `pyproject.toml`, Default to None, in which case it will be determined based on the destination storage.

    Returns:
        Tuple[Dict[str, str], Dict[str, WritableConfigValue], Dict[str, WritableConfigValue], files_ops.TSourceType]:
            A tuple containing:
            - A dictionary of copied files (destination path -> source path).
            - The type of the source (e.g., "template", "core", "verified").
            - Name of the selected ide for dlthub sources (defaults to "cursor")
    """
    # try to import the destination and get config spec
    if destination_type:
        destination_reference = Destination.from_reference(destination_type)
        destination_spec = destination_reference.spec

    # lookup core storages
    core_sources_storage = _get_core_sources_storage()
    templates_storage = _get_templates_storage()

    # discover type of source
    source_type: files_ops.TSourceType = "template"
    # extract source name from dlthub source name: dlthub:<name>
    # TODO: add new source type
    is_dlthub_source, display_source_name, source_name = _get_source_display_name(source_name)
    if source_name in files_ops.get_sources_names(core_sources_storage, source_type="core"):
        source_type = "core"
    # do not look into verified sources when setting up dlthub source
    elif not is_dlthub_source:
        verified_sources_storage = _clone_and_get_verified_sources_storage(repo_location, branch)
        if source_name in files_ops.get_sources_names(
            verified_sources_storage, source_type="verified"
        ):
            source_type = "verified"

    # prepare destination storage
    dest_storage = FileStorage(destination_storage_path)
    if not dest_storage.has_folder(settings_dir):
        dest_storage.create_folder(settings_dir)
    # get local index of verified source files
    local_index = files_ops.load_verified_sources_local_index(source_name)
    # folder deleted at dest - full refresh
    if not dest_storage.has_folder(source_name):
        local_index["files"] = {}
    # is update or new source
    is_new_source = len(local_index["files"]) == 0

    # look for existing source
    source_configuration: SourceConfiguration = None
    remote_index: TVerifiedSourceFileIndex = None
    remote_modified: Dict[str, TVerifiedSourceFileEntry] = {}
    remote_deleted: Dict[str, TVerifiedSourceFileEntry] = {}

    if source_type == "verified":
        # get pipeline files
        source_configuration = files_ops.get_verified_source_configuration(
            verified_sources_storage, source_name
        )
        # get file index from remote verified source files being copied
        remote_index = files_ops.get_remote_source_index(
            source_configuration.storage.storage_path,
            source_configuration.files,
            source_configuration.requirements.dlt_version_constraint(),
        )
        # diff local and remote index to get modified and deleted files
        remote_new, remote_modified, remote_deleted = files_ops.gen_index_diff(
            local_index, remote_index
        )
        # find files that are modified locally
        conflict_modified, conflict_deleted = files_ops.find_conflict_files(
            local_index, remote_new, remote_modified, remote_deleted, dest_storage
        )
        # add new to modified
        remote_modified.update(remote_new)
        if conflict_modified or conflict_deleted:
            # select source files that can be copied/updated
            _, remote_modified, remote_deleted = _select_source_files(
                source_name, remote_modified, remote_deleted, conflict_modified, conflict_deleted
            )
        if not remote_deleted and not remote_modified:
            fmt.echo("No files to update, exiting")
            return None, source_type, None

        if remote_index["is_dirty"]:
            fmt.warning(
                f"The verified sources repository is dirty. {display_source_name} source files may"
                " not update correctly in the future."
            )

    else:
        if source_type == "core":
            source_configuration = files_ops.get_core_source_configuration(
                core_sources_storage, source_name, eject_source
            )
            from importlib.metadata import Distribution

            dist = Distribution.from_name(DLT_PKG_NAME)
            extras = dist.metadata.get_all("Provides-Extra") or []

            # Match the extra name to the source name
            canonical_source_name = source_name.replace("_", "-").lower()

            if canonical_source_name in extras:
                source_configuration.requirements.update_dlt_extras(canonical_source_name)

            #  create remote modified index to copy files when ejecting
            remote_modified = {file_name: None for file_name in source_configuration.files}
        else:
            # is single file template source
            if not is_valid_schema_name(source_name):
                raise InvalidSchemaName(source_name)
            source_configuration = files_ops.get_template_configuration(
                templates_storage, source_name, display_source_name
            )

        if dest_storage.has_file(source_configuration.dest_pipeline_script):
            fmt.warning(
                "Pipeline script %s already exists, exiting"
                % source_configuration.dest_pipeline_script
            )
            return None, source_type, None

    # add .dlt/*.toml files to be copied
    # source_configuration.files.extend(
    #     [run_ctx.get_setting(CONFIG_TOML), run_ctx.get_setting(SECRETS_TOML)]
    # )

    # add dlt extras line to requirements
    if destination_type:
        source_configuration.requirements.update_dlt_extras(destination_type)

    # Check compatibility with installed dlt
    if not source_configuration.requirements.is_installed_dlt_compatible():
        msg = (
            "This pipeline requires a newer version of dlt than your installed version"
            f" ({source_configuration.requirements.current_dlt_version()}). Pipeline requires"
            f" '{source_configuration.requirements.dlt_requirement_base}'"
        )
        fmt.warning(msg)
        if not fmt.confirm(
            "Would you like to continue anyway? (you can update dlt after this step)", default=True
        ):
            fmt.echo(
                "You can update dlt with: pip3 install -U"
                f' "{source_configuration.requirements.dlt_requirement_base}"'
            )
            return None, source_type, None

    # read module source and parse it
    visitor = utils.parse_init_script(
        "init",
        source_configuration.storage.load(source_configuration.src_pipeline_script),
        source_configuration.src_pipeline_script,
    )
    if visitor.is_destination_imported:
        raise CliCommandInnerException(
            "init",
            f"The pipeline script {source_configuration.src_pipeline_script} imports a destination"
            " from dlt.destinations. You should specify destinations by name when calling"
            " dlt.pipeline or dlt.run in init scripts.",
        )
    if n.PIPELINE not in visitor.known_calls:
        raise CliCommandInnerException(
            "init",
            f"The pipeline script {source_configuration.src_pipeline_script} does not seem to"
            " initialize a pipeline with dlt.pipeline. Please initialize pipeline explicitly in"
            " your init scripts.",
        )

    # find all arguments in all calls to replace
    transformed_nodes = source_detection.find_call_arguments_to_replace(
        visitor,
        [
            ("destination", destination_type or "duckdb"),
        ],
        source_configuration.src_pipeline_script,
    )

    # inspect the script
    import_pipeline_script(
        source_configuration.storage.storage_path,
        source_configuration.storage.to_relative_path(source_configuration.src_pipeline_script),
        ignore_missing_imports=True,
    )

    # detect all the required secrets and configs that should go into tomls files
    if source_configuration.source_type == "template":
        # replace destination, pipeline_name and dataset_name in templates
        # template sources are always in module starting with "pipeline"
        # for templates, place config and secrets into top level section
        required_secrets, required_config, checked_sources = source_detection.detect_source_configs(
            SourceReference.SOURCES, source_configuration.source_module_prefix, ()
        )
        if is_dlthub_source:
            transformed_nodes = source_detection.find_call_arguments_to_replace(
                visitor,
                [
                    ("destination", destination_type),
                    ("pipeline_name", display_source_name + "_pipeline"),
                    ("dataset_name", display_source_name + "_data"),
                ],
                source_configuration.src_pipeline_script,
            )
            # template has a strict rules where sources are placed
            for source_q_name, source_config in checked_sources.items():
                if source_q_name not in visitor.known_sources_resources:
                    raise CliCommandInnerException(
                        "init",
                        f"The pipeline script {source_configuration.src_pipeline_script} imports a"
                        f" source/resource {source_config.name} from section"
                        f" {source_config.section}. In init scripts you must declare all sources"
                        " and resources in single file. Known names are"
                        f" {list(visitor.known_sources_resources.keys())}.",
                    )
            # rename sources and resources
            transformed_nodes.extend(
                source_detection.find_source_calls_to_replace(visitor, display_source_name)
            )

    else:
        # pipeline sources are in module with name starting from {pipeline_name}
        # for verified pipelines place in the specific source section
        required_secrets, required_config, checked_sources = source_detection.detect_source_configs(
            SourceReference.SOURCES,
            source_configuration.source_module_prefix,
            (known_sections.SOURCES, source_name),
        )
        if len(checked_sources) == 0:
            raise CliCommandInnerException(
                "init",
                f"The pipeline script {source_configuration.src_pipeline_script} is not creating or"
                " importing any sources or resources. Exiting...",
            )

    # add destination spec to required secrets
    if destination_type or add_example_pipeline_script:
        required_secrets["destinations:" + destination_type] = WritableConfigValue(
            destination_type, destination_spec, None, ("destination",)
        )
    # add the global telemetry to required config
    required_config["runtime.dlthub_telemetry"] = WritableConfigValue(
        "dlthub_telemetry", bool, utils.get_telemetry_status(), ("runtime",)
    )

    # modify the script
    script_lines = rewrite_python_script(visitor.source_lines, transformed_nodes)
    dest_script_source = "".join(script_lines)
    # validate by parsing
    ast.parse(source=dest_script_source)

    selected_ide: TSupportedIde = None

    # ask for confirmation
    if is_new_source:
        if source_configuration.source_type == "core":
            fmt.echo(
                "Creating a new pipeline with the dlt core source %s (%s)"
                % (fmt.bold(display_source_name), source_configuration.doc)
            )
            if eject_source:
                fmt.echo(
                    "NOTE: Source code of %s will be ejected. Remember to modify the pipeline "
                    "example script to import the ejected source." % (fmt.bold(display_source_name))
                )
            else:
                fmt.echo(
                    "NOTE: Beginning with dlt 1.0.0, the source %s will no longer be copied from"
                    " the verified sources repo but imported from dlt.sources. You can provide the"
                    " --eject flag to revert to the old behavior." % (fmt.bold(display_source_name))
                )
        elif source_configuration.source_type == "verified":
            new_entity_type = "a new pipeline with" if destination_type else ""
            fmt.echo(
                "Creating and configuring %s the verified source %s (%s)"
                % (new_entity_type, fmt.bold(display_source_name), source_configuration.doc)
            )
        else:
            if source_configuration.is_default_template:
                fmt.echo(
                    "NOTE: Could not find a dlt source or template wih the name %s. Selecting the"
                    " default template." % (fmt.bold(display_source_name))
                )
                fmt.echo(
                    "NOTE: In case you did not want to use the default template, run 'dlt init -l'"
                    " to see all available sources and templates."
                )
            if is_dlthub_source:
                fmt.echo("dlt will generate useful project rules tailored to your assistant/IDE.")
                selected_ide = fmt.prompt(
                    "Press Enter to accept the default (cursor), or type a name",
                    choices=SUPPORTED_IDES,
                    default="cursor",
                    show_choices=False,
                    show_default=False,
                )

                fmt.echo(
                    "Initializing pipeline %s, adding %s rules, code snippets and docs for %s"
                    " source."
                    % (
                        fmt.bold(source_configuration.dest_pipeline_script),
                        fmt.bold(selected_ide),
                        fmt.bold(display_source_name),
                    )
                )
            else:
                fmt.echo(
                    "Creating and configuring a new pipeline with the dlt core template %s (%s)"
                    % (
                        fmt.bold(source_configuration.dest_pipeline_script),
                        source_configuration.doc,
                    )
                )

        if not fmt.confirm("Do you want to proceed?", default=True):
            raise CliCommandInnerException("init", "Aborted")

    dependency_system = target_dependency_system or _get_dependency_system(dest_storage)
    _welcome_message(
        display_source_name,
        destination_type,
        source_configuration,
        dependency_system,
        is_new_source,
        add_example_pipeline_script,
    )

    # copy files at the very end
    copy_files = []
    # copy template files
    for file_name in TEMPLATE_FILES:
        dest_path = dest_storage.make_full_path(file_name)
        if templates_storage.has_file(file_name):
            if dest_storage.has_file(dest_path):
                # do not overwrite any init files
                continue
            copy_files.append((templates_storage.make_full_path(file_name), dest_path))

    # only those that were modified should be copied from verified sources
    for file_name in remote_modified:
        copy_files.append(
            (
                source_configuration.storage.make_full_path(file_name),
                # copy into where "sources" reside in run context, being root dir by default
                dest_storage.make_full_path(os.path.join(sources_dir, file_name)),
            )
        )
    # if dry-run, do not actually modify storage, just return file content
    pipeline_script_target_path = dest_storage.make_full_path(
        os.path.join(sources_dir, source_configuration.dest_pipeline_script)
    )
    if dry_run:
        files_to_create: Dict[str, str] = {}
        for source_path, dest_path in copy_files:
            try:
                files_to_create[dest_path] = dest_storage.load(source_path)
            except UnicodeDecodeError:
                fmt.warning(
                    f"File {source_path} was skipped not a text file. It will not be copied to"
                    f" {dest_path}"
                )
        if add_example_pipeline_script:
            files_to_create[pipeline_script_target_path] = dest_script_source
        # todo: handle remote index changes?
        return files_to_create, source_type, None

    # modify storage
    else:
        for src_path, dest_path in copy_files:
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            shutil.copy2(src_path, dest_path)
        if remote_index:
            # delete files
            for file_name in remote_deleted:
                if dest_storage.has_file(file_name):
                    dest_storage.delete(file_name)
            files_ops.save_verified_source_local_index(
                source_name, remote_index, remote_modified, remote_deleted
            )
        # create example script
        if (
            not dest_storage.has_file(source_configuration.dest_pipeline_script)
            and add_example_pipeline_script
        ):
            dest_storage.save(pipeline_script_target_path, dest_script_source)

        # generate tomls with comments
        secrets_prov = SecretsTomlProvider(settings_dir)
        write_values(secrets_prov._config_toml, required_secrets.values(), overwrite_existing=False)

        config_prov = ConfigTomlProvider(settings_dir)
        write_values(config_prov._config_toml, required_config.values(), overwrite_existing=False)

        # write toml files
        secrets_prov.write_toml()
        config_prov.write_toml()

        # if there's no dependency system write the requirements file
        if dependency_system is None:
            requirements_txt = "\n".join(source_configuration.requirements.compiled())
            dest_storage.save(utils.REQUIREMENTS_TXT, requirements_txt)

        copied_files: Dict[str, str] = {dest_path: src_path for src_path, dest_path in copy_files}
        return copied_files, source_type, selected_ide


def _get_source_display_name(source_name: str) -> Tuple[bool, str, str]:
    """Parses dlthub source name and splits it into template and display names"""
    if is_vibe_source := source_name.startswith("dlthub:"):
        # skip prefix and setup display source name
        display_source_name = source_name[7:]
        # use special template for vibe coding
        source_name = "vibe_rest_api"
    else:
        display_source_name = source_name
    return is_vibe_source, display_source_name, source_name


def _get_core_sources_storage() -> FileStorage:
    """Get FileStorage for core sources"""
    local_path = Path(os.path.dirname(os.path.realpath(__file__))).parent / SOURCES_MODULE_NAME
    return FileStorage(str(local_path))


def _get_templates_storage() -> FileStorage:
    """Get FileStorage for single file templates"""
    # look up init storage in core
    init_path = (
        Path(os.path.dirname(os.path.realpath(__file__))).parent
        / SOURCES_MODULE_NAME
        / SINGLE_FILE_TEMPLATE_MODULE_NAME
    )
    return FileStorage(str(init_path))


def _clone_and_get_verified_sources_storage(repo_location: str, branch: str = None) -> FileStorage:
    """Clone and get FileStorage for verified sources templates"""

    fmt.echo("Looking up verified sources at %s..." % fmt.bold(repo_location))
    clone_storage = git.get_fresh_repo_files(repo_location, get_dlt_repos_dir(), branch=branch)
    # copy dlt source files from here
    return FileStorage(clone_storage.make_full_path(SOURCES_MODULE_NAME))


def _select_source_files(
    source_name: str,
    remote_modified: Dict[str, TVerifiedSourceFileEntry],
    remote_deleted: Dict[str, TVerifiedSourceFileEntry],
    conflict_modified: Sequence[str],
    conflict_deleted: Sequence[str],
) -> Tuple[str, Dict[str, TVerifiedSourceFileEntry], Dict[str, TVerifiedSourceFileEntry]]:
    # some files were changed and cannot be updated (or are created without index)
    fmt.echo(
        "Existing files for %s source were changed and cannot be automatically updated"
        % fmt.bold(source_name)
    )
    if conflict_modified:
        fmt.echo(
            "Following files are MODIFIED locally and CONFLICT with incoming changes: %s"
            % fmt.bold(", ".join(conflict_modified))
        )
    if conflict_deleted:
        fmt.echo(
            "Following files are DELETED locally and CONFLICT with incoming changes: %s"
            % fmt.bold(", ".join(conflict_deleted))
        )
    can_update_files = set(remote_modified.keys()) - set(conflict_modified)
    can_delete_files = set(remote_deleted.keys()) - set(conflict_deleted)
    if len(can_update_files) > 0 or len(can_delete_files) > 0:
        if len(can_update_files) > 0:
            fmt.echo(
                "Following files can be automatically UPDATED: %s"
                % fmt.bold(", ".join(can_update_files))
            )
        if len(can_delete_files) > 0:
            fmt.echo(
                "Following files can be automatically DELETED: %s"
                % fmt.bold(", ".join(can_delete_files))
            )
        prompt = (
            "Should incoming changes be Skipped, Applied (local changes will be lost) or Merged (%s"
            " UPDATED | %s DELETED | all local changes remain)?"
            % (fmt.bold(",".join(can_update_files)), fmt.bold(",".join(can_delete_files)))
        )
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
        remote_modified = {n: e for n, e in remote_modified.items() if n in can_update_files}
        remote_deleted = {n: e for n, e in remote_deleted.items() if n in can_delete_files}
    else:
        # fully overwrite, leave all files to be copied
        fmt.echo("Applying all incoming changes to local files.")

    return resolution, remote_modified, remote_deleted


def _welcome_message(
    source_name: str,
    destination_type: str,
    source_configuration: SourceConfiguration,
    dependency_system: str,
    is_new_source: bool,
    added_pipeline_script: bool = True,
) -> None:
    new_entity_type = "pipeline" if destination_type else "source"
    fmt.echo()
    if source_configuration.source_type in ["template", "core"]:
        fmt.echo(
            "Your new %s %s is ready to be customized!" % (new_entity_type, fmt.bold(source_name))
        )
        if added_pipeline_script:
            fmt.echo(
                "* Review and change how dlt loads your data in %s"
                % fmt.bold(source_configuration.dest_pipeline_script)
            )
    else:
        if is_new_source:
            fmt.echo("Verified source %s was added to your project!" % fmt.bold(source_name))
            if added_pipeline_script:
                fmt.echo(
                    "* See the usage examples and code snippets to copy from %s"
                    % fmt.bold(source_configuration.dest_pipeline_script)
                )
        else:
            fmt.echo(
                "Verified source %s was updated to the newest version!" % fmt.bold(source_name)
            )

    if is_new_source:
        destination_str = " for %s" % fmt.bold(destination_type) if destination_type else ""
        fmt.echo(
            "* Add credentials%s and other secrets to %s"
            % (destination_str, fmt.bold(utils.make_dlt_settings_path(SECRETS_TOML)))
        )

    if destination_type == "destination":
        fmt.echo(
            "* You have selected the custom destination as your pipelines destination. Please refer"
            " to our docs at https://dlthub.com/docs/dlt-ecosystem/destinations/destination on how"
            " to add a destination function that will consume your data."
        )

    if dependency_system:
        fmt.echo("* Add the required dependencies to %s:" % fmt.bold(dependency_system))
        compiled_requirements = source_configuration.requirements.compiled()
        for dep in compiled_requirements:
            fmt.echo("  " + fmt.bold(dep))
        if destination_type:
            fmt.echo(
                "  If the dlt dependency is already added, make sure you install the extra for %s"
                " to it"
                % fmt.bold(destination_type)
            )
        if dependency_system == utils.REQUIREMENTS_TXT:
            qs = "' '"
            fmt.echo(
                "  To install with pip: %s"
                % fmt.bold(f"pip3 install '{qs.join(compiled_requirements)}'")
            )
        elif dependency_system == utils.PYPROJECT_TOML:
            fmt.echo("  If you are using poetry you may issue the following command:")
            fmt.echo(fmt.bold("  poetry add %s -E %s" % (DLT_PKG_NAME, destination_type)))
        fmt.echo()
    else:
        fmt.echo(
            "* %s was created. Install it with:\npip3 install -r %s"
            % (fmt.bold(utils.REQUIREMENTS_TXT), utils.REQUIREMENTS_TXT)
        )

    if is_new_source and new_entity_type == "pipeline":
        fmt.echo(
            "* Read %s for more information"
            % fmt.bold("https://dlthub.com/docs/walkthroughs/create-a-pipeline")
        )
    else:
        fmt.echo(
            "* Read %s for more information"
            % fmt.bold("https://dlthub.com/docs/walkthroughs/add-a-verified-source")
        )


def _get_dependency_system(dest_storage: FileStorage) -> str:
    if dest_storage.has_file(utils.PYPROJECT_TOML):
        return utils.PYPROJECT_TOML
    elif dest_storage.has_file(utils.REQUIREMENTS_TXT):
        return utils.REQUIREMENTS_TXT
    else:
        return None


def _list_template_sources() -> Dict[str, SourceConfiguration]:
    template_storage = _get_templates_storage()
    sources: Dict[str, SourceConfiguration] = {}
    for source_name in files_ops.get_sources_names(template_storage, source_type="template"):
        sources[source_name] = files_ops.get_template_configuration(
            template_storage, source_name, source_name
        )
    return sources


def _list_core_sources() -> Dict[str, SourceConfiguration]:
    core_sources_storage = _get_core_sources_storage()

    sources: Dict[str, SourceConfiguration] = {}
    for source_name in files_ops.get_sources_names(core_sources_storage, source_type="core"):
        sources[source_name] = files_ops.get_core_source_configuration(
            core_sources_storage, source_name, eject_source=False
        )
    return sources


def _list_verified_sources(
    repo_location: str, branch: str = None
) -> Dict[str, SourceConfiguration]:
    verified_sources_storage = _clone_and_get_verified_sources_storage(repo_location, branch)

    sources: Dict[str, SourceConfiguration] = {}
    for source_name in files_ops.get_sources_names(
        verified_sources_storage, source_type="verified"
    ):
        try:
            sources[source_name] = files_ops.get_verified_source_configuration(
                verified_sources_storage, source_name
            )
        except Exception as ex:
            fmt.warning(f"Verified source {source_name} not available: {ex}")

    return sources


def _list_core_destinations() -> list[str]:
    return dlt.destinations.__all__
