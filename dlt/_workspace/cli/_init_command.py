import os
import ast
import warnings
from importlib.metadata import Distribution
from typing import Dict, Optional, Sequence, Tuple

from dlt.common.libs import git
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.providers import SECRETS_TOML
from dlt.common.pipeline import get_dlt_repos_dir
from dlt.version import DLT_PKG_NAME, __version__
from dlt.common.destination import Destination
from dlt.common.reflection.utils import rewrite_python_script
from dlt.common.runtime import run_context
from dlt.common.schema.utils import is_valid_schema_name
from dlt.common.schema.exceptions import InvalidSchemaName
from dlt.common.storages.file_storage import FileStorage

import dlt.destinations
from dlt.sources import SourceReference
import dlt.reflection.names as n
from dlt.reflection.script_inspector import import_pipeline_script

from dlt._workspace.cli import echo as fmt, _pipeline_files as files_ops, source_detection, utils
from dlt._workspace.cli.config_toml_writer import WritableConfigValue
from dlt._workspace.cli._pipeline_files import (
    TEMPLATE_FILES,
    SOURCES_MODULE_NAME,
    SINGLE_FILE_TEMPLATE_MODULE_NAME,
    SourceConfiguration,
    TVerifiedSourceFileEntry,
    TVerifiedSourceFileIndex,
)
from dlt._workspace.cli._write_state import WorkspaceWriteState
from dlt._workspace.cli.exceptions import CliCommandException, CliCommandInnerException
from dlt._workspace.cli._urls import DLT_INIT_DOCS_URL, DLT_AI_DOCS_URL  # noqa: F401


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
    # detect and warn on deprecated dlthub: pattern
    display_source_name: Optional[str] = None
    if source_name.startswith("dlthub:"):
        display_source_name = source_name[7:]
        fmt.warning(
            "The `dlthub:<source>` syntax is deprecated. User dltHub AI Workbench instead:\n"
            "%s\n"
            "will get you started. See %s for more details."
            % (fmt.cli_cmd("ai init"), fmt.bold(DLT_AI_DOCS_URL))
        )
        source_name = "context_rest_api"

    run_ctx = run_context.active()
    destination_storage_path = run_ctx.run_dir
    settings_dir = run_ctx.settings_dir
    sources_dir = run_ctx.get_run_entity("sources")

    return init_pipeline_at_destination(
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
        display_source_name=display_source_name,
    )


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
    display_source_name: str = None,
) -> Tuple[Optional[Dict[str, str]], files_ops.TSourceType]:
    """Scaffold a pipeline (config, secrets, optional example script) for `source_name` → `destination_type`.

    Args:
        source_name (str): Name of the source to initialize.
        destination_type (str): Destination name (e.g. "bigquery", "redshift").
        repo_location (str): Verified-sources repository URL or local path.
        branch (str): Branch in `repo_location` to fetch from.
        eject_source (bool): When True, copy the core source's code into the project so the user can edit it.
        dry_run (bool): When True, return the planned file copies without writing.
        add_example_pipeline_script (bool): When True, generate an example pipeline script.
        destination_storage_path (str): Project root to write into.
        settings_dir (str): Directory for `config.toml` / `secrets.toml`.
        sources_dir (str): Directory under which verified sources are copied.
        target_dependency_system (str): `"requirements.txt"` or `"pyproject.toml"`; controls the welcome message.
        display_source_name (str): User-facing source name (e.g. for deprecated `dlthub:<source>` syntax).

    Returns:
        `(copied_files, source_type)` where `copied_files` maps destination paths to source paths
        (or `None` on dry-run) and `source_type` is `"template"`, `"core"`, or `"verified"`.
    """
    # validate the user-facing name (display_source_name for dlthub: sources)
    name_to_validate = display_source_name or source_name
    # source and destination names are used as Python identifiers in generated code
    if not is_valid_schema_name(name_to_validate):
        fmt.error(
            "Source name %s is not a valid Python identifier. Use snake_case names"
            " containing only lowercase letters, numbers and underscores (max %d"
            " characters)."
            % (fmt.bold(name_to_validate), InvalidSchemaName.MAXIMUM_SCHEMA_NAME_LENGTH)
        )
        raise CliCommandException()
    # try to import the destination and get config spec
    if destination_type:
        destination_reference = Destination.from_reference(destination_type)
        destination_spec = destination_reference.spec

    # lookup core storages
    core_sources_storage = files_ops.get_core_sources_storage()
    templates_storage = files_ops.get_single_file_templates_storage()

    # discover type of source
    source_type: files_ops.TSourceType = "template"
    if source_name in files_ops.get_sources_names(core_sources_storage, source_type="core"):
        source_type = "core"
    # skip verified sources lookup for dlthub: sources (always use template)
    elif not display_source_name:
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
            return None, source_type

        if remote_index["is_dirty"]:
            fmt.warning(
                f"The verified sources repository is dirty. {source_name} source files may"
                " not update correctly in the future."
            )

    else:
        if source_type == "core":
            source_configuration = files_ops.get_core_source_configuration(
                core_sources_storage, source_name, eject_source
            )
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
            source_configuration = files_ops.get_template_configuration(
                templates_storage, source_name, display_source_name or source_name
            )

        if dest_storage.has_file(source_configuration.dest_pipeline_script):
            fmt.warning(
                "Pipeline script %s already exists, exiting"
                % source_configuration.dest_pipeline_script
            )
            return None, source_type

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
            return None, source_type

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
            ("destination", destination_type or "duckdb", True),
        ],
        source_configuration.src_pipeline_script,
    )

    # inspect the script to populate source references
    # suppress warnings emitted during import (e.g. psutil missing from LogCollector)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        if source_configuration.source_type != "core":
            import_pipeline_script(
                source_configuration.storage.storage_path,
                source_configuration.storage.to_relative_path(
                    source_configuration.src_pipeline_script
                ),
                ignore_missing_imports=True,
            )
        else:
            # core sources are imported directly from the pipeline script
            # which is in the _workspace module
            import_pipeline_script(
                os.path.dirname(source_configuration.src_pipeline_script),
                os.path.basename(source_configuration.src_pipeline_script),
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
        # dlthub: sources get extra AST transforms: rename pipeline, dataset, sources
        if display_source_name:
            transformed_nodes = source_detection.find_call_arguments_to_replace(
                visitor,
                [
                    ("destination", destination_type or "duckdb", True),
                    ("pipeline_name", display_source_name + "_pipeline", True),
                    ("dataset_name", display_source_name + "_data", False),
                ],
                source_configuration.src_pipeline_script,
            )
            for source_q_name, source_config in checked_sources.items():
                if source_q_name not in visitor.known_sources_resources:
                    raise CliCommandInnerException(
                        "init",
                        "The pipeline script %s imports a source/resource %s from"
                        " section %s. In init scripts you must declare all sources"
                        " and resources in single file. Known names are %s."
                        % (
                            source_configuration.src_pipeline_script,
                            source_config.name,
                            source_config.section,
                            list(visitor.known_sources_resources.keys()),
                        ),
                    )
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

    # ask for confirmation
    if is_new_source:
        if source_configuration.source_type == "core":
            fmt.echo(
                "Creating a new pipeline with the dlt core source %s (%s)"
                % (fmt.bold(source_name), source_configuration.doc)
            )
            if eject_source:
                fmt.echo(
                    "NOTE: Source code of %s will be ejected. Remember to modify the pipeline "
                    "example script to import the ejected source." % (fmt.bold(source_name))
                )
            else:
                fmt.echo(
                    "NOTE: Beginning with dlt 1.0.0, the source %s will no longer be copied from"
                    " the verified sources repo but imported from dlt.sources. You can provide the"
                    " --eject flag to revert to the old behavior." % (fmt.bold(source_name))
                )
        elif source_configuration.source_type == "verified":
            new_entity_type = "a new pipeline with" if destination_type else ""
            fmt.echo(
                "Creating and configuring %s the verified source %s (%s)"
                % (new_entity_type, fmt.bold(source_name), source_configuration.doc)
            )
        else:
            if source_configuration.is_default_template:
                fmt.echo(
                    "NOTE: Could not find a dlt source or template with the name %s. Selecting the"
                    " default template." % (fmt.bold(source_name))
                )
                fmt.echo(
                    "NOTE: In case you did not want to use the default template, run '%s'"
                    " to see all available sources and templates."
                    % fmt.cli_cmd("init -l")
                )
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
        source_name,
        destination_type,
        source_configuration,
        dependency_system,
        is_new_source,
        add_example_pipeline_script,
    )

    # stage all writes; commit at the very end
    state = WorkspaceWriteState(dest_storage, settings_dir)

    for file_name in TEMPLATE_FILES:
        if templates_storage.has_file(file_name):
            state.add_file_copy(
                templates_storage.make_full_path(file_name),
                dest_storage.make_full_path(file_name),
                accept_existing=True,
            )

    # verified-source files: conflicts already resolved earlier in `_select_source_files`
    for file_name in remote_modified:
        state.add_file_copy(
            source_configuration.storage.make_full_path(file_name),
            dest_storage.make_full_path(os.path.join(sources_dir, file_name)),
        )

    pipeline_script_target_path = dest_storage.make_full_path(
        os.path.join(sources_dir, source_configuration.dest_pipeline_script)
    )

    for value in required_secrets.values():
        state.add_secrets_value(value)
    for value in required_config.values():
        state.add_config_value(value)

    if dependency_system is None:
        state.add_new_file(
            dest_storage.make_full_path(utils.REQUIREMENTS_TXT),
            "\n".join(source_configuration.requirements.compiled()),
        )

    if dry_run:
        files_to_create = state.preview()
        if add_example_pipeline_script:
            files_to_create[pipeline_script_target_path] = dest_script_source
        # todo: handle remote index changes?
        return files_to_create, source_type

    copied_files = state.commit(allow_overwrite=True)

    if remote_index:
        for file_name in remote_deleted:
            if dest_storage.has_file(file_name):
                dest_storage.delete(file_name)
        files_ops.save_verified_source_local_index(
            source_name, remote_index, remote_modified, remote_deleted
        )

    if (
        not dest_storage.has_file(source_configuration.dest_pipeline_script)
        and add_example_pipeline_script
    ):
        dest_storage.save(pipeline_script_target_path, dest_script_source)

    return copied_files, source_type


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
        qs = "' '"
        if dependency_system == utils.REQUIREMENTS_TXT:
            fmt.echo(
                "  To install with pip: %s"
                % fmt.bold(f"pip3 install '{qs.join(compiled_requirements)}'")
            )
        elif dependency_system == utils.PYPROJECT_TOML:
            fmt.echo(
                "  To add with uv: %s" % fmt.bold(f"uv add '{qs.join(compiled_requirements)}'")
            )
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
    template_storage = files_ops.get_single_file_templates_storage()
    sources: Dict[str, SourceConfiguration] = {}
    for source_name in files_ops.get_sources_names(template_storage, source_type="template"):
        sources[source_name] = files_ops.get_template_configuration(
            template_storage, source_name, source_name
        )
    return sources


def _list_core_sources() -> Dict[str, SourceConfiguration]:
    core_sources_storage = files_ops.get_core_sources_storage()

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


def init_command_wrapper(
    source_name: str,
    destination_type: str,
    repo_location: str,
    branch: str,
    eject_source: bool = False,
) -> None:
    init_command(
        source_name,
        destination_type,
        repo_location,
        branch,
        eject_source,
    )


@utils.track_command("list_sources", False)
def list_sources_command_wrapper(repo_location: str, branch: str) -> None:
    list_sources_command(repo_location, branch)


@utils.track_command("list_destinations", False)
def list_destinations_command_wrapper() -> None:
    list_destinations_command()
