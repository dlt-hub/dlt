"""Staged workspace file/secret writes with atomic commit."""

import os
import shutil
from typing import Any, Dict, List

from dlt.common.configuration.providers import (
    ConfigTomlProvider,
    SecretsTomlProvider,
)
from dlt.common.storages.file_storage import FileStorage

from dlt._workspace.cli import echo as fmt
from dlt._workspace.cli.config_toml_writer import WritableConfigValue, write_values
from dlt._workspace.cli.exceptions import CliCommandException


class WorkspaceWriteState:
    """Stages workspace file/secret/config writes for atomic commit."""

    def __init__(self, dest_storage: FileStorage, settings_dir: str) -> None:
        self.dest_storage = dest_storage
        self.settings_dir = settings_dir

        self.dirs_to_create: List[str] = []
        self.new_files: List[Dict[str, Any]] = []
        self.file_copies: List[Dict[str, Any]] = []
        self.pending_secrets: List[WritableConfigValue] = []
        self.pending_config: List[WritableConfigValue] = []

        self._committed_copies: Dict[str, str] = {}

    def add_new_file(self, path: str, content: str, *, accept_existing: bool = False) -> None:
        """Stage a file with inline content."""
        self.new_files.append(
            {
                "path": path,
                "content": content,
                "accept_existing": accept_existing,
            }
        )

    def add_file_copy(
        self, src_path: str, dest_path: str, *, accept_existing: bool = False
    ) -> None:
        """Stage a file copy via `shutil.copy2`."""
        self.file_copies.append(
            {
                "src_path": src_path,
                "dest_path": dest_path,
                "accept_existing": accept_existing,
            }
        )

    def add_secrets_value(self, value: WritableConfigValue) -> None:
        """Stage a `secrets.toml` entry."""
        self.pending_secrets.append(value)

    def add_config_value(self, value: WritableConfigValue) -> None:
        """Stage a `config.toml` entry."""
        self.pending_config.append(value)

    def preview(self) -> Dict[str, str]:
        """Return `{dest_path: content}` for everything that would be written."""
        result: Dict[str, str] = {}
        for f in self.new_files:
            if f["accept_existing"] and os.path.exists(f["path"]):
                continue
            result[f["path"]] = f["content"]
        for c in self.file_copies:
            if c["accept_existing"] and os.path.exists(c["dest_path"]):
                continue
            try:
                with open(c["src_path"], "r", encoding="utf-8") as fh:
                    result[c["dest_path"]] = fh.read()
            except UnicodeDecodeError:
                # binary file — drop from preview, real commit still copies via shutil
                fmt.warning(
                    f"File {c['src_path']} was skipped, not a text file. It will not be"
                    f" copied to {c['dest_path']}"
                )
        return result

    def check_file_conflicts(self) -> None:
        """Raise `CliCommandException` if any non-`accept_existing` target already exists."""
        conflicts: List[str] = []
        for f in self.new_files:
            if not f["accept_existing"] and os.path.exists(f["path"]):
                conflicts.append(f["path"])
        for c in self.file_copies:
            if not c["accept_existing"] and os.path.exists(c["dest_path"]):
                conflicts.append(c["dest_path"])
        if conflicts:
            for path in conflicts:
                fmt.error(f"File conflict: {fmt.bold(path)} already exists.")
            raise CliCommandException()

    def commit(self, *, allow_overwrite: bool = False) -> Dict[str, str]:
        """Write all staged changes to disk.

        Args:
            allow_overwrite (bool): When `False`, raises on conflicts.

        Returns:
            Dict[str, str]: `{dest_path: src_path}` for performed file copies. Inline files
                are not included, matching the historical `init_pipeline_at_destination` shape.
        """
        if not allow_overwrite:
            self.check_file_conflicts()

        self._create_dirs()
        self._write_staged_files()
        self._after_files_hook()
        self._write_secrets_toml()
        self._write_config_toml()

        return dict(self._committed_copies)

    def _create_dirs(self) -> None:
        for d in self.dirs_to_create:
            os.makedirs(d, exist_ok=True)
        for f in self.new_files:
            parent = os.path.dirname(f["path"])
            if parent:
                os.makedirs(parent, exist_ok=True)
        for c in self.file_copies:
            parent = os.path.dirname(c["dest_path"])
            if parent:
                os.makedirs(parent, exist_ok=True)
        if self.pending_secrets or self.pending_config:
            os.makedirs(self._resolve_settings_path(), exist_ok=True)

    def _write_staged_files(self) -> None:
        for f in self.new_files:
            if f["accept_existing"] and os.path.exists(f["path"]):
                continue
            with open(f["path"], "w", encoding="utf-8") as fh:
                fh.write(f["content"])
        for c in self.file_copies:
            if c["accept_existing"] and os.path.exists(c["dest_path"]):
                continue
            shutil.copy2(c["src_path"], c["dest_path"])
            self._committed_copies[c["dest_path"]] = c["src_path"]

    def _after_files_hook(self) -> None:
        """Subclass extension point between file writes and toml writes."""
        return None

    def _write_secrets_toml(self) -> None:
        if not self.pending_secrets:
            return
        provider = SecretsTomlProvider(self.settings_dir)
        write_values(provider._config_toml, self.pending_secrets, overwrite_existing=False)
        provider.write_toml()

    def _write_config_toml(self) -> None:
        if not self.pending_config:
            return
        provider = ConfigTomlProvider(self.settings_dir)
        write_values(provider._config_toml, self.pending_config, overwrite_existing=False)
        provider.write_toml()

    def _resolve_settings_path(self) -> str:
        if os.path.isabs(self.settings_dir):
            return self.settings_dir
        return os.path.join(self.dest_storage.storage_path, self.settings_dir)
