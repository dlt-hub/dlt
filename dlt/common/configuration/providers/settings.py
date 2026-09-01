"""Shared logic for file-based settings providers (`toml.py`, `yaml.py`).

`SettingsTomlProvider` and `SettingsYamlProvider` differ only in how they read and write their
backing files (and, for TOML, in keeping a `tomlkit` document in sync for comment/formatting
preservation and in the Google Colab / Streamlit fallbacks). Everything else -- resolving profile
aware paths, tracking where each value came from (`_value_origins`) and undoing writes on
`preserve()` -- is format agnostic and lives here.
"""

import os
from contextlib import contextmanager
from pathlib import PurePath
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

from dlt.common.configuration.exceptions import ConfigProviderException
from dlt.common.configuration.utils import auto_config_fragment

from .doc import CustomLoaderDocProvider

GLOBAL_ORIGIN_PREFIX = "global:"


class TValueOrigins(Dict[str, Any]):
    """Mirrors a config doc shape: leaf values are origin names, tables carry their own origin."""

    origin: str = ""

    def clone(self) -> "TValueOrigins":
        cloned = TValueOrigins(
            {k: v.clone() if isinstance(v, TValueOrigins) else v for k, v in self.items()}
        )
        cloned.origin = self.origin
        return cloned


class SettingsDocProvider(CustomLoaderDocProvider):
    """Base class for providers backed by one or more settings files (toml, yaml, ...).

    Subclasses are responsible for resolving file paths, reading/merging/writing the files
    in their native format and keeping any format specific representation (ie. a `tomlkit`
    document) they need for round-tripping. This class implements the rest: tracking the file
    (or fallback source) each value came from, and restoring that bookkeeping on `preserve()`.
    """

    _present_locations: List[str]
    _global_dir: Optional[str]
    _value_origins: TValueOrigins

    def get_value_location(self, key: str, pipeline_name: Optional[str], *sections: str) -> str:
        """Get location (file name) of a value from the origins doc built when files were loaded.
        Values from it are located with a `global:` prefix so that files sharing a name are told apart.
        """
        node = self._origins_node(self.get_key_path(key, pipeline_name, *sections))
        if node is None:
            return ""
        return node.origin if isinstance(node, TValueOrigins) else node

    def set_value(self, key: str, value: Any, pipeline_name: Optional[str], *sections: str) -> None:
        self._drop_origin(self.get_key_path(key, pipeline_name, *sections))
        super().set_value(key, value, pipeline_name, *sections)

    def set_fragment(
        self, key: Optional[str], value_or_fragment: str, pipeline_name: str, *sections: str
    ) -> None:
        if (fragment := auto_config_fragment(value_or_fragment)) is not None:
            if key is None:
                self._value_origins = TValueOrigins()
            else:
                self._drop_origins(fragment)
        else:
            self._drop_origin(self.get_key_path(key, pipeline_name, *sections))
        super().set_fragment(key, value_or_fragment, pipeline_name, *sections)

    @contextmanager
    def preserve(self) -> Iterator[None]:
        saved_origins = self._value_origins.clone()
        with super().preserve():
            try:
                yield
            finally:
                self._value_origins = saved_origins

    @property
    def present_locations(self) -> List[str]:
        return self._present_locations

    def _is_in_global_dir(self, path: str) -> bool:
        """Tells if `path` sits in the global dir. On Windows paths compare case insensitively."""
        if not self._global_dir:
            return False
        # PurePath folds case and separators on Windows but keeps ".." verbatim, abspath resolves it
        return PurePath(os.path.abspath(path)).is_relative_to(os.path.abspath(self._global_dir))

    def _path_origin(self, path: str) -> str:
        """Names the file `path`, marking the global dir so files sharing a name are told apart."""
        if self._is_in_global_dir(path):
            return GLOBAL_ORIGIN_PREFIX + os.path.basename(path)
        return os.path.basename(path)

    @staticmethod
    def _doc_origins(doc: Mapping[str, Any], origin: str) -> TValueOrigins:
        """Mirrors `doc` shape replacing each value with `origin`, tables carry `origin` as well."""
        origins = TValueOrigins()
        origins.origin = origin
        for k, v in doc.items():
            origins[k] = (
                SettingsDocProvider._doc_origins(v, origin) if isinstance(v, dict) else origin
            )
        return origins

    def _origins_node(self, full_path: Sequence[str]) -> Any:
        """Returns origins doc node under `full_path` or None if not known."""
        node: Any = self._value_origins
        for k in full_path:
            if not isinstance(node, dict) or k not in node:
                return None
            node = node[k]
        return node

    def _drop_origin(self, full_path: Sequence[str]) -> None:
        """Forgets origin of a value under `full_path`, dropping whole subtree for tables."""
        if not full_path:
            return
        parent = self._origins_node(full_path[:-1])
        if isinstance(parent, dict):
            parent.pop(full_path[-1], None)

    def _drop_origins(self, doc: Mapping[str, Any], path: Tuple[str, ...] = ()) -> None:
        """Forgets origins of all values present in `doc`, a fragment merged from the root."""
        if not isinstance(doc, dict):
            return
        for k, v in doc.items():
            sub_path = path + (k,)
            # a fragment table merges key by key, but it replaces a value that was not a table
            if isinstance(v, dict) and isinstance(self._origins_node(sub_path), dict):
                self._drop_origins(v, sub_path)
            else:
                self._drop_origin(sub_path)


class SettingsProviderReadException(ConfigProviderException):
    def __init__(
        self, provider_name: str, file_name: str, full_paths: List[str], doc_exception: str
    ) -> None:
        self.file_name = file_name
        self.full_paths = full_paths
        msg = f"A problem encountered when loading {provider_name} from paths {full_paths}:\n"
        msg += doc_exception
        super().__init__(provider_name, msg)
