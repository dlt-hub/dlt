import tomlkit
import yaml
from typing import Any, Callable, Dict, MutableMapping, Sequence, Optional, Tuple, Type

from dlt.common.configuration.utils import auto_cast, auto_config_fragment
from dlt.common.utils import update_dict_nested

from .provider import ConfigProvider, get_key_name


class BaseDocProvider(ConfigProvider):
    _config_doc: Dict[str, Any]
    """Holds a dict with config values"""

    def __init__(self, config_doc: Dict[str, Any]) -> None:
        self._config_doc = config_doc

    @staticmethod
    def get_key_name(key: str, *sections: str) -> str:
        return get_key_name(key, ".", *sections)

    def get_value(
        self, key: str, hint: Type[Any], pipeline_name: str, *sections: str
    ) -> Tuple[Optional[Any], str]:
        full_path = sections + (key,)
        if pipeline_name:
            full_path = (pipeline_name,) + full_path
        full_key = self.get_key_name(key, pipeline_name, *sections)
        node = self._config_doc
        try:
            for k in full_path:
                if not isinstance(node, dict):
                    raise KeyError(k)
                node = node[k]
            return node, full_key
        except KeyError:
            return None, full_key

    def set_value(self, key: str, value: Any, pipeline_name: Optional[str], *sections: str) -> None:
        """Sets `value` under `key` in `sections` and optionally for `pipeline_name`

        If key already has value of type dict and value to set is also of type dict, the new value
        is merged with old value.
        """
        self._set_value(self._config_doc, key, value, pipeline_name, *sections)

    def set_fragment(
        self, key: Optional[str], value_or_fragment: str, pipeline_name: str, *sections: str
    ) -> None:
        """Tries to interpret `value_or_fragment` as a fragment of toml, yaml or json string and replace/merge into config doc.

        If `key` is not provided, fragment is considered a full document and will replace internal config doc. Otherwise
        fragment is merged with config doc from the root element and not from the element under `key`!

        For simple values it falls back to `set_value` method.
        """
        self._config_doc = self._set_fragment(
            self._config_doc, key, value_or_fragment, pipeline_name, *sections
        )

    def to_toml(self) -> str:
        return tomlkit.dumps(self._config_doc)

    def to_yaml(self) -> str:
        return yaml.dump(
            self._config_doc, allow_unicode=True, default_flow_style=False, sort_keys=False
        )

    @property
    def supports_sections(self) -> bool:
        return True

    @property
    def is_empty(self) -> bool:
        return len(self._config_doc) == 0

    @staticmethod
    def _set_value(
        master: MutableMapping[str, Any],
        key: str,
        value: Any,
        pipeline_name: Optional[str],
        *sections: str,
    ) -> None:
        if pipeline_name:
            sections = (pipeline_name,) + sections
        if key is None:
            raise ValueError("dlt_secrets_toml must contain toml document")

        # descend from root, create tables if necessary
        for k in sections:
            if not isinstance(master, dict):
                raise KeyError(k)
            if k not in master:
                master[k] = {}
            master = master[k]
        if isinstance(value, dict):
            # remove none values, TODO: we need recursive None removal
            value = {k: v for k, v in value.items() if v is not None}
            # if target is also dict then merge recursively
            if isinstance(master.get(key), dict):
                update_dict_nested(master[key], value)
                return
        master[key] = value

    @staticmethod
    def _set_fragment(
        master: MutableMapping[str, Any],
        key: Optional[str],
        value_or_fragment: str,
        pipeline_name: str,
        *sections: str,
    ) -> Any:
        """Tries to interpret `value_or_fragment` as a fragment of toml, yaml or json string and replace/merge into config doc.

        If `key` is not provided, fragment is considered a full document and will replace internal config doc. Otherwise
        fragment is merged with config doc from the root element and not from the element under `key`!

        For simple values it falls back to `set_value` method.
        """
        fragment = auto_config_fragment(value_or_fragment)
        if fragment is not None:
            # always update the top document
            if key is None:
                master = fragment
            else:
                # TODO: verify that value contains only the elements under key
                update_dict_nested(master, fragment)
        else:
            # set value using auto_cast
            BaseDocProvider._set_value(
                master, key, auto_cast(value_or_fragment), pipeline_name, *sections
            )
        return master


class CustomLoaderDocProvider(BaseDocProvider):
    def __init__(
        self,
        name: str,
        loader: Callable[[], Dict[str, Any]],
        supports_secrets: bool = True,
        locations: Sequence[str] = None,
    ) -> None:
        """Provider that calls `loader` function to get a Python dict with config/secret values to be queried.
        The `loader` function typically loads a string (ie. from file), parses it (ie. as toml or yaml), does additional
        processing and returns a Python dict to be queried.

        Instance of CustomLoaderDocProvider must be registered for the returned dict to be used to resolve config values.
        >>> import dlt
        >>> dlt.config.register_provider(provider)

        Args:
            name(str): name of the provider that will be visible ie. in exceptions
            loader(Callable[[], Dict[str, Any]]): user-supplied function that will load the document with config/secret values
            supports_secrets(bool): allows to store secret values in this provider
            locations(Sequence[str], optional): a human readable list of locations from which configuration is read, used to
                display meaningful error messages when configurations was not resolved.

        """
        self._name = name
        self._supports_secrets = supports_secrets
        self._locations = locations
        super().__init__(loader())

    @property
    def name(self) -> str:
        return self._name

    @property
    def supports_secrets(self) -> bool:
        return self._supports_secrets

    @property
    def is_writable(self) -> bool:
        return True

    @property
    def locations(self) -> Sequence[str]:
        return self._locations
