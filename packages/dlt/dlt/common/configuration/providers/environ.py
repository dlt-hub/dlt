from os import environ
from os.path import isdir
from typing import Any, Optional, Type, Tuple

from dlt.common.configuration.specs.base_configuration import is_secret_hint

from .provider import ConfigProvider, get_key_name

SECRET_STORAGE_PATH: str = "/run/secrets/%s"


class EnvironProvider(ConfigProvider):
    @staticmethod
    def get_key_name(key: str, *sections: str) -> str:
        return get_key_name(key, "__", *sections).upper()

    @property
    def name(self) -> str:
        return "Environment Variables"

    def get_value(
        self, key: str, hint: Type[Any], pipeline_name: str, *sections: str
    ) -> Tuple[Optional[Any], str]:
        # apply section to the key
        key = self.get_key_name(key, pipeline_name, *sections)
        if is_secret_hint(hint):
            # try secret storage
            try:
                # must conform to RFC1123 DNS LABELS (https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-label-names)
                secret_name = key.lower().replace("_", "-")
                secret_path = SECRET_STORAGE_PATH % secret_name
                # kubernetes stores secrets as files in a dir, docker compose plainly
                if isdir(secret_path):
                    secret_path += "/" + secret_name
                with open(secret_path, "r", encoding="utf-8") as f:
                    secret = f.read()
                # add secret to environ so forks have access
                # warning: removing new lines is not always good. for password OK for PEMs not
                # warning: in regular secrets that is dealt with in particular configuration logic
                environ[key] = secret.strip()
                # do not strip returned secret
                return secret, key
            # includes FileNotFound
            except OSError:
                pass
        return environ.get(key, None), key

    @property
    def supports_secrets(self) -> bool:
        return True

    @property
    def supports_sections(self) -> bool:
        return True

    @property
    def is_empty(self) -> bool:
        return len(environ) == 0
