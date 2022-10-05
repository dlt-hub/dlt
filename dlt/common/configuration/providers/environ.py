from os import environ
from os.path import isdir
from typing import Any, Optional, Type, Tuple

from dlt.common.typing import TSecretValue

from .provider import Provider

SECRET_STORAGE_PATH: str = "/run/secrets/%s"

class EnvironProvider(Provider):

    @staticmethod
    def get_key_name(key: str, *namespaces: str) -> str:
        # env key is always upper case
        if namespaces:
            namespaces = filter(lambda x: bool(x), namespaces)
            env_key = "__".join((*namespaces, key))
        else:
            env_key = key
        return env_key.upper()

    @property
    def name(self) -> str:
        return "Environment Variables"

    def get_value(self, key: str, hint: Type[Any], *namespaces: str) -> Tuple[Optional[Any], str]:
        # apply namespace to the key
        key = self.get_key_name(key, *namespaces)
        if hint is TSecretValue:
            # try secret storage
            try:
                # must conform to RFC1123
                secret_name = key.lower().replace("_", "-")
                secret_path = SECRET_STORAGE_PATH % secret_name
                # kubernetes stores secrets as files in a dir, docker compose plainly
                if isdir(secret_path):
                    secret_path += "/" + secret_name
                with open(secret_path, "r", encoding="utf-8") as f:
                    secret = f.read()
                # add secret to environ so forks have access
                # TODO: removing new lines is not always good. for password OK for PEMs not
                # TODO: in regular secrets that is dealt with in particular configuration logic
                environ[key] = secret.strip()
                # do not strip returned secret
                return secret, key
            # includes FileNotFound
            except OSError:
                pass
        return environ.get(key, None), key

    @property
    def is_secret(self) -> bool:
        return True
