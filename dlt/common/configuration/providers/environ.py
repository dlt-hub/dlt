from os import environ
from os.path import isdir
from typing import Any, Optional, Type

from dlt.common.typing import TSecretValue

SECRET_STORAGE_PATH: str = "/run/secrets/%s"


def get_key_name(key: str, namespace: str = None) -> str:
    if namespace:
        return namespace + "__" + key
    else:
        return key


def get_key(key: str, hint: Type[Any], namespace: str = None) -> Optional[str]:
    # apply namespace to the key
    key = get_key_name(key, namespace)
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
            return secret
        # includes FileNotFound
        except OSError:
            pass
    return environ.get(key, None)