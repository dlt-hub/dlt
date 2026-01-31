import contextlib
import io
import sys
import binascii
import pickle
import base64
from typing import Any, Sequence

from dlt.common.utils import digest128b


class _ClassMeta(type):
    """Allows for type to have attributes that are not defined in the class."""

    def __getattr__(cls, name: str) -> Any:
        return name


class MissingUnpickledType(object, metaclass=_ClassMeta):
    def __init__(*args: Any, **kwargs: Any) -> None:
        pass


class SynthesizingUnpickler(pickle.Unpickler):
    """Unpickler that synthesizes missing types instead of raising"""

    def find_class(self, module: str, name: str) -> Any:
        full_name = f"{module}.{name}"
        module_obj = sys.modules[__name__]
        with contextlib.suppress(AttributeError):
            return getattr(module_obj, full_name)
        try:
            return super().find_class(module, name)
        except Exception:
            # synthesize type if class not found or deserialization failed (ie. version mismatch)
            t = type(name, (MissingUnpickledType,), {"__module__": module})
            setattr(module_obj, full_name, t)
            return t


def encode_obj(o: Any, ignore_pickle_errors: bool = True) -> str:
    try:
        # sign dump and return
        dump = pickle.dumps(o)
        return digest128b(dump) + base64.b64encode(dump).decode("ascii")
    except Exception:
        # unfortunately most of the pickle exceptions does not derive from pickle.PickleError
        if ignore_pickle_errors:
            return None
        else:
            raise


def decode_obj(line: str, ignore_pickle_errors: bool = True) -> Any:
    if len(line) <= 20:
        return None
    try:
        dump = base64.b64decode(line[20:], validate=True)
        # check signature
        if digest128b(dump) != line[:20]:
            return None
        with io.BytesIO(dump) as s:
            return SynthesizingUnpickler(s).load()
    except binascii.Error:
        return None
    except Exception:
        # unfortunately most of the pickle exceptions does not derive from pickle.PickleError
        if ignore_pickle_errors:
            return None
        else:
            raise


def decode_last_obj(lines: Sequence[str], ignore_pickle_errors: bool = True) -> Any:
    if not lines:
        return None
    for line in reversed(lines):
        obj = decode_obj(line, ignore_pickle_errors=ignore_pickle_errors)
        if obj is not None:
            return obj
    return None
