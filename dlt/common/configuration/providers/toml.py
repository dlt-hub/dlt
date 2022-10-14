import os
import inspect
import dataclasses
import tomlkit
from inspect import Signature, Parameter
from typing import Any, List, Type
# from makefun import wraps
from functools import wraps

from dlt.common.typing import DictStrAny, StrAny, TAny, TFun
from dlt.common.configuration import make_configuration, is_valid_hint
from dlt.common.configuration.specs import BaseConfiguration


def _read_toml(file_name: str) -> StrAny:
    config_file_path = os.path.abspath(os.path.join(".", "experiments/.dlt", file_name))

    if os.path.isfile(config_file_path):
        with open(config_file_path, "r", encoding="utf-8") as f:
            # use whitespace preserving parser
            return tomlkit.load(f)
    else:
        return {}