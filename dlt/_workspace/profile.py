import os

from dlt.common.configuration.specs.pluggable_run_context import RunContextBase

DEFAULT_PROFILE = "dev"
PIN_FILE_NAME = "profile-name"

BUILT_IN_PROFILES = {
    "dev": "dev profile, workspace default",
    "prod": "production profile, assumed by pipelines deployed in Runtime",
    "tests": "profile assumed when running tests",
    "access": (
        "production profile, assumed by interactive notebooks in Runtime, typically with limited"
        " access rights"
    ),
}
LOCAL_PROFILES = ["dev", "tests"]
"""Profiles used in local workspace"""


def get_profile_pin_file(context: RunContextBase) -> str:
    return context.get_setting(PIN_FILE_NAME)


def read_profile_pin(context: RunContextBase) -> str:
    # load profile name from .profile-name
    profile_pin = get_profile_pin_file(context)
    if os.path.isfile(profile_pin):
        with open(profile_pin, "tr", encoding="utf-8") as pin:
            return pin.readline().strip()
    return None


def save_profile_pin(context: RunContextBase, profile: str) -> None:
    # load profile name from .profile-name
    profile_pin = get_profile_pin_file(context)
    with open(profile_pin, "wt", encoding="utf-8") as pin:
        pin.write(profile)
