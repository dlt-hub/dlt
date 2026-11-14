import os

import pytest

from dlt._workspace.settings_files import (
    create_profile_settings,
    load_yaml_file,
    profile_settings_files,
    save_yaml_file,
)

from tests.utils import get_test_storage_root
from tests.workspace.utils import isolated_workspace


def test_create_profile_settings() -> None:
    with isolated_workspace("empty") as ctx:
        config, secrets = profile_settings_files(ctx, "ci-write")
        with open(secrets, "w", encoding="utf-8") as f:
            f.write("key = 1\n")
        # only the missing files are created, an existing one is kept as is
        assert create_profile_settings(ctx, ["ci-write", "ci-read"]) == [
            config,
            *profile_settings_files(ctx, "ci-read"),
        ]
        with open(secrets, encoding="utf-8") as f:
            assert f.read() == "key = 1\n"
        with open(config, encoding="utf-8") as f:
            assert f.read().startswith("# settings of profile ci-write")
        assert create_profile_settings(ctx, ["ci-write"]) == []


def test_yaml_files() -> None:
    path = os.path.join(get_test_storage_root(), "settings", "doc.yaml")
    assert load_yaml_file(path) == {}
    save_yaml_file(path, {"engine_version": 1, "profiles": {"dev": {"location": "local"}}})
    assert load_yaml_file(path) == {"engine_version": 1, "profiles": {"dev": {"location": "local"}}}
    with open(path, "w", encoding="utf-8") as f:
        f.write("")
    assert load_yaml_file(path) == {}
    with open(path, "w", encoding="utf-8") as f:
        f.write("- a\n- b\n")
    with pytest.raises(ValueError, match="must contain a yaml mapping"):
        load_yaml_file(path)
    with open(path, "w", encoding="utf-8") as f:
        f.write("a: [\n")
    with pytest.raises(ValueError, match="not valid yaml"):
        load_yaml_file(path)
