import os
from typing import List

import pytest

from dlt.common.storages.file_storage import FileStorage

from dlt._workspace.cli._write_state import WorkspaceWriteState
from dlt._workspace.cli.config_toml_writer import WritableConfigValue
from dlt._workspace.cli.exceptions import CliCommandException


def _make_state(tmp_path) -> WorkspaceWriteState:
    storage = FileStorage(str(tmp_path), makedirs=True)
    settings_dir = os.path.join(str(tmp_path), ".dlt")
    return WorkspaceWriteState(storage, settings_dir)


def test_add_new_file_writes_content(tmp_path) -> None:
    state = _make_state(tmp_path)
    target = str(tmp_path / "hello.txt")
    state.add_new_file(target, "hi\n")

    state.commit()

    assert os.path.isfile(target)
    with open(target, encoding="utf-8") as f:
        assert f.read() == "hi\n"


def test_add_file_copy_copies_bytes(tmp_path) -> None:
    src = tmp_path / "src.txt"
    src.write_text("payload\n", encoding="utf-8")
    dst = str(tmp_path / "out" / "dst.txt")

    state = _make_state(tmp_path)
    state.add_file_copy(str(src), dst)

    written = state.commit()

    assert os.path.isfile(dst)
    with open(dst, encoding="utf-8") as f:
        assert f.read() == "payload\n"
    # commit returns {dest: src} for file copies only
    assert written == {dst: str(src)}


def test_accept_existing_skips_existing_file(tmp_path) -> None:
    target = tmp_path / "secrets.toml"
    target.write_text("# user edits\n", encoding="utf-8")

    state = _make_state(tmp_path)
    state.add_new_file(str(target), "# default\n", accept_existing=True)

    state.commit()

    # user content preserved
    with open(target, encoding="utf-8") as f:
        assert f.read() == "# user edits\n"


def test_accept_existing_skips_existing_copy(tmp_path) -> None:
    src = tmp_path / "src.txt"
    src.write_text("new\n", encoding="utf-8")
    dst = tmp_path / "dst.txt"
    dst.write_text("old\n", encoding="utf-8")

    state = _make_state(tmp_path)
    state.add_file_copy(str(src), str(dst), accept_existing=True)

    written = state.commit()

    with open(dst, encoding="utf-8") as f:
        assert f.read() == "old\n"
    # skipped copies are not in the return dict
    assert written == {}


def test_check_file_conflicts_raises(tmp_path) -> None:
    existing = tmp_path / "pyproject.toml"
    existing.write_text("# theirs\n", encoding="utf-8")

    state = _make_state(tmp_path)
    state.add_new_file(str(existing), "# ours\n")

    with pytest.raises(CliCommandException):
        state.commit()
    # nothing written
    with open(existing, encoding="utf-8") as f:
        assert f.read() == "# theirs\n"


def test_commit_allow_overwrite_overwrites(tmp_path) -> None:
    existing = tmp_path / "pyproject.toml"
    existing.write_text("# theirs\n", encoding="utf-8")

    state = _make_state(tmp_path)
    state.add_new_file(str(existing), "# ours\n")

    state.commit(allow_overwrite=True)

    with open(existing, encoding="utf-8") as f:
        assert f.read() == "# ours\n"


def test_preview_returns_content_no_writes(tmp_path) -> None:
    src = tmp_path / "src.txt"
    src.write_text("from-source\n", encoding="utf-8")
    dst = str(tmp_path / "dst.txt")
    inline = str(tmp_path / "inline.txt")

    state = _make_state(tmp_path)
    state.add_file_copy(str(src), dst)
    state.add_new_file(inline, "from-inline\n")

    preview = state.preview()

    assert preview == {dst: "from-source\n", inline: "from-inline\n"}
    # no files actually written
    assert not os.path.isfile(dst)
    assert not os.path.isfile(inline)


def test_preview_skips_accept_existing_present(tmp_path) -> None:
    target = tmp_path / "secrets.toml"
    target.write_text("# user\n", encoding="utf-8")

    state = _make_state(tmp_path)
    state.add_new_file(str(target), "# default\n", accept_existing=True)

    # the existing file is preserved on commit, so preview should not list it
    assert state.preview() == {}


def test_pending_secrets_writes_secrets_toml(tmp_path) -> None:
    state = _make_state(tmp_path)
    state.add_secrets_value(
        WritableConfigValue("api_token", str, "<your-token>", ("sources", "github"))
    )

    state.commit()

    secrets_path = tmp_path / ".dlt" / "secrets.toml"
    assert secrets_path.is_file()
    body = secrets_path.read_text(encoding="utf-8")
    assert "[sources.github]" in body
    assert "api_token" in body


def test_pending_config_writes_config_toml(tmp_path) -> None:
    state = _make_state(tmp_path)
    state.add_config_value(WritableConfigValue("dlthub_telemetry", bool, True, ("runtime",)))

    state.commit()

    config_path = tmp_path / ".dlt" / "config.toml"
    assert config_path.is_file()
    body = config_path.read_text(encoding="utf-8")
    assert "[runtime]" in body
    assert "dlthub_telemetry" in body


def test_after_files_hook_runs_between_files_and_tomls(tmp_path) -> None:
    events: List[str] = []
    target = tmp_path / "marker.txt"

    class _Recorder(WorkspaceWriteState):
        def _write_staged_files(self) -> None:
            events.append("files")
            super()._write_staged_files()

        def _after_files_hook(self) -> None:
            events.append("hook")
            # subclass writes its own artefact here, like dlt-plus's dlt.yml
            target.write_text("hook-wrote-this\n", encoding="utf-8")

        def _write_secrets_toml(self) -> None:
            events.append("secrets")
            super()._write_secrets_toml()

    state = _Recorder(
        FileStorage(str(tmp_path), makedirs=True), os.path.join(str(tmp_path), ".dlt")
    )
    state.add_new_file(str(tmp_path / "before.txt"), "x\n")
    state.add_secrets_value(
        WritableConfigValue("api_token", str, "<your-token>", ("sources", "github"))
    )

    state.commit()

    assert events == ["files", "hook", "secrets"]
    assert target.read_text(encoding="utf-8") == "hook-wrote-this\n"


def test_committed_copies_only_includes_actual_copies(tmp_path) -> None:
    src1 = tmp_path / "a.txt"
    src1.write_text("a\n", encoding="utf-8")
    src2 = tmp_path / "b.txt"
    src2.write_text("b\n", encoding="utf-8")
    dst1 = str(tmp_path / "out" / "a.txt")
    dst2 = str(tmp_path / "out" / "b.txt")
    # pre-existing dest with accept_existing=True should NOT appear in returned mapping
    os.makedirs(os.path.dirname(dst2), exist_ok=True)
    with open(dst2, "w", encoding="utf-8") as f:
        f.write("kept\n")

    state = _make_state(tmp_path)
    state.add_file_copy(str(src1), dst1)
    state.add_file_copy(str(src2), dst2, accept_existing=True)
    # inline files never appear in the returned mapping
    state.add_new_file(str(tmp_path / "inline.txt"), "inline\n")

    written = state.commit()

    assert written == {dst1: str(src1)}
