import argparse
from typing import List, Optional, Type

import pytest

from dlt._workspace.cli.compose import (
    ComposedExecutable,
    configure_parser,
    group_commands,
    get_existing_subparsers_action,
)
from dlt.common.configuration.plugins import SupportsCliCommand


def _make_command(
    *,
    name: str,
    parent: Optional[str] = None,
    compose_mode: str = "replace",
    args: Optional[List[str]] = None,
    record: Optional[List[str]] = None,
    label: str = "",
) -> Type[SupportsCliCommand]:
    """Builds a synthetic SupportsCliCommand class with the given composition fields."""
    args = args or []

    class _Cmd(SupportsCliCommand):
        command = name
        help_string = f"{name} test"
        description = None
        docs_url = None

    _Cmd.parent = parent
    _Cmd.compose = compose_mode  # type: ignore[assignment]

    def _configure(self: SupportsCliCommand, parser: argparse.ArgumentParser) -> None:
        for a in args:
            parser.add_argument(a)

    def _execute(self: SupportsCliCommand, args: argparse.Namespace) -> None:
        if record is not None:
            record.append(label or name)

    _Cmd.configure_parser = _configure  # type: ignore[method-assign]
    _Cmd.execute = _execute  # type: ignore[method-assign]
    return _Cmd


def test_group_splits_top_and_subcommands() -> None:
    top, sub = group_commands(
        [
            _make_command(name="x"),
            _make_command(name="run", parent="x"),
            None,
        ]
    )
    assert list(top.keys()) == ["x"]
    assert list(sub.keys()) == [("x", "run")]


def test_classify_groups_same_command_name() -> None:
    a = _make_command(name="x", compose_mode="extend")
    b = _make_command(name="x", compose_mode="extend")
    top, _ = group_commands([a, b])
    assert len(top["x"]) == 2


def test_classify_rejects_additive_on_subcommand() -> None:
    bad = _make_command(name="run", parent="x", compose_mode="additive")
    with pytest.raises(ValueError, match="compose='additive'"):
        group_commands([bad])


def test_replace_mode_runs_only_first_plugin() -> None:
    record: List[str] = []
    a = _make_command(name="x", record=record, label="A")
    b = _make_command(name="x", record=record, label="B")
    node = configure_parser(argparse.ArgumentParser(), [a(), b()])

    node.execute(argparse.Namespace())
    assert record == ["A"]


def test_extend_mode_runs_all_executes_in_order() -> None:
    record: List[str] = []
    a = _make_command(name="x", compose_mode="extend", record=record, label="A")
    b = _make_command(name="x", compose_mode="extend", record=record, label="B")
    node = configure_parser(argparse.ArgumentParser(), [a(), b()])

    node.execute(argparse.Namespace())
    assert record == ["A", "B"]


def test_disagreeing_compose_modes_in_same_group_raise() -> None:
    a = _make_command(name="x", compose_mode="extend")
    b = _make_command(name="x", compose_mode="replace")
    with pytest.raises(ValueError, match="disagree on `compose`"):
        configure_parser(argparse.ArgumentParser(), [a(), b()])


def test_additive_falls_back_to_primary_when_args_func_missing() -> None:
    record: List[str] = []
    base = _make_command(name="x", compose_mode="additive", record=record, label="base")
    node = configure_parser(argparse.ArgumentParser(), [base()])

    node.execute(argparse.Namespace())
    assert record == ["base"]


def test_additive_routes_to_subparser_via_args_func() -> None:
    record: List[str] = []
    base = _make_command(name="x", compose_mode="additive", record=record, label="base")
    node = ComposedExecutable(
        name="x", compose="additive", primary=base(), members=[base()], parser=None
    )

    def addon(args: argparse.Namespace) -> None:
        record.append("addon")

    node.execute(argparse.Namespace(execute=addon))
    assert record == ["addon"]


def test_get_existing_subparsers_action_returns_none_or_action() -> None:
    parser = argparse.ArgumentParser()
    assert get_existing_subparsers_action(parser) is None

    parser.add_subparsers(dest="op")
    assert isinstance(get_existing_subparsers_action(parser), argparse._SubParsersAction)
