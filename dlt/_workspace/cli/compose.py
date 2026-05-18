"""Composition primitives for plugins contributing CLI commands."""
import argparse
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple, Type, cast

from dlt.common.configuration.plugins import SupportsCliCommand, TCliCommandCompose


def get_existing_subparsers_action(
    parser: argparse.ArgumentParser,
) -> Optional[argparse._SubParsersAction]:  # type: ignore[type-arg]
    """Returns the `_SubParsersAction` previously installed on `parser`, or None."""
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action
    return None


def group_commands(
    results: Sequence[Optional[Type[SupportsCliCommand]]],
) -> Tuple[Dict[str, List[SupportsCliCommand]], Dict[Tuple[str, str], List[SupportsCliCommand]],]:
    """Groups commands received from plugins into top level ie. `dlt pipeline` and subcommands that
    may be composed via `additive` mode ie. `dlt pipeline show` and `dlt pipeline info`.
    """
    top: Dict[str, List[SupportsCliCommand]] = {}
    sub: Dict[Tuple[str, str], List[SupportsCliCommand]] = {}
    for cls in results:
        if cls is None:
            continue
        inst = cls()
        if inst.parent is None:
            top.setdefault(inst.command, []).append(inst)
        else:
            # subcommands have parent set
            if inst.compose == "additive":
                raise ValueError(
                    f"command {inst.command!r} (parent={inst.parent!r}) declares"
                    " compose='additive', which is only valid on top-level commands"
                    " (single-level nesting)."
                )
            sub.setdefault((inst.parent, inst.command), []).append(inst)
    return top, sub


@dataclass
class ComposedExecutable:
    """Result of composing one or more `SupportsCliCommand` into a single executable."""

    name: str
    compose: TCliCommandCompose
    primary: SupportsCliCommand
    members: List[SupportsCliCommand] = field(default_factory=list)
    parser: Optional[argparse.ArgumentParser] = None

    def execute(self, args: argparse.Namespace) -> None:
        if self.compose == "extend":
            for m in self.members:
                m.execute(args)
            return
        if self.compose == "additive":
            # set_defaults(execute=...) per subparser routes here. Fall back to
            # primary.execute when no subcommand was selected (e.g. `dlt pipeline`
            # without an operation).
            execute = getattr(args, "execute", None)
            if execute is not None:
                execute(args)
            else:
                self.primary.execute(args)
            return
        # replace: primary handles it
        self.primary.execute(args)

    @property
    def help_string(self) -> str:
        return self.primary.help_string

    @property
    def description(self) -> Optional[str]:
        return cast(Optional[str], getattr(self.primary, "description", None))

    @property
    def docs_url(self) -> Optional[str]:
        return cast(Optional[str], getattr(self.primary, "docs_url", None))

    @property
    def command(self) -> str:
        return self.primary.command


def _validate_uniform_compose(group: List[SupportsCliCommand]) -> TCliCommandCompose:
    modes = {c.compose for c in group}
    if len(modes) > 1:
        raise ValueError(
            f"plugins disagree on `compose` for command {group[0].command!r}: {modes!r}"
        )
    return modes.pop()


def configure_parser(
    parser: argparse.ArgumentParser,
    group: List[SupportsCliCommand],
) -> ComposedExecutable:
    """Configures `parser` and returns the executable that dispatches `group` commands to configured executables.

    Typically called twice: always for group of top level commands (ie. `extend` composes top level commands) and then for
    all additive subparsers.
    """
    if not group:
        raise ValueError("configure_parser called with empty group")
    mode = _validate_uniform_compose(group)
    name = group[0].command

    if mode == "replace":
        group[0].configure_parser(parser)
        return ComposedExecutable(
            name=name, compose=mode, primary=group[0], members=[group[0]], parser=parser
        )

    if mode == "extend":
        # contract: only the first plugin's configure_parser runs; others contribute only execute
        group[0].configure_parser(parser)
        return ComposedExecutable(
            name=name, compose=mode, primary=group[0], members=list(group), parser=parser
        )

    if mode == "additive":
        # primary's configure_parser declares the subparsers action; sub-subcommands
        # are attached later by the host (which re-enters this function in replace/extend mode).
        group[0].configure_parser(parser)
        return ComposedExecutable(
            name=name, compose=mode, primary=group[0], members=list(group), parser=parser
        )

    raise ValueError(f"unknown compose mode {mode!r} for command {name!r}")
