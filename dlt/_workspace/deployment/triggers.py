"""Trigger normalization utilities."""

from typing import List, Union

from dlt._workspace.deployment.typing import TTrigger


def normalize_triggers(
    trigger: Union[None, str, TTrigger, List[Union[str, TTrigger]]],
) -> List[TTrigger]:
    """Normalize trigger input to a list of TTrigger values.

    Accepts None, a single string/TTrigger, or a list of mixed.
    For now, wraps raw strings as TTrigger without shorthand expansion.
    """
    if trigger is None:
        return []
    if isinstance(trigger, str):
        trigger = [trigger]
    return [TTrigger(t) for t in trigger]
