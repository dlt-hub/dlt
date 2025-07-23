from typing import Any, Sequence

from dlt.common.reflection.ref import ImportTrace


class ReferenceImportError:
    """A mixing for your exception to store and pretty print ref import traces."""

    def __init__(self, *args: Any, **kwargs: Any):
        self.traces: Sequence[ImportTrace] = kwargs.get("traces")

    def __str__(self) -> str:
        if not self.traces:
            return "No import traces were corrected"
        msg = "Modules and attributes were tried in the following order and failed to import:\n"
        for trace in self.traces:
            msg += f"\tmod:`{trace.module}` attr: `{trace.attr_name}` failed due to: {trace.reason}"
            if trace.exc:
                msg += f" and causing exception: {trace.exc}"
            msg += "\n"

        return msg
