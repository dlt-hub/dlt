from dlt.common.destination import DestinationCapabilitiesContext


def capabilities() -> DestinationCapabilitiesContext:
    return DestinationCapabilitiesContext.generic_capabilities("jsonl")
