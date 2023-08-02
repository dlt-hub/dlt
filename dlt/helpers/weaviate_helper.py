from typing import List, Dict, Any, Iterable
from dlt.extract.source import DltResource


def weaviate_adapter(
    resource: DltResource,
    vectorize: List[str] = None,
    tokenization: Dict[str, str] = None,
) -> DltResource:
    if vectorize or tokenization:
        column_hints = {}
        if vectorize:
            for prop in vectorize:
                column_hints[prop] = {
                    "name": prop,
                    "x-vectorize": True,
                }
        if tokenization:
            for prop, method in tokenization.items():
                if prop in column_hints:
                    column_hints[prop]["x-tokenization"] = method
                else:
                    column_hints[prop] = {
                        "name": prop,
                        "x-tokenization": method,
                    }

        resource.apply_hints(columns=column_hints)
    else:
        raise ValueError(
            "Either 'vectorize' or 'tokenization' must be specified."
        )

    return resource
