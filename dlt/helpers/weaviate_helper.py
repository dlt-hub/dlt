from typing import List, Dict, Union, Any

from dlt.extract.decorators import resource as make_resource
from dlt.extract.source import DltResource


def weaviate_adapter(
    data: Any,
    vectorize: Union[List[str], str] = None,
    tokenization: Dict[str, str] = None,
) -> DltResource:

    resource: DltResource
    if not isinstance(data, DltResource):
        resource_name: str = None
        if not hasattr(data, "__name__"):
            resource_name = "content"
        resource = make_resource(data, name=resource_name)
    else:
        resource = data

    if vectorize or tokenization:
        column_hints = {}
        if vectorize:
            if isinstance(vectorize, str):
                vectorize = [vectorize]
            if not isinstance(vectorize, list):
                raise ValueError(
                    "vectorize must be a list of column names or a single "
                    "column name as a string."
                )

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
        raise ValueError("Either 'vectorize' or 'tokenization' must be specified.")

    return resource
