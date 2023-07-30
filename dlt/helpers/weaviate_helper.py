from typing import List, Dict, Any
from dlt.extract.source import DltResource


def _weaviate_properties_to_hints(properties: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Converts Weaviate properties to DLT column hints."""
    hints = {}
    for prop in properties:
        config = prop.copy()
        name = config.pop("name")
        hints[name] = {"name": name, "config": config}
    return hints


def weaviate_adapter(
    resource: DltResource,
    vectorize: List[str] = None,
    tokenization: Dict[str, str] = None,
    class_schema: Dict[str, Any] = None,
) -> DltResource:
    if class_schema:
        column_hints = _weaviate_properties_to_hints(class_schema.get("properties", []))
        resource.apply_hints(columns=column_hints)
    elif vectorize or tokenization:
        properties = {}
        if vectorize:
            for prop in vectorize:
                properties[prop] = {
                    "name": prop,
                    "moduleConfig": {
                        "__VECTORIZER__": {
                            "skip": False,
                        }
                    },
                }
        if tokenization:
            for prop, method in tokenization.items():
                if prop in properties:
                    properties[prop]["tokenization"] = method
                else:
                    properties[prop] = {
                        "name": prop,
                        "tokenization": method,
                    }

        column_hints = _weaviate_properties_to_hints(properties.values())
        resource.apply_hints(columns=column_hints)
    else:
        raise ValueError(
            "Either 'class_schema' or 'vectorize' and/or 'tokenization' must be specified."
        )

    return resource
