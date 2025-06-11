# NOTE: experimental tool to update the config docs
from __future__ import annotations

from typing import Any, get_origin, get_args, Final, Annotated, Union, Literal

import dlt
import inspect, ast
import dataclasses
from typing import get_type_hints

from dlt.common.configuration.specs.base_configuration import KNOWN_CONFIG_SPEC_CLASSES, BaseConfiguration
from dlt.common.destination.reference import Destination


OUTPUT_FILE = "docs/website/docs/reference/configuration.md"

OUTPUT_HEADER = """---
title: Configuration Reference
description: Reference of all configuration options available in dlt
keywords: [configuration, reference]
---

# Configuration Reference

This page contains a reference of most configuration options and objects available in DLT.

"""

EXCLUDED_CLASSES = [
    "ExtractorConfiguration",
    "PipeIteratorConfiguration",
    "BufferedDataWriterConfiguration"
    "PipelineConfiguration",
]

# List of logical configuration groups, grouped by common base classes
CONFIG_GROUPS = [
    {
        "name": "Destination Configurations",
        "base_classes": [
            "DestinationClientConfiguration",
        ]
    },
    {
        "name": "Credential Configurations",
        "base_classes": [
            "CredentialsConfiguration",
        ]
    }
]

def clean_type(tp: Any) -> str:
    
    # "primitive" types
    if tp in {int, float, str, bool, complex, bytes, type}:
        return tp.__name__
    
    # unwrap final, annotated, union, literal
    origin = get_origin(tp)
    args = get_args(tp)
    
    if origin in {Final, Annotated}:
        return clean_type(args[0])
    
    if origin in {Union, Literal}:
        non_none_args = [arg for arg in args if arg is not type(None)]
        if len(non_none_args) == 1:
            return clean_type(non_none_args[0])  # Strip Optional
        return " | ".join(clean_type(arg) for arg in non_none_args)
    
    # convert classes to links
    if isinstance(tp, type) and issubclass(tp, BaseConfiguration):
        return f"[{tp.__name__}](#{tp.__name__.lower()})"

    # fallback to string
    return str(tp)

def extract_config_properties(cls):
    # Get source code of the class
    source = inspect.getsource(cls)
    tree = ast.parse(source)

    # Prepare field type hints
    type_hints = get_type_hints(cls)
    result = {}

    # Track docstrings found after each field definition
    doc_map = {}

    lines = source.splitlines()

    for node in ast.walk(tree):
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            field_name = node.target.id
            doc = None

            # Try to find docstring immediately after the field
            next_line_index = node.end_lineno
            if next_line_index and next_line_index < len(lines):
                next_line = lines[next_line_index].strip()
                if next_line.startswith('"""') or next_line.startswith("'''"):
                    doc = next_line.strip('"""').strip("'''").strip()

            doc_map[field_name] = doc

    # Combine with dataclass field info
    for field in dataclasses.fields(cls):
        if field.name.startswith("_"):
            continue
        doc = doc_map.get(field.name) or ""
        doc = doc.replace("<", "").replace(">", "")
        
        type = type_hints.get(field.name, 'Unknown')
        type = clean_type(type)
        type = str(type).replace("<", "").replace(">", "")
        result[field.name] = {
            "name": field.name,
            "type": type,
            "doc": doc
        }

    return result


def extract_config_info(cls: type) -> dict:
    """Extracts information about a config spec class"""
    try:
        return {
            "name": cls.__name__,
            "module": cls.__module__,
            "description": cls.__doc__ if not cls.__doc__.startswith(cls.__name__) else None,
            "bases": [b.__name__ for b in cls.__mro__],
            "properties": extract_config_properties(cls)
        }
    except OSError as e:
        print(f"Error processing {cls.__name__}: {e}")

if __name__ == "__main__":
    print("Collecting config docs...")
    
    all_config_specs = {}
    
    # get sorted specs
    found_config_specs = list(KNOWN_CONFIG_SPEC_CLASSES)
    found_config_specs.sort(key=lambda x: x.__name__)
    
    # process all specs
    for cls in found_config_specs:
        print("Processing", cls.__name__)
        try:
            if cls.__name__ not in EXCLUDED_CLASSES and (spec := extract_config_info(cls)):
                all_config_specs[cls.__name__] = spec
        except (IndentationError, NameError) as e:
            print(f"Error processing {cls.__name__}: {e}")
            
    # for each spec, find doc strings in superclasses if missing
    for name, spec in all_config_specs.items():
        for property in spec["properties"].values():
            if not property["doc"]:
                for base in spec["bases"]:
                    if base in all_config_specs and property["name"] in all_config_specs[base]["properties"]:
                        doc = all_config_specs[base]["properties"][property["name"]]["doc"]
                        if doc:
                            spec["properties"][property["name"]]["doc"] = doc
                            break

    
    # write g
    lines = []
    for group in CONFIG_GROUPS:
        lines.append(f"## {group['name']}")
        for name, spec in all_config_specs.items():
            for base in spec["bases"]:
                if base in group["base_classes"]:
                    lines.append(f"### {name}")
                    lines.append(f"{spec['description']}")
                    lines.append("")
                    for property in spec["properties"].values():
                        lines.append(f"* **`{property['name']}`** - _{property['type']}_ <br /> {property['doc']}")
                    lines.append("")
                    break 
                
                   
    # write to output
    with open(OUTPUT_FILE, "w") as f:
        f.write(OUTPUT_HEADER)
        f.write("\n".join(lines))

        # f.write(extract_config_info(cls))
