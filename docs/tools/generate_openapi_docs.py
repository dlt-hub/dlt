"""Generates the dltHub Runtime REST API reference from OpenAPI specs.

Reads the specs bundled in the installed `dlthub-client` package (`dlthub_sdk/_specs`), or
from `--spec-dir`, and renders one Docusaurus page per service.

Usage:
    uv run --no-project --with dlthub-client --with pyyaml python tools/generate_openapi_docs.py \
        [--spec-dir DIR] [--output-dir DIR]
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import pathlib
import re
import textwrap
from http import HTTPStatus
from importlib.metadata import version
from typing import Any, Dict, List

import yaml

from mdx_utils import escape_mdx

DOCS_DIR = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = DOCS_DIR / "website" / "docs_processed" / "hub" / "reference" / "api"

#: spec file stem -> (page slug, sidebar label, base URL and token in curl examples); also the
#: page order
SERVICES = {
    "api": ("runtime-api", "Runtime API", "https://api.dlthub.com", "$DLTHUB_API_KEY"),
    "dataplane_api": ("dataplane-api", "Dataplane API", "$DATAPLANE_URL", "$DATAPLANE_TOKEN"),
    "telemetry": ("telemetry-api", "Telemetry API", "$DATAPLANE_URL", "$DATAPLANE_TOKEN"),
    "logs": ("logs-api", "Logs API", "$DATAPLANE_URL", "$DATAPLANE_TOKEN"),
}

HTTP_METHODS = ("get", "post", "put", "patch", "delete")

#: framework default for timezone-aware datetimes, says nothing the type does not
DATETIME_BOILERPLATE = "datetime with the constraint that the value must have timezone info"

Schema = Dict[str, Any]


def prose(text: str | None) -> str:
    return escape_mdx(textwrap.dedent(text or "").strip())


def cell(text: str | None) -> str:
    """Single-line table cell content."""
    return " ".join(prose(text).split()).replace("|", "\\|")


def schema_anchor(name: str) -> str:
    return "schema-" + name.lower()


def operation_title(operation: Schema) -> str:
    """`TriggerJobs` -> `Trigger jobs`; falls back to the operation id for sentence summaries."""
    summary = operation.get("summary") or ""
    if summary and " " not in summary:
        return re.sub(r"(?<!^)(?=[A-Z])", " ", summary).capitalize()
    return str(operation["operationId"]).replace("_", " ").capitalize()


def field_description(schema: Schema) -> str | None:
    description: str | None = schema.get("description")
    return None if description == DATETIME_BOILERPLATE else description


def ref_name(ref: str) -> str:
    return ref.rsplit("/", 1)[-1]


def type_str(schema: Schema) -> str:
    if not schema:
        return "any"
    if "$ref" in schema:
        name = ref_name(schema["$ref"])
        return f"[{name}](#{schema_anchor(name)})"
    for key in ("oneOf", "anyOf"):
        if key in schema:
            options = [type_str(s) for s in schema[key]]
            # keep `null` last so `X or null` reads as an optional value
            options.sort(key=lambda o: o == "null")
            return " or ".join(dict.fromkeys(options))
    if "allOf" in schema and len(schema["allOf"]) == 1:
        return type_str(schema["allOf"][0])
    if "enum" in schema:
        return "one of " + ", ".join(f"`{v}`" for v in schema["enum"])
    if "const" in schema:
        return f"`{schema['const']}`"
    schema_type = schema.get("type")
    if isinstance(schema_type, list):
        return " or ".join(type_str({**schema, "type": t}) for t in schema_type)
    if schema_type == "array":
        return "array of " + type_str(schema.get("items", {}))
    if schema_type == "object":
        additional = schema.get("additionalProperties")
        if isinstance(additional, dict):
            return "map of " + type_str(additional)
        return "object"
    if schema_type is None:
        return "any"
    if "format" in schema:
        return f"{schema_type} ({schema['format']})"
    return str(schema_type)


def property_table(schema: Schema) -> str:
    properties = schema.get("properties") or {}
    if not properties:
        return ""
    required = set(schema.get("required") or [])
    rows = ["| Field | Type | Required | Description |", "| --- | --- | --- | --- |"]
    for name, prop in properties.items():
        description = cell(field_description(prop))
        if "default" in prop and prop["default"] is not None:
            description = f"{description} Default: `{json.dumps(prop['default'])}`.".strip()
        rows.append(
            f"| `{name}` | {cell(type_str(prop))} | {'yes' if name in required else 'no'} |"
            f" {description} |"
        )
    return "\n".join(rows) + "\n"


def render_parameters(parameters: List[Schema]) -> str:
    if not parameters:
        return ""
    rows = ["| Name | In | Type | Required | Description |", "| --- | --- | --- | --- | --- |"]
    for param in parameters:
        name = param["name"]
        description = param.get("description")
        if not description and param["in"] == "path" and name.endswith("_id"):
            description = "ID of the " + name[: -len("_id")].replace("_", " ")
        rows.append(
            f"| `{name}` | {param['in']} | {cell(type_str(param.get('schema', {})))} |"
            f" {'yes' if param.get('required') else 'no'} | {cell(description)} |"
        )
    return "**Parameters**\n\n" + "\n".join(rows) + "\n\n"


def render_content(content: Dict[str, Schema]) -> str:
    """Renders the body type of each media type, e.g. `[JobResponse](#...)`."""
    parts = []
    for media_type, media in content.items():
        body = type_str(media.get("schema", {}))
        parts.append(body if media_type == "application/json" else f"{body} (`{media_type}`)")
    return ", ".join(parts)


def render_request_body(request_body: Schema | None) -> str:
    if not request_body:
        return ""
    content = request_body.get("content") or {}
    md = "**Request body**"
    if not request_body.get("required"):
        md += " (optional)"
    md += f": {render_content(content)}\n\n"
    for media in content.values():
        schema = media.get("schema", {})
        if "$ref" not in schema:
            md += property_table(schema) + "\n"
    return md


def status_phrase(status: str) -> str:
    try:
        return HTTPStatus(int(status)).phrase
    except ValueError:
        return status


def render_responses(responses: Dict[str, Schema]) -> str:
    rows = ["| Status | Description | Body |", "| --- | --- | --- |"]
    # errors sharing a body collapse into one row, they all differ only by status
    errors: Dict[str, List[str]] = {}
    for status, response in responses.items():
        body = cell(render_content(response.get("content") or {})) or "none"
        if status.isdigit() and int(status) >= 400:
            errors.setdefault(body, []).append(f"`{status}`")
            continue
        description = response.get("description") or ""
        # the framework fills in the reason phrase of the status line, which reads oddly
        if description.endswith(" follows"):
            description = status_phrase(status)
        rows.append(f"| `{status}` | {cell(description)} | {body} |")
    for body, statuses in errors.items():
        rows.append(f"| {', '.join(statuses)} | Error | {body} |")
    return "**Responses**\n\n" + "\n".join(rows) + "\n\n"


def render_operation(
    spec: Schema, method: str, path: str, operation: Schema, base_url: str, token: str
) -> str:
    operation_id = operation["operationId"]
    md = f"### {operation_title(operation)} {{#{operation_id}}}\n\n"
    md += f"```text\n{method.upper()} {path}\n```\n\n"
    if operation.get("deprecated"):
        md += ":::warning\nThis endpoint is deprecated.\n:::\n\n"
    description = prose(operation.get("description"))
    if description:
        md += description + "\n\n"
    md += render_parameters(operation.get("parameters") or [])
    md += render_request_body(operation.get("requestBody"))
    md += render_curl_example(spec, method, path, operation, base_url, token)
    md += render_responses(operation.get("responses") or {})
    return md


def resolve(spec: Schema, schema: Schema) -> Schema:
    while "$ref" in schema:
        schema = spec["components"]["schemas"][ref_name(schema["$ref"])]
    return schema


def example_value(spec: Schema, name: str, schema: Schema, depth: int = 0) -> Any:
    """Placeholder value for a field: its default or first enum value, else one of its type."""
    schema = resolve(spec, schema)
    if schema.get("default") is not None:
        return schema["default"]
    if schema.get("enum"):
        return schema["enum"][0]
    if "const" in schema:
        return schema["const"]
    for key in ("anyOf", "oneOf", "allOf"):
        variants = [v for v in schema.get(key, []) if v.get("type") != "null"]
        if variants:
            return example_value(spec, name, variants[0], depth)
    schema_type = schema.get("type")
    if schema_type == "array":
        item = example_value(spec, name, schema.get("items", {}), depth)
        return [item] if item != {} else []
    if schema_type == "object" or "properties" in schema:
        return example_body(spec, schema, depth + 1)
    if schema_type == "integer" or schema_type == "number":
        return 0
    if schema_type == "boolean":
        return False
    return f"<{name}>"


def example_body(spec: Schema, schema: Schema, depth: int = 0) -> Dict[str, Any]:
    """Required fields of an object schema with placeholder values, all fields if none is."""
    schema = resolve(spec, schema)
    if depth > 3:
        return {}
    properties = schema.get("properties") or {}
    names = [name for name in schema.get("required") or [] if name in properties]
    return {
        name: example_value(spec, name, properties[name], depth) for name in names or properties
    }


def path_variable(match: "re.Match[str]") -> str:
    return "$" + match.group(1).upper()


def render_curl_example(
    spec: Schema, method: str, path: str, operation: Schema, base_url: str, token: str
) -> str:
    url = base_url + re.sub(r"{(\w+)}", path_variable, path)
    # uploads take the token returned by the matching `-issue-upload` operation
    if path.endswith("/-upload"):
        token = "$UPLOAD_TOKEN"
    request = f'curl "{url}"' if method == "get" else f'curl -X {method.upper()} "{url}"'
    lines = [request, f'-H "Authorization: Bearer {token}"']
    content = (operation.get("requestBody") or {}).get("content") or {}
    if "application/json" in content:
        body = example_body(spec, content["application/json"].get("schema", {}))
        lines.append('-H "Content-Type: application/json"')
        data = json.dumps(body)
        if len(data) > 60:
            fields = ",\n".join(f"    {json.dumps(k)}: {json.dumps(v)}" for k, v in body.items())
            data = "{\n" + fields + "\n  }"
        lines.append(f"-d '{data}'")
    elif "multipart/form-data" in content:
        schema = resolve(spec, content["multipart/form-data"].get("schema", {}))
        for name, prop in (schema.get("properties") or {}).items():
            if prop.get("format") == "binary":
                lines.append(f'-F "{name}=@<{name}>"')
            else:
                lines.append(f'-F "{name}={example_value(spec, name, prop)}"')
    command = " \\\n  ".join(lines)
    return (
        "<details>\n<summary>Example request</summary>\n\n"
        f"```sh\n{command}\n```\n\n</details>\n\n"
    )


def tag_label(tag: str) -> str:
    return tag.replace("-", " ").capitalize()


def render_spec(spec: Schema, slug: str, label: str, base_url: str, token: str) -> str:
    info = spec.get("info", {})
    md = "---\n"
    md += f"title: {label}\n"
    md += f"description: {label} reference of the dltHub Runtime\n"
    md += f"keywords: [dlthub, runtime, rest api, openapi, {slug}]\n"
    md += "---\n\n"
    md += f"<!-- generated from the {info.get('title')} OpenAPI spec, do not edit -->\n\n"
    md += prose(info.get("description")) + "\n\n"

    by_tag: Dict[str, List[str]] = {}
    for path, item in spec.get("paths", {}).items():
        for method in HTTP_METHODS:
            if method in item:
                operation = item[method]
                tag = (operation.get("tags") or ["endpoints"])[0]
                by_tag.setdefault(tag, []).append(
                    render_operation(spec, method, path, operation, base_url, token)
                )
    for tag in sorted(by_tag):
        md += f"## {tag_label(tag)}\n\n" + "".join(by_tag[tag])

    schemas = spec.get("components", {}).get("schemas", {})
    if schemas:
        md += "## Schemas\n\n"
        for name in sorted(schemas):
            schema = schemas[name]
            md += f"### {name} {{#{schema_anchor(name)}}}\n\n"
            if schema.get("description"):
                md += prose(schema["description"]) + "\n\n"
            table = property_table(schema)
            md += table + "\n" if table else f"Type: {type_str(schema)}\n\n"
    return md.rstrip() + "\n"


def bundled_spec_dir() -> pathlib.Path:
    """Locates the specs shipped in `dlthub-client` without importing the package."""
    spec = importlib.util.find_spec("dlthub_sdk")
    if spec is None or not spec.submodule_search_locations:
        raise SystemExit("dlthub-client is not installed; pass --spec-dir")
    spec_dir = pathlib.Path(spec.submodule_search_locations[0]) / "_specs"
    if not spec_dir.is_dir():
        raise SystemExit(
            f"dlthub-client {version('dlthub-client')} bundles no OpenAPI specs; "
            "0.28.8a2 is the first version that does"
        )
    return spec_dir


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec-dir", type=pathlib.Path, default=None)
    parser.add_argument("--output-dir", type=pathlib.Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    if args.spec_dir is None:
        args.spec_dir = bundled_spec_dir()
        print(f"Reading the OpenAPI specs of dlthub-client {version('dlthub-client')}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    for stem, (slug, label, base_url, token) in SERVICES.items():
        spec_path = args.spec_dir / f"{stem}.openapi.yaml"
        with open(spec_path, encoding="utf-8") as f:
            spec = yaml.safe_load(f)
        (args.output_dir / f"{slug}.md").write_text(
            render_spec(spec, slug, label, base_url, token), encoding="utf-8"
        )
    print(f"Generated {len(SERVICES)} API reference pages in {args.output_dir}")


if __name__ == "__main__":
    main()
