"""Generates the dltHub Runtime REST API reference from OpenAPI specs.

Reads the specs bundled in the installed `dlthub-client` package (`dlthub_sdk/_specs`), or
from `--spec-dir`, and renders one Docusaurus page per service.

Every operation has an example request for curl and for Python `requests`.

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
from importlib.metadata import version
from typing import Any, Dict, List, Tuple

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

#: the error body every service shares, documented once on the API overview page
ERROR_LINK = "[error body](overview.md#errors)"

#: nesting depth to which bodies are expanded, deeper objects show as `object`
MAX_DEPTH = 3

#: schemas shown as `object` without their fields: dlt's own job definition, documented with dlt
OPAQUE_SCHEMAS = ("TJobDefinition",)

#: values of string formats in example responses
FORMAT_EXAMPLES = {
    "date-time": "2026-01-01T00:00:00Z",
    "date": "2026-01-01",
    "uuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "uri": "https://example.com",
    "email": "user@example.com",
}

#: query parameters every list operation takes, described once on the API overview page
PAGINATION_PARAMS = ("limit", "offset", "cursor", "order")
PAGINATION_LINK = "[Lists and pagination](overview.md#lists-and-pagination)"

#: marks required parameters and request fields at the start of their description
REQUIRED = "**Required.**"

Schema = Dict[str, Any]


def prose(text: str | None) -> str:
    return escape_mdx(textwrap.dedent(text or "").strip())


def cell(text: str | None) -> str:
    """Single-line table cell content."""
    return " ".join(prose(text).split()).replace("|", "\\|")


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


def type_str(spec: Schema, schema: Schema) -> str:
    if not schema:
        return "any"
    if "$ref" in schema:
        resolved = resolve(spec, schema)
        if resolved.get("type") == "object" or "properties" in resolved:
            return "object"
        return type_str(spec, resolved)
    for key in ("oneOf", "anyOf"):
        if key in schema:
            options = [type_str(spec, s) for s in schema[key]]
            # keep `null` last so `X or null` reads as an optional value
            options.sort(key=lambda o: o == "null")
            return " or ".join(dict.fromkeys(options))
    if "allOf" in schema and len(schema["allOf"]) == 1:
        return type_str(spec, schema["allOf"][0])
    if "enum" in schema:
        # the values go into the description, see `with_values`
        return str(schema.get("type") or "string")
    if "const" in schema:
        return f"`{schema['const']}`"
    schema_type = schema.get("type")
    if isinstance(schema_type, list):
        return " or ".join(type_str(spec, {**schema, "type": t}) for t in schema_type)
    if schema_type == "array":
        return "array of " + type_str(spec, schema.get("items", {}))
    if schema_type == "object":
        additional = schema.get("additionalProperties")
        if isinstance(additional, dict):
            return "map of " + type_str(spec, additional)
        return "object"
    if schema_type is None:
        return "any"
    if "format" in schema:
        return f"{schema_type} ({schema['format']})"
    return str(schema_type)


def enum_values(spec: Schema, schema: Schema) -> List[Any]:
    """Allowed values of an enum, also inside arrays and optional values."""
    schema = resolve(spec, schema)
    if "enum" in schema:
        return list(schema["enum"])
    values: List[Any] = []
    for variant in schema.get("anyOf", []) + schema.get("oneOf", []) + schema.get("allOf", []):
        values += [v for v in enum_values(spec, variant) if v not in values]
    if schema.get("type") == "array":
        values += enum_values(spec, schema.get("items", {}))
    return values


def with_values(spec: Schema, schema: Schema, description: str) -> str:
    values = enum_values(spec, schema)
    if not values:
        return description
    return f"{description} One of: {', '.join(f'`{v}`' for v in values)}.".strip()


def is_opaque(schema: Schema) -> bool:
    refs = re.findall(r'"\$ref": "[^"]*/(\w+)"', json.dumps(schema))
    return any(ref in OPAQUE_SCHEMAS for ref in refs)


def unwrap(spec: Schema, schema: Schema) -> Schema:
    """Resolves refs and single-variant unions, so `X or null` gives the schema of `X`."""
    schema = resolve(spec, schema)
    for key in ("anyOf", "oneOf"):
        variants = [v for v in schema.get(key, []) if v.get("type") != "null"]
        if len(variants) == 1:
            return unwrap(spec, variants[0])
    if len(schema.get("allOf", [])) == 1:
        return unwrap(spec, schema["allOf"][0])
    return schema


def field_rows(
    spec: Schema, schema: Schema, prefix: str = "", depth: int = 0, seen: Tuple[int, ...] = ()
) -> List[List[str]]:
    """One row per field, nested fields named by their path, e.g. `items[].id`."""
    schema = unwrap(spec, schema)
    required = set(schema.get("required") or [])
    rows = []
    for name, prop in (schema.get("properties") or {}).items():
        description = with_values(spec, prop, cell(field_description(prop)))
        if "default" in prop and prop["default"] is not None:
            description = f"{description} Default: `{json.dumps(prop['default'])}`.".strip()
        path = f"{prefix}{name}"
        required_ = "yes" if name in required else "no"
        rows.append([f"`{path}`", cell(type_str(spec, prop)), required_, description])
        if is_opaque(prop):
            continue
        value = unwrap(spec, prop)
        if value.get("type") == "array":
            value, path = unwrap(spec, value.get("items", {})), path + "[]"
        # recursive schemas stop where they repeat
        if value.get("properties") and depth < MAX_DEPTH and id(value) not in seen:
            rows += field_rows(spec, value, path + ".", depth + 1, seen + (id(value),))
    return rows


def fields_table(spec: Schema, schema: Schema, with_required: bool) -> str:
    schema = unwrap(spec, schema)
    prefix = ""
    if schema.get("type") == "array":
        schema, prefix = unwrap(spec, schema.get("items", {})), "[]."
    rows = field_rows(spec, schema, prefix)
    if not rows:
        return f"Type: {type_str(spec, schema)}\n\n"
    for row in rows:
        path, type_, required, description = row
        if with_required and required == "yes":
            description = f"{REQUIRED} {description}"
        row[:] = [path, type_, description]
    header = ["Field", "Type", "Description"]
    lines = ["| " + " | ".join(header) + " |", "|" + " --- |" * len(header)]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join(lines) + "\n\n"


def render_parameters(spec: Schema, parameters: List[Schema]) -> str:
    """One table per location; path parameters are always required, others are marked."""
    tables: Dict[str, List[str]] = {}
    pagination = []
    for param in parameters:
        name = param["name"]
        if param["in"] == "query" and name in PAGINATION_PARAMS:
            pagination.append(f"`{name}`")
            continue
        description = cell(param.get("description"))
        if not description and param["in"] == "path" and name.endswith("_id"):
            description = "ID of the " + name[: -len("_id")].replace("_", " ")
        description = with_values(spec, param.get("schema", {}), description)
        if param.get("required") and param["in"] != "path":
            description = f"{REQUIRED} {description}"
        tables.setdefault(param["in"], []).append(
            f"| `{name}` | {cell(type_str(spec, param.get('schema', {})))} | {description} |"
        )
    md = ""
    for location, rows in tables.items():
        md += f"**{location.capitalize()} parameters**\n\n| Name | Type | Description |\n"
        md += "| --- | --- | --- |\n" + "\n".join(rows) + "\n\n"
    if pagination:
        md += f"**Pagination:** {', '.join(pagination)}, see {PAGINATION_LINK}\n\n"
    return md


def request_body_fields(spec: Schema, request_body: Schema) -> str:
    md = "" if request_body.get("required") else "The body is optional.\n\n"
    for media_type, media in (request_body.get("content") or {}).items():
        if media_type != "application/json":
            md += f"Sent as `{media_type}`.\n\n"
        md += fields_table(spec, media.get("schema", {}), with_required=True)
    return md


def fold(summary: str, group: str, tabs: List[Tuple[str, str, str]]) -> str:
    """A collapsed block of tabs, each `(value, label, markdown)`, without the admonition box."""
    md = f'<div class="plain-details api-details">\n<details>\n<summary>{summary}</summary>\n\n'
    md += f'<Tabs groupId="{group}">\n'
    for value, label, content in tabs:
        md += f'  <TabItem value="{value}" label="{label}">\n\n{content.rstrip()}\n\n  </TabItem>\n'
    return md + "</Tabs>\n\n</details>\n</div>\n\n"


def render_responses(spec: Schema, responses: Dict[str, Schema]) -> str:
    """Success bodies with an example and their fields, then the error statuses in one line."""
    #: statuses of each distinct success body, keyed by its schema
    bodies: Dict[str, List[str]] = {}
    md = ""
    errors = []
    for status, response in responses.items():
        if not status.startswith("2"):
            errors.append(f"`{status}`")
            continue
        content = response.get("content") or {}
        schema = content.get("application/json", {}).get("schema", {})
        if schema:
            bodies.setdefault(json.dumps(schema, sort_keys=True), []).append(status)
        elif content:
            media_types = ", ".join(f"`{m}`" for m in content)
            label = "" if status == "200" else f" (`{status}`)"
            md += f"**Response{label}:** {media_types}\n\n"
        else:
            md += f"**Response:** `{status}` with no body\n\n"
    for key, statuses in bodies.items():
        schema = json.loads(key)
        example = json.dumps(example_value(spec, "body", schema, complete=True), indent=2)
        # arrays of plain values fit on one line
        example = re.sub(
            r"\[\s+([^\[\]{}]*?)\s+\]",
            lambda m: "[" + ", ".join(v.strip() for v in m.group(1).split(",\n")) + "]",
            example,
        )
        summary = "Response"
        if statuses != ["200"]:
            summary += " (" + ", ".join(f"<code>{status}</code>" for status in statuses) + ")"
        tabs = [
            ("example", "Example", f"```json\n{example}\n```"),
            ("fields", "Fields", fields_table(spec, schema, with_required=False)),
        ]
        md += fold(summary, "api-response", tabs)
    if errors:
        md += f"**Errors:** {', '.join(errors)}, each with the {ERROR_LINK}\n\n"
    return md


def render_operation(
    spec: Schema, method: str, path: str, operation: Schema, base_url: str, token: str
) -> str:
    operation_id = operation["operationId"]
    md = f"### {operation_title(operation)} {{#{operation_id}}}\n\n"
    md += f"**{method.upper()}** `{path}`\n\n"
    if operation.get("deprecated"):
        md += ":::warning\nThis endpoint is deprecated.\n:::\n\n"
    description = prose(operation.get("description"))
    if description:
        md += description + "\n\n"
    md += render_parameters(spec, operation.get("parameters") or [])
    md += render_request(spec, method, path, operation, base_url, token)
    md += render_responses(spec, operation.get("responses") or {})
    return md


def resolve(spec: Schema, schema: Schema) -> Schema:
    while "$ref" in schema:
        schema = spec["components"]["schemas"][ref_name(schema["$ref"])]
    return schema


def example_value(
    spec: Schema, name: str, schema: Schema, depth: int = 0, complete: bool = False
) -> Any:
    """Placeholder value for a field: its default or first enum value, else one of its type.

    A `complete` value fills in every field and gives formatted strings a value of their format.
    """
    if complete and is_opaque(schema):
        return {}
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
            return example_value(spec, name, variants[0], depth, complete)
    schema_type = schema.get("type")
    if schema_type == "array":
        item = example_value(spec, name, schema.get("items", {}), depth, complete)
        return [item] if item != {} else []
    if schema_type == "object" or "properties" in schema:
        return example_body(spec, schema, depth + 1, complete)
    if schema_type == "integer" or schema_type == "number":
        return 0
    if schema_type == "boolean":
        return False
    if complete and schema.get("format") in FORMAT_EXAMPLES:
        return FORMAT_EXAMPLES[schema["format"]]
    return f"<{name}>"


def example_body(
    spec: Schema, schema: Schema, depth: int = 0, complete: bool = False
) -> Dict[str, Any]:
    """Required fields of an object schema with placeholder values, all fields if none is."""
    schema = resolve(spec, schema)
    if depth > MAX_DEPTH:
        return {}
    properties = schema.get("properties") or {}
    names = [name for name in schema.get("required") or [] if name in properties]
    if complete:
        names = list(properties)
    return {
        name: example_value(spec, name, properties[name], depth, complete)
        for name in names or properties
    }


def path_variable(match: "re.Match[str]") -> str:
    return "$" + match.group(1).upper()


def request_token(path: str, token: str) -> str:
    # uploads take the token returned by the matching `-issue-upload` operation
    return "$UPLOAD_TOKEN" if path.endswith("/-upload") else token


def curl_example(
    spec: Schema, method: str, path: str, operation: Schema, base_url: str, token: str
) -> str:
    url = base_url + re.sub(r"{(\w+)}", path_variable, path)
    token = request_token(path, token)
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
    return " \\\n  ".join(lines)


def py_literal(value: Any, indent: int = 0) -> str:
    """Python source of a JSON value, split over lines when it is long."""
    inline = repr(value) if not isinstance(value, str) else json.dumps(value)
    if isinstance(value, dict):
        items = [f"{json.dumps(k)}: {py_literal(v, indent + 4)}" for k, v in value.items()]
        inline = "{" + ", ".join(items) + "}"
        if len(inline) + indent > 80:
            pad = " " * (indent + 4)
            return "{\n" + "".join(f"{pad}{item},\n" for item in items) + " " * indent + "}"
    elif isinstance(value, list):
        inline = "[" + ", ".join(py_literal(v, indent) for v in value) + "]"
    return inline


def placeholder(value: str) -> str:
    """`$DATAPLANE_URL` -> `<dataplane_url>`, other values as they are."""
    return f"<{value[1:].lower()}>" if value.startswith("$") else value


def python_example(
    spec: Schema, method: str, path: str, operation: Schema, base_url: str, token: str
) -> str:
    # ids and the data plane URL are placeholders to fill in, the token is read from the env
    url = placeholder(base_url) + re.sub(r"{(\w+)}", r"<\1>", path)
    env = request_token(path, token).lstrip("$")
    args = [json.dumps(url), f'headers={{"Authorization": f"Bearer {{os.environ[\'{env}\']}}"}}']
    lines = []
    content = (operation.get("requestBody") or {}).get("content") or {}
    if "application/json" in content:
        body = example_body(spec, content["application/json"].get("schema", {}))
        args.append(f"json={py_literal(body, 4)}")
    elif "multipart/form-data" in content:
        schema = resolve(spec, content["multipart/form-data"].get("schema", {}))
        files, data = {}, {}
        for name, prop in (schema.get("properties") or {}).items():
            if prop.get("format") == "binary":
                files[name] = f'open("<{name}>", "rb")'
            else:
                data[name] = json.dumps(example_value(spec, name, prop))
        if files:
            args.append("files={" + ", ".join(f'"{k}": {v}' for k, v in files.items()) + "}")
        if data:
            args.append("data={" + ", ".join(f'"{k}": {v}' for k, v in data.items()) + "}")
    media_types = [
        media_type
        for status, response in (operation.get("responses") or {}).items()
        if status.startswith("2")
        for media_type in response.get("content") or {}
    ]
    streams = any(m in ("text/event-stream", "application/x-ndjson") for m in media_types)
    if streams:
        args.append("stream=True")
    call = f"response = requests.{method}(\n" + "".join(f"    {a},\n" for a in args) + ")"
    lines += [call, "response.raise_for_status()"]
    if streams:
        lines.append("for line in response.iter_lines():\n    print(line.decode())")
    elif "application/json" in media_types:
        lines.append("print(response.json())")
    return "import os\n\nimport requests\n\n" + "\n".join(lines)


def render_request(
    spec: Schema, method: str, path: str, operation: Schema, base_url: str, token: str
) -> str:
    tabs = []
    if operation.get("requestBody"):
        tabs.append(("body", "Body", request_body_fields(spec, operation["requestBody"])))
    curl = curl_example(spec, method, path, operation, base_url, token)
    python = python_example(spec, method, path, operation, base_url, token)
    tabs.append(("curl", "curl", f"```sh\n{curl}\n```"))
    tabs.append(("python", "Python", f"```python\n{python}\n```"))
    return fold("Request", "api-request", tabs)


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
