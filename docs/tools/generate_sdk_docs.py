"""Generates the dltHub Python SDK reference from the installed `dlthub_sdk` package.

The package is read statically with griffe, never imported. Every name in `dlthub_sdk.__all__`
is rendered on one page, grouped by the module that defines it.

Usage:
    uv run --no-project --with dlthub-client --with griffe python tools/generate_sdk_docs.py OUTPUT
"""

from __future__ import annotations

import argparse
import html
import pathlib
import re
import textwrap
from importlib.metadata import version
from typing import Dict, List, Optional, Union

import griffe

from mdx_utils import escape_mdx

PACKAGE = "dlthub_sdk"
DISTRIBUTION = "dlthub-client"

#: module (relative to the package) -> section label; also the section order
SECTIONS = {
    "": "Connecting",
    "domain.runtime": "Runtime",
    "domain.organizations": "Organizations",
    "domain.workspaces": "Workspaces",
    "domain.caller": "Caller",
    "domain.dataplanes": "Data planes",
    "domain.deployments": "Deployments",
    "domain.configurations": "Configurations",
    "domain.files": "Files",
    "domain.jobs": "Jobs",
    "domain.job_runs": "Job runs",
    "domain.logs": "Logs",
    "domain.variables": "Variables",
    "domain.telemetry": "Telemetry",
    "credentials": "Credentials",
    "errors": "Errors",
}

INTRO = """\
:::caution Beta release
The Python SDK is in beta. Its API is stable and will only be extended, but its packaging may
change: it can move to another PyPI package and another top-level package name than
`{package}`.
:::

The Python SDK is the `{package}` package. It ships with `{distribution}`, which `dlt[hub]`
installs:

```sh
pip install "dlt[hub]"
```

## Getting started

Connect with an API key. The client closes when the `with` block ends:

```python
import dlthub_sdk

with dlthub_sdk.connect(token="dlt_u_...") as runtime:
    for organization in runtime.organizations.list():
        for workspace in organization.workspaces.list():
            print(organization.name, workspace.id, workspace.name)
```

Every object leads to the objects that belong to it, so you go from the client to a workspace,
from the workspace to a job, and from the job to its runs and their logs:

```python
with dlthub_sdk.connect(token="dlt_u_...") as runtime:
    workspace = runtime.workspaces.get(id="<workspace id>")
    job = workspace.jobs.get_by_ref("jobs.load")

    run = job.start_run()
    for line in run.stream_logs():
        print(line.content)
    run = run.wait()
    print(run.status)

    for past_run in job.runs.list(limit=5):
        print(past_run.number, past_run.status, past_run.duration_seconds)
```

A workspace also reaches its variables, deployments, configurations and pipeline telemetry:

```python
with dlthub_sdk.connect(token="dlt_u_...") as runtime:
    workspace = runtime.workspaces.get(id="<workspace id>")

    workspace.variables.set(
        "SOURCES__API__TOKEN", "...", profile=dlthub_sdk.WORKSPACE_PROFILE, secret=True
    )
    print(workspace.deployments.latest().version)
    for pipeline in workspace.telemetry.pipelines():
        print(pipeline.pipeline_name, pipeline.latest_status, pipeline.total_rows_loaded)
```

[`connect_async()`](#connect_async) gives the same objects for `asyncio`. Methods that call the
platform return awaitables and lists are async iterables:

```python
async with dlthub_sdk.connect_async(token="dlt_u_...") as runtime:
    workspace = await runtime.workspaces.get(id="<workspace id>")
    job = await workspace.jobs.get_by_ref("jobs.load")
    run = await job.start_run()
    async for line in run.stream_logs():
        print(line.content)
```

Types on this page are those of the blocking client. Every entity also has `to_dict()`, which
returns its fields as a plain dict. API keys, base URLs and the data plane are explained in the
[REST API reference](api/index.md), which the SDK is built on."""

Object = Union[griffe.Object, griffe.Alias]


def target(obj: Object) -> griffe.Object:
    return obj.final_target if isinstance(obj, griffe.Alias) else obj


def anchor(name: str) -> str:
    return name.lower().replace(".", "-")


def strip_bracketed(text: str, prefix: str) -> str:
    """Removes every `prefix[...]` including its balanced brackets."""
    while (start := text.find(prefix + "[")) != -1:
        depth, i = 0, start + len(prefix)
        while i < len(text):
            depth += {"[": 1, "]": -1}.get(text[i], 0)
            i += 1
            if depth == 0:
                break
        text = text[:start] + text[i:]
    return text


def clean_type(annotation: object) -> str:
    """Shows the type a blocking client sees, without mode generics."""
    if annotation is None:
        return ""
    text = strip_bracketed(str(annotation), " | Awaitable")
    text = re.sub(r"^Awaitable\[(.*)\]$", r"\1", text)
    previous = None
    while previous != text:
        previous = text
        text = re.sub(r"\b([A-Z]\w*)\[(?:M|Any)\]", r"\1", text)
    return text


class Renderer:
    def __init__(self, exports: Dict[str, griffe.Object]) -> None:
        self.exports = exports
        self.anchors = {anchor(name) for name in exports}
        for name, obj in exports.items():
            if isinstance(obj, griffe.Class):
                self.anchors.update(
                    anchor(f"{name}.{member}")
                    for member, value in obj.members.items()
                    if not member.startswith("_") and isinstance(target(value), griffe.Function)
                )

    def link(self, match: re.Match[str]) -> str:
        role, ref = match.group(1), match.group(2).lstrip("~")
        label = ref + ("()" if role in ("meth", "func") else "")
        # fields have no anchor of their own, they link to their class
        for candidate in (ref, ref.split(".")[0]):
            if anchor(candidate) in self.anchors:
                return f"[`{label}`](#{anchor(candidate)})"
        return f"`{label}`"

    def prose(self, text: Optional[str]) -> str:
        text = textwrap.dedent(text or "").strip()
        text = re.sub(r":(\w+):`([^`]+)`", self.link, text)
        text = re.sub(r"(?<!`)``(?!`)", "`", text)
        return escape_mdx(text)

    def cell(self, text: Optional[str]) -> str:
        return " ".join(self.prose(text).split()).replace("|", "\\|")

    def signature(self, func: griffe.Function, name: str) -> str:
        params: List[str] = []
        kinds = griffe.ParameterKind
        seen_keyword_only = False
        for param in func.parameters:
            if param.name in ("self", "cls"):
                continue
            if param.kind is kinds.keyword_only and not seen_keyword_only:
                params.append("*")
                seen_keyword_only = True
            prefix = {kinds.var_positional: "*", kinds.var_keyword: "**"}.get(param.kind, "")
            text = prefix + param.name
            if param.annotation is not None:
                text += f": {clean_type(param.annotation)}"
            if param.default is not None:
                text += f" = {self.default(func, str(param.default))}"
            params.append(text)
        returns = clean_type(func.returns)
        tail = f" -> {returns}" if returns else ""
        one_line = f"{name}({', '.join(params)}){tail}"
        if len(one_line) <= 90:
            return one_line
        return f"{name}(\n" + "".join(f"    {p},\n" for p in params) + f"){tail}"

    def default(self, func: griffe.Function, default: str) -> str:
        """Shows the value behind a private module constant, which users cannot import."""
        if default.startswith("_") and default in func.module.members:
            value = getattr(target(func.module.members[default]), "value", None)
            if value is not None:
                return str(value)
        return default

    def exception(self, name: object) -> str:
        text = str(name)
        return f"[`{text}`](#{anchor(text)})" if text in self.exports else f"`{text}`"

    def table(self, rows: List[List[str]], header: List[str]) -> str:
        lines = ["| " + " | ".join(header) + " |", "|" + " --- |" * len(header)]
        lines += ["| " + " | ".join(row) + " |" for row in rows]
        return "\n".join(lines) + "\n\n"

    def docstring(self, obj: griffe.Object, title_args: str = "Arguments") -> str:
        """Renders the docstring sections of `obj`, typing parameters from its signature."""
        if not obj.docstring:
            return ""
        annotations: Dict[str, str] = {}
        if isinstance(obj, griffe.Function):
            annotations = {p.name: clean_type(p.annotation) for p in obj.parameters}
        elif isinstance(obj, griffe.Class):
            init = target(obj.members["__init__"]) if "__init__" in obj.members else None
            if isinstance(init, griffe.Function):
                for p in init.parameters:
                    if p.annotation is not None:
                        annotations[p.name] = clean_type(p.annotation)
            for name, member in obj.members.items():
                member = target(member)
                annotation = getattr(member, "annotation", None)
                if annotation is not None:
                    annotations[name] = clean_type(annotation)
        values = self.enum_values(obj) if isinstance(obj, griffe.Class) else {}
        kind = griffe.DocstringSectionKind
        md = ""
        sections = obj.docstring.parse("google")
        for section in sections:
            if section.kind is kind.text:
                md += self.prose(section.value) + "\n\n"
            elif section.kind in (kind.parameters, kind.other_parameters):
                rows = [
                    [
                        f"`{p.name}`",
                        self.type_cell(annotations.get(p.name)),
                        self.cell(p.description),
                    ]
                    for p in section.value
                ]
                md += f"**{title_args}**\n\n" + self.table(rows, ["Name", "Type", "Description"])
            elif section.kind is kind.attributes and values:
                rows = [
                    [f"`{a.name}`", f"`{values.get(a.name, '')}`", self.cell(a.description)]
                    for a in section.value
                ]
                md += "**Members**\n\n" + self.table(rows, ["Member", "Value", "Description"])
            elif section.kind is kind.attributes:
                rows = [
                    [
                        f"`{a.name}`",
                        self.type_cell(annotations.get(a.name)),
                        self.cell(a.description),
                    ]
                    for a in section.value
                ]
                md += "**Fields**\n\n" + self.table(rows, ["Name", "Type", "Description"])
            elif section.kind is kind.returns:
                parts = [self.cell(r.description) for r in section.value if r.description]
                if isinstance(obj, griffe.Function) and clean_type(obj.returns):
                    parts.insert(0, self.type_cell(clean_type(obj.returns)) + " —")
                md += "**Returns:** " + " ".join(parts) + "\n\n"
            elif section.kind is kind.yields:
                parts = [self.cell(r.description) for r in section.value if r.description]
                md += "**Yields:** " + " ".join(parts) + "\n\n"
            elif section.kind is kind.raises:
                rows = [
                    [self.exception(r.annotation), self.cell(r.description)] for r in section.value
                ]
                md += "**Raises**\n\n" + self.table(rows, ["Exception", "When"])
            elif section.kind is kind.examples:
                for sub_kind, value in section.value:
                    if sub_kind is kind.examples:
                        md += f"```python\n{value}\n```\n\n"
                    else:
                        md += self.prose(value) + "\n\n"
            elif section.kind is kind.admonition:
                admonition = section.value
                md += f":::note[{admonition.annotation}]\n{self.prose(admonition.description)}\n:::\n\n"
        if isinstance(obj, griffe.Class) and not any(s.kind is kind.attributes for s in sections):
            md += self.inline_attributes(obj, annotations, values)
        return md

    def inline_attributes(
        self, cls: griffe.Class, annotations: Dict[str, str], values: Dict[str, str]
    ) -> str:
        """Renders fields or enum members documented by a docstring under each attribute."""
        documented = []
        for name, member in cls.members.items():
            member = target(member)
            if (
                not name.startswith("_")
                and isinstance(member, griffe.Attribute)
                and "property" not in member.labels
                and member.docstring
            ):
                documented.append((name, member.docstring.value))
        if not documented:
            return ""
        if values:
            rows = [[f"`{n}`", f"`{values.get(n, '')}`", self.cell(d)] for n, d in documented]
            return "**Members**\n\n" + self.table(rows, ["Member", "Value", "Description"])
        rows = [[f"`{n}`", self.type_cell(annotations.get(n)), self.cell(d)] for n, d in documented]
        return "**Fields**\n\n" + self.table(rows, ["Name", "Type", "Description"])

    def type_cell(self, annotation: Optional[str]) -> str:
        """Renders a type as one code element whose exported names link to their anchors."""
        if not annotation:
            return ""
        parts = []
        for token in re.split(r"([A-Za-z_]\w*)", annotation):
            if anchor(token) in self.anchors:
                parts.append(f'<a href="#{anchor(token)}">{token}</a>')
            else:
                # the pipe would end the table cell
                parts.append(html.escape(token).replace("|", "&#124;"))
        return "<code>" + "".join(parts) + "</code>"

    def function(self, func: griffe.Function, name: str, heading: str) -> str:
        md = f"{heading} `{name}()` {{#{anchor(name)}}}\n\n"
        md += f"```python\n{self.signature(func, name.split('.')[-1])}\n```\n\n"
        return md + self.docstring(func)

    def enum_values(self, cls: griffe.Class) -> Dict[str, str]:
        if not any(str(b) in ("StrEnum", "Enum", "IntEnum") for b in cls.bases):
            return {}
        values: Dict[str, str] = {}
        for name, member in cls.members.items():
            member = target(member)
            if not name.startswith("_") and isinstance(member, griffe.Attribute):
                values[name] = str(member.value)
        return values

    def klass(self, cls: griffe.Class, name: str) -> str:
        md = f"### `{name}` {{#{anchor(name)}}}\n\n"
        bases = [str(b) for b in cls.bases if str(b).split("[")[0] in self.exports]
        if bases:
            md += "Subclass of " + ", ".join(f"[`{b}`](#{anchor(b)})" for b in bases) + ".\n\n"
        md += self.docstring(cls, title_args="Fields")
        properties = []
        for member_name, member in cls.members.items():
            member = target(member)
            if member_name.startswith("_"):
                continue
            if isinstance(member, griffe.Attribute) and "property" in member.labels:
                summary = member.docstring.value.splitlines()[0] if member.docstring else ""
                properties.append(
                    [
                        f"`{member_name}`",
                        self.type_cell(clean_type(member.annotation)),
                        self.cell(summary),
                    ]
                )
        if properties:
            md += "**Properties**\n\n" + self.table(properties, ["Name", "Type", "Description"])
        for member_name, member in cls.members.items():
            member = target(member)
            if member_name.startswith("_") or not isinstance(member, griffe.Function):
                continue
            md += self.function(member, f"{name}.{member_name}", "####")
        return md

    def attribute(self, attr: griffe.Attribute, name: str) -> str:
        md = f"### `{name}` {{#{anchor(name)}}}\n\n"
        if attr.value is not None:
            md += f"```python\n{name} = {attr.value}\n```\n\n"
        return md + (self.prose(attr.docstring.value) + "\n\n" if attr.docstring else "")

    def render(self, obj: griffe.Object, name: str) -> str:
        if isinstance(obj, griffe.Function):
            return self.function(obj, name, "###")
        if isinstance(obj, griffe.Class):
            return self.klass(obj, name)
        if isinstance(obj, griffe.Attribute):
            return self.attribute(obj, name)
        return ""


def section_label(module_path: str) -> str:
    relative = module_path[len(PACKAGE) + 1 :]
    if relative in SECTIONS:
        return SECTIONS[relative]
    if relative.startswith("_"):
        return "Helpers"
    return relative.rsplit(".", 1)[-1].replace("_", " ").capitalize()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=pathlib.Path)
    args = parser.parse_args()

    package = griffe.load(
        PACKAGE, docstring_parser="google", resolve_aliases=True, resolve_external=False
    )
    exports = {str(name): target(package[str(name)]) for name in package.exports or []}
    renderer = Renderer(exports)

    sections: Dict[str, List[str]] = {}
    for name, obj in exports.items():
        sections.setdefault(section_label(obj.module.path), []).append(name)
    order = list(SECTIONS.values()) + ["Helpers"]
    ordered = sorted(sections, key=lambda s: (order.index(s) if s in order else len(order), s))

    client_version = version(DISTRIBUTION)
    md = "---\n"
    md += "title: Python SDK reference\n"
    md += "description: Reference of the dltHub Runtime Python SDK (dlthub_sdk)\n"
    md += "keywords: [dlthub, runtime, python sdk, dlthub_sdk, api]\n"
    md += "---\n"
    md += "# Python SDK reference\n\n"
    md += f"<!-- generated from {DISTRIBUTION} {client_version}, do not edit -->\n\n"
    md += INTRO.format(package=PACKAGE, distribution=DISTRIBUTION) + "\n\n"
    md += f"This page is generated from `{DISTRIBUTION}` {client_version}.\n\n"
    for label in ordered:
        md += f"## {label}\n\n"
        for name in sections[label]:
            md += renderer.render(exports[name], name)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(md.rstrip() + "\n", encoding="utf-8")
    print(f"Generated the SDK reference for {DISTRIBUTION} {client_version} in {args.output}")


if __name__ == "__main__":
    main()
