"""Generates the dltHub Python SDK reference from the installed `dlthub_sdk` package.

The package is read statically with griffe, never imported. Every name in `dlthub_sdk.__all__`
is rendered in a section for the module that defines it, and the sections are grouped into pages.
The pages, led by a getting started page, and a `sidebar.json` listing them are written to
OUTPUT_DIR.

Usage:
    uv run --no-project --with dlthub-client --with griffe \
        python tools/generate_sdk_docs.py OUTPUT_DIR
"""

from __future__ import annotations

import argparse
import html
import json
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

#: page slug -> (title, section labels); also the page order. A section of a new module gets a
#: page of its own
PAGES = {
    "getting-started": ("Getting started", []),
    "client": ("Client", ["Connecting", "Runtime", "Credentials", "Conventions"]),
    "organizations-and-workspaces": (
        "Organizations and workspaces",
        ["Organizations", "Workspaces", "Caller", "Data planes"],
    ),
    "deployments": ("Deployments and configurations", ["Deployments", "Configurations", "Files"]),
    "jobs": ("Jobs", ["Jobs"]),
    "job-runs": ("Job runs and logs", ["Job runs", "Logs"]),
    "variables": ("Variables", ["Variables"]),
    "telemetry": ("Telemetry", ["Telemetry"]),
    "errors": ("Errors", ["Errors"]),
}

#: section label -> text above its objects
SECTION_INTROS = {
    "Conventions": "Update methods leave a field as it is when you pass `KEEP`, the default of"
    " every optional update argument, clear it when you pass `None` and set it when you pass a"
    " value.",
}

#: base class -> rank of its subclasses within a section: how you reach an object, the object,
#: what it returns and takes, then enums
ROLES = {"Namespace": 0, "Collection": 1, "Entity": 2, "TypedDict": 4, "Protocol": 4, "StrEnum": 5}

#: doc id of the folder the pages are served from
DOC_ID = "hub/reference/python-sdk"
TITLE = "Python SDK reference"

INTRO = """\
:::info
The Python SDK and the [REST API](../api/index.md) are in public preview. Their interfaces will
remain stable from this point onward and will only be extended. The packaging of the SDK may
still change: it can move to another PyPI package and another top-level package name than
`{package}`.
:::

The Python SDK is the `{package}` package. It ships with `{distribution}`, which `dlt[hub]`
installs:

```sh
pip install "dlt[hub]"
```

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

[`connect_async()`](client.md#connect_async) gives the same objects for `asyncio`. Methods that call the
platform return awaitables and lists are async iterables:

```python
async with dlthub_sdk.connect_async(token="dlt_u_...") as runtime:
    workspace = await runtime.workspaces.get(id="<workspace id>")
    job = await workspace.jobs.get_by_ref("jobs.load")
    run = await job.start_run()
    async for line in run.stream_logs():
        print(line.content)
```

## Collections and attributes

Each object in this reference lists its collections and its attributes:

- **Collections** lead to the objects that belong to it, such as `workspace.jobs` or `job.runs`.
  `list()` iterates over their objects and `get()` fetches one by its ID; most can also
  `count()` them. A few, such as `workspace.variables` and `workspace.telemetry`, group operations
  instead of holding objects.
- **Attributes** are the data the object carries. `to_dict()` returns them as a plain dict.

Types in this reference are those of the blocking client. API keys, base URLs and the data plane
are explained in the [REST API reference](../api/index.md), which the SDK is built on."""

#: shown at the top of every page but the getting started page
START_HERE = "New to the SDK? [Getting started](getting-started.md) shows how to connect and use it."

Object = Union[griffe.Object, griffe.Alias]


def target(obj: Object) -> griffe.Object:
    return obj.final_target if isinstance(obj, griffe.Alias) else obj


def anchor(name: str) -> str:
    # constants keep their case so `KEEP` and `Keep` get distinct anchors
    return (name if name.isupper() else name.lower()).replace(".", "-")


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
    while (start := text.find("ReadOnly[")) != -1:
        depth, end = 0, start + len("ReadOnly")
        for end in range(end, len(text)):
            depth += {"[": 1, "]": -1}.get(text[end], 0)
            if depth == 0:
                break
        text = text[:start] + text[start + len("ReadOnly[") : end] + text[end + 1 :]
    text = re.sub(r"^Awaitable\[(.*)\]$", r"\1", text)
    previous = None
    while previous != text:
        previous = text
        text = re.sub(r"\b([A-Z]\w*)\[(?:M|Any)\]", r"\1", text)
    return text


class Renderer:
    def __init__(self, exports: Dict[str, griffe.Object], pages: Dict[str, str]) -> None:
        self.exports = exports
        #: anchor -> slug of the page it is on
        self.anchors = {anchor(name): pages[name] for name in exports}
        for name, obj in exports.items():
            if isinstance(obj, griffe.Class):
                self.anchors.update(
                    (anchor(f"{name}.{member}"), pages[name])
                    for member, value in obj.members.items()
                    if not member.startswith("_") and isinstance(target(value), griffe.Function)
                )
        self.page = ""
        #: heading level of the objects on the page, one less when the page has no section headings
        self.level = 3

    def href(self, anchor_: str) -> str:
        """Markdown link target of an anchor, on this page or another one."""
        page = self.anchors[anchor_]
        return f"#{anchor_}" if page == self.page else f"{page}.md#{anchor_}"

    def url(self, anchor_: str) -> str:
        """URL of an anchor relative to this page, for HTML links Docusaurus does not resolve."""
        page = self.anchors[anchor_]
        return f"#{anchor_}" if page == self.page else f"{page}#{anchor_}"

    def link(self, match: re.Match[str]) -> str:
        role, ref = match.group(1), match.group(2).lstrip("~")
        label = ref + ("()" if role in ("meth", "func") else "")
        # fields have no anchor of their own, they link to their class
        for candidate in (ref, ref.split(".")[0]):
            if anchor(candidate) in self.anchors:
                return f"[`{label}`]({self.href(anchor(candidate))})"
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
        return f"[`{text}`]({self.href(anchor(text))})" if text in self.exports else f"`{text}`"

    def table(self, rows: List[List[str]], header: List[str]) -> str:
        lines = ["| " + " | ".join(header) + " |", "|" + " --- |" * len(header)]
        lines += ["| " + " | ".join(row) + " |" for row in rows]
        return "\n".join(lines) + "\n\n"

    def docstring(
        self,
        obj: griffe.Object,
        title_args: str = "Arguments",
        lead: str = "",
        properties: Optional[List[List[str]]] = None,
    ) -> str:
        """Renders the docstring sections of `obj`, typing parameters from its signature.

        `lead` is placed after the description, before the first other section. `properties` rows
        join the attributes table of a class, or form one when its docstring has none.
        """
        properties = properties or []
        if not obj.docstring:
            return self.attributes_table(properties)
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
            if section.kind is not kind.text and lead:
                md, lead = md + lead, ""
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
                if title_args == "Attributes":
                    rows, properties = rows + properties, []
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
                md += self.attributes_table(rows + properties)
                properties = []
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
        md += lead
        if isinstance(obj, griffe.Class) and not any(s.kind is kind.attributes for s in sections):
            return md + self.inline_attributes(obj, annotations, values, properties)
        return md + self.attributes_table(properties)

    def attributes_table(self, rows: List[List[str]]) -> str:
        if not rows:
            return ""
        return "**Attributes**\n\n" + self.table(rows, ["Name", "Type", "Description"])

    def inline_attributes(
        self,
        cls: griffe.Class,
        annotations: Dict[str, str],
        values: Dict[str, str],
        properties: List[List[str]],
    ) -> str:
        """Renders attributes or enum members documented by a docstring under each attribute."""
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
        if values and documented:
            rows = [[f"`{n}`", f"`{values.get(n, '')}`", self.cell(d)] for n, d in documented]
            md = "**Members**\n\n" + self.table(rows, ["Member", "Value", "Description"])
            return md + self.attributes_table(properties)
        rows = [[f"`{n}`", self.type_cell(annotations.get(n)), self.cell(d)] for n, d in documented]
        return self.attributes_table(rows + properties)

    def type_cell(self, annotation: Optional[str]) -> str:
        """Renders a type as one code element whose exported names link to their anchors."""
        if not annotation:
            return ""
        parts = []
        for token in re.split(r"([A-Za-z_]\w*)", annotation):
            if anchor(token) in self.anchors:
                parts.append(f'<a href="{self.url(anchor(token))}">{token}</a>')
            else:
                # the pipe would end the table cell
                parts.append(html.escape(token).replace("|", "&#124;"))
        return "<code>" + "".join(parts) + "</code>"

    def function(self, func: griffe.Function, name: str, level: int) -> str:
        md = f"{'#' * level} `{name}()` {{#{anchor(name)}}}\n\n"
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

    def collection(self, annotation: str) -> str:
        """Example call on a namespace or collection type, e.g. `list()`; empty for other types."""
        obj = self.exports.get(annotation)
        if not isinstance(obj, griffe.Class) or role(obj) > ROLES["Collection"]:
            return ""
        methods = [
            n for n, m in obj.members.items()
            if not n.startswith("_") and isinstance(target(m), griffe.Function)
        ]
        if "list" in methods:
            return "list()"
        return f"{methods[0]}()" if methods else ""

    def klass(self, cls: griffe.Class, name: str) -> str:
        md = f"{'#' * self.level} `{name}` {{#{anchor(name)}}}\n\n"
        bases = [str(b) for b in cls.bases if str(b).split("[")[0] in self.exports]
        if bases:
            links = [f"[`{b}`]({self.href(anchor(b.split('[')[0]))})" for b in bases]
            md += "Subclass of " + ", ".join(links) + ".\n\n"
        variable = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
        collections, properties = [], []
        for member_name, member in cls.members.items():
            member = target(member)
            if member_name.startswith("_"):
                continue
            if isinstance(member, griffe.Attribute) and "property" in member.labels:
                summary = member.docstring.value.splitlines()[0] if member.docstring else ""
                returns = clean_type(member.annotation)
                collection = self.collection(returns)
                if collection:
                    collections.append(
                        [
                            f"`{variable}.{member_name}`",
                            self.type_cell(returns),
                            f"`{variable}.{member_name}.{collection}`",
                            self.cell(summary),
                        ]
                    )
                else:
                    row = [f"`{member_name}`", self.type_cell(returns), self.cell(summary)]
                    properties.append(row)
        lead = ""
        if collections:
            header = ["Name", "Type", "Example", "Description"]
            title = "[Collections](getting-started.md#collections-and-attributes)"
            lead = f"**{title}**\n\n" + self.table(collections, header)
        md += self.docstring(cls, title_args="Attributes", lead=lead, properties=properties)
        for member_name, member in cls.members.items():
            member = target(member)
            if member_name.startswith("_") or not isinstance(member, griffe.Function):
                continue
            md += self.function(member, f"{name}.{member_name}", self.level + 1)
        return md

    def attribute(self, attr: griffe.Attribute, name: str) -> str:
        md = f"{'#' * self.level} `{name}` {{#{anchor(name)}}}\n\n"
        if attr.value is not None:
            md += f"```python\n{name} = {attr.value}\n```\n\n"
        return md + (self.prose(attr.docstring.value) + "\n\n" if attr.docstring else "")

    def render(self, obj: griffe.Object, name: str) -> str:
        if isinstance(obj, griffe.Function):
            return self.function(obj, name, self.level)
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
        return "Conventions"
    return relative.rsplit(".", 1)[-1].replace("_", " ").capitalize()


def base_names(obj: griffe.Object) -> List[str]:
    return [str(b).split("[")[0] for b in getattr(obj, "bases", [])]


def role(obj: griffe.Object) -> int:
    """Rank of an object within its section, see `ROLES`; functions first, constants last."""
    if isinstance(obj, griffe.Function):
        return -1
    if not isinstance(obj, griffe.Class):
        return 6
    return min((ROLES[b] for b in base_names(obj) if b in ROLES), default=3)


def class_tree(names: List[str], exports: Dict[str, griffe.Object]) -> List[str]:
    """Orders classes depth first from the roots, each followed by its own subclasses."""
    ordered: List[str] = []

    def visit(name: str) -> None:
        ordered.append(name)
        for child in names:
            if child not in ordered and name in base_names(exports[child]):
                visit(child)

    roots = [name for name in names if not set(base_names(exports[name])) & set(names)]
    # the root with the most subclasses, the base exception, comes first
    for name in sorted(roots, key=lambda r: -sum(r in base_names(exports[n]) for n in names)):
        visit(name)
    return ordered


def order_section(label: str, names: List[str], exports: Dict[str, griffe.Object]) -> List[str]:
    if label == "Errors":
        return class_tree(names, exports)
    return sorted(names, key=lambda name: (role(exports[name]), name.lower()))


def page_slug(label: str) -> str:
    for slug, (_, labels) in PAGES.items():
        if label in labels:
            return slug
    return label.lower().replace(" ", "-")


def frontmatter(title: str, description: str) -> str:
    md = "---\n"
    md += f"title: {title}\n"
    md += f"description: {description}\n"
    md += "keywords: [dlthub, runtime, python sdk, dlthub_sdk, api]\n"
    return md + "---\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output_dir", type=pathlib.Path)
    args = parser.parse_args()

    package = griffe.load(
        PACKAGE, docstring_parser="google", resolve_aliases=True, resolve_external=False
    )
    exports = {str(name): target(package[str(name)]) for name in package.exports or []}

    sections: Dict[str, List[str]] = {}
    for name, obj in exports.items():
        sections.setdefault(section_label(obj.module.path), []).append(name)
    sections = {label: order_section(label, names, exports) for label, names in sections.items()}
    order = [label for _, labels in PAGES.values() for label in labels]
    ordered = sorted(sections, key=lambda s: (order.index(s) if s in order else len(order), s))
    pages: Dict[str, List[str]] = {"getting-started": []}
    for label in ordered:
        pages.setdefault(page_slug(label), []).append(label)
    renderer = Renderer(
        exports, {name: page_slug(label) for label in ordered for name in sections[label]}
    )

    client_version = version(DISTRIBUTION)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    # pages of an earlier client version would otherwise stay behind
    for stale in args.output_dir.glob("*.md"):
        stale.unlink()
    for slug, labels in pages.items():
        renderer.page = slug
        title = PAGES[slug][0] if slug in PAGES else labels[0]
        preposition = "with" if slug == "getting-started" else "in"
        md = frontmatter(title, f"{title} {preposition} the dltHub Runtime Python SDK ({PACKAGE})")
        md += f"# {title}\n\n"
        md += f"<!-- generated from {DISTRIBUTION} {client_version}, do not edit -->\n\n"
        if slug == "getting-started":
            md += INTRO.format(package=PACKAGE, distribution=DISTRIBUTION) + "\n\n"
            md += f"This reference is generated from `{DISTRIBUTION}` {client_version}.\n\n"
        else:
            md += START_HERE + "\n\n"
        # a single section named like the page would repeat its title
        renderer.level = 2 if labels == [title] else 3
        for label in labels:
            if renderer.level == 3:
                md += f"## {label}\n\n"
            if label in SECTION_INTROS:
                md += SECTION_INTROS[label] + "\n\n"
            for name in sections[label]:
                md += renderer.render(exports[name], name)
        (args.output_dir / f"{slug}.md").write_text(md.rstrip() + "\n", encoding="utf-8")

    sidebar = {
        "type": "category",
        "label": TITLE,
        "link": {
            "type": "generated-index",
            "title": TITLE,
            "description": f"Reference of the dltHub Runtime Python SDK ({PACKAGE})",
            "slug": f"/{DOC_ID}",
        },
        "items": [f"{DOC_ID}/{slug}" for slug in pages],
    }
    (args.output_dir / "sidebar.json").write_text(json.dumps(sidebar, indent=2) + "\n")
    print(
        f"Generated the SDK reference for {DISTRIBUTION} {client_version}"
        f" in {len(pages)} pages in {args.output_dir}"
    )


if __name__ == "__main__":
    main()
