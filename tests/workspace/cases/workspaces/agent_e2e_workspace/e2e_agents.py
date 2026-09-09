"""Agent jobs the end-to-end tests run on real loops and real models."""

import asyncio
from typing import List

from dlt.common.typing import Annotated, Doc

from dlt.hub.run import TAgentOutput, TJobRunContext, agent

self_report_md = agent("e2e:self-report", name="self_report_md")


class SelfReport(TAgentOutput):
    local_tools: Annotated[
        List[str],
        Doc("Name of every tool in your tool list that is not an MCP tool, copied verbatim."),
    ]
    mcp_tools: Annotated[
        List[str], Doc("Name of every MCP tool in your tool list, copied verbatim.")
    ]
    skills: Annotated[List[str], Doc("Name of every skill available to you.")]
    markers: Annotated[List[str], Doc("Every MARKER-<WORDS> token you were given, copied exactly.")]
    workspace_name: Annotated[str, Doc("The `name` field `get_workspace_info` returned.")]


@agent(
    access={"local": ["read"]},
    tools=["workspace"],
    skills=["e2e:visible-skill"],
    rules=["e2e:visible-rule"],
    model="haiku",
    limits={"max_turns": 12},
)
def self_report_py(run_context: TJobRunContext = None) -> SelfReport:
    """Reports the tools, skills and rule markers it was given.

    You are a self-report agent. Describe your own setup in the structured output and do nothing
    else. This prompt carries the token MARKER-PROMPT-CONTROL.

    Rules for this task:
    - Use no tool except the MCP tool `get_workspace_info` and the skill loader. Never read,
      search or list files.
    - Copy names and tokens exactly as you see them. Never invent, guess or rename anything.

    Fill the output like this:
    1. `local_tools`: the name of every tool in your tool list that is not an MCP tool, such as
       file, shell and web tools.
    2. `mcp_tools`: the name of every MCP tool in your tool list.
    3. `skills`: the name of every skill available to you. Load each skill, or read it where its
       text is already in your instructions, and copy the `MARKER-` token it carries into
       `markers`.
    4. `markers`: every token of the form `MARKER-<WORDS>` you can see in your instructions,
       rules, project notes and skills.
    5. `workspace_name`: call `get_workspace_info` and copy the `name` field of its result.
    6. `status`: `succeeded`. `summary`: one sentence on what you found.
    """
    return asyncio.run(run_context["ai_loop"].run())  # type: ignore[no-any-return]
