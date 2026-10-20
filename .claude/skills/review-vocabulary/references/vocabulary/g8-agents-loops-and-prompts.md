# G8 — Agents, loops and prompts

Three nouns carry the feature, and they nest: a definition is written once, a job adds how it
operates, a run is one execution.

**Included**

| Concept | Write |
|---|---|
| what an `AGENT.md` or a decorated function declares: system prompt, inputs, output, tools, skills, rules, access | **agent definition** (`TAgentSpec` in code, `TAgentDefinition` for the block the manifest carries; pydantic-ai's `AgentSpec`) |
| the file it is read from | **`AGENT.md`**, and **agent file** for the path |
| the name a definition is referred to by | **agent definition reference**: `<toolkit>:<agent>`, a workspace path, or `<module>:<function>` for a function with no `AGENT.md` behind it |
| the definition plus the runtime settings that say how it operates: model, limits, trigger, instructions, loop, identity | **agent job** (`run.agent(...)`; what pydantic-ai and claude-agent-sdk call an Agent, and what the web UI lists as one) |
| one execution of an agent job: inputs in, output and trace out, settings overridable for that run | **agent run** (`Agent.run()` in the frameworks) |
| what the model returns: `status`, `summary` and the declared fields | **agent output** |
| the envelope the launcher delivers | **job result** (G7 owns it; an agent job's is `TAgentJobResult`) |
| the binding to one agent framework | **loop**, or **agent loop** |
| the text the model gets as its role and task | **system prompt** |
| the first message of the run | **user turn** |
| what a person tells this run to do | **instructions** |
| one model request | **turn** |
| the record of what the loop ran and did | **agent trace** |
| `{{ name }}` in a body | **placeholder** |

**Excluded**

| Never | Because |
|---|---|
| agent spec (in prose) | **agent definition**; `TAgentSpec` keeps its name as a type |
| agent reference, agent ref (in prose) | **agent definition reference**; the field `agent_ref` keeps its name |
| agent (bare) where the sentence needs one of the three | say which: **agent definition**, **agent job** or **agent run** |
| harness (for the Claude Code CLI) | the **dltHub AI Harness** owns that word; write **the Claude Code CLI** |
| adapter (for a loop) | dlt's adapters are `bigquery_adapter` and friends; write **loop** |
| prompt (bare) | say which one: **system prompt** or **user turn** |
| spec (bare, for an agent) | bare `spec` is the configspec; write **agent definition** |
| trace (bare, for an agent) | bare `trace` is the pipeline trace; write **agent trace** |
| manifest (for `AGENT.md`) | **agent file**; the manifest is the deployment manifest (G7) |
| framework (as a loop's name) | name it: **pydantic-ai**, **claude-agent-sdk** |

**Rulings**

- **Bare `agent` is the actor.** "The agent reads the logs", "the agent returns `aborted`": the
  model acting during a run. That stays. The moment a sentence is about the file, the `run.agent`
  product or one execution, it names the definition, the job or the run.
- **`Agent` (capitalised) is the agent job.** That is how the web UI labels it and how pydantic-ai
  and claude-agent-sdk name the same thing. Legal in UI copy and when talking about a framework;
  in dlt prose write **agent job**.
- **An agent run is a job run.** Use **agent run** when the sentence is about what the agent
  received and produced; **job run** when it is about scheduling, status or logs, as for any job.
- **`instructions` means two opposite things across the boundary.** In dlt it is the user turn. In
  pydantic-ai, `AgentSpec.instructions` is the system prompt. Qualify every mention of the
  framework field: "pydantic-ai's `instructions` field (its system prompt)".
- **`turn` is one model request.** The **user turn** is the first message. A turn counter counts
  requests. Do not let one word carry both without the qualifier.
- **`loop` is the agent loop.** In a file that also has Python loops, write **agent loop** once,
  then `loop`.
- **Model names and aliases are technical nouns** (Rule 1.8): `sonnet`, `opus`, `claude-sonnet-5`.
