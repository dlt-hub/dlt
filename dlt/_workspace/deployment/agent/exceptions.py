from typing import List, Optional, Sequence

from dlt._workspace.deployment.exceptions import DeploymentException


class AgentException(DeploymentException):
    pass


class UnknownAgentLoop(AgentException, KeyError):
    def __init__(self, loop_type: str, known: Sequence[str]) -> None:
        self.loop_type = loop_type
        super().__init__(
            f"No plugin implements agent loop {loop_type!r}. dlt ships {', '.join(known)};"
            " a third-party loop must register a `plug_agent_loop` hook."
        )


class InvalidAgentSpec(AgentException, ValueError):
    def __init__(self, path: str, reason: str) -> None:
        self.path = path
        super().__init__(f"Invalid agent file {path!r}: {reason}")


class AgentComponentNotFound(AgentException, FileNotFoundError):
    def __init__(
        self,
        ref: str,
        kind: str,
        searched: List[str],
        toolkit: Optional[str] = None,
        installed: bool = False,
    ) -> None:
        """A skill, rule or agent the manifest names is not in the workspace.

        Args:
            ref (str): The reference as written.
            kind (str): `agent`, `skill` or `rule`.
            searched (List[str]): Files that would have answered the reference.
            toolkit (Optional[str]): Toolkit the ref names. `None` when the ref is a path,
                empty when it names no toolkit at all.
            installed (bool): Whether that toolkit is installed in this workspace.
        """
        self.ref = ref
        self.kind = kind
        self.searched = searched
        self.toolkit = toolkit
        lines = [f"Cannot resolve {kind} {ref!r}."]
        if toolkit is None:
            lines.append(f"Make sure {searched[0]} is present." if searched else "")
        elif not toolkit:
            lines.append(
                f"The reference names no toolkit. Write it as `<toolkit>:{ref}` and install that"
                " toolkit, or point at a file inside the workspace."
            )
            lines.append("`dlthub ai toolkit list` shows what is available.")
        elif installed:
            lines.append(
                f"Toolkit {toolkit!r} is installed but has no {kind} {ref.rpartition(':')[2]!r}."
                f" Update it with `dlthub ai toolkit install {toolkit} --overwrite`,"
                f" or see what it carries with `dlthub ai toolkit info {toolkit}`."
            )
        else:
            lines.append(
                f"Toolkit {toolkit!r} is not installed in this workspace."
                f" Install it with `dlthub ai toolkit install {toolkit}`."
            )
            lines.append("`dlthub ai toolkit list` shows what is available.")
        if toolkit is not None and searched:
            lines.append(f"It must leave {searched[0]} in place.")
        super().__init__("\n".join(line for line in lines if line))


class UnsupportedAgentModel(AgentException, ValueError):
    def __init__(self, loop_type: str, model: str, reason: str) -> None:
        self.loop_type = loop_type
        self.model = model
        super().__init__(f"Loop {loop_type!r} cannot run model {model!r}: {reason}")


class AgentTraceNotAvailable(AgentException):
    def __init__(self, loop_type: str) -> None:
        super().__init__(f"Loop {loop_type!r} has no trace: it has not completed a run.")


class AgentRunFailed(AgentException):
    def __init__(self, loop_type: str, agent_ref: str, reason: str) -> None:
        self.loop_type = loop_type
        self.agent_ref = agent_ref
        super().__init__(f"Agent {agent_ref!r} on {loop_type!r} failed: {reason}")


class AgentTokenLimitExceeded(AgentRunFailed):
    def __init__(self, loop_type: str, agent_ref: str, used: int, limit: int) -> None:
        self.used = used
        self.limit = limit
        super().__init__(
            loop_type,
            agent_ref,
            f"used {used:,} tokens, over its limit of {limit:,}. Raise `limits.max_tokens` on"
            " the agent or `agent.max_tokens` in configuration, or narrow the task",
        )
