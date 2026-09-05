"""Agent loop that records what the launcher gave it instead of calling a model.

Registered on import so a launcher started with `python -m` resolves it too: the launcher
imports the entry point module, and the job modules import this one.
"""

import os
from typing import Any, ClassVar, Dict, Optional

from dlt.common.configuration import plugins
from dlt.common.configuration.container import Container
from dlt.common.configuration.plugins import PluginContext

from dlt._workspace._known_env import WORKSPACE__PROFILE
from dlt._workspace.deployment.agent.loop import AgentLoop
from dlt._workspace.deployment.agent.typing import TAgentSpec
from dlt._workspace.deployment.agent.typing import TAgentLimits

MOCK_LOOP = "mock-loop"
PLUGIN_NAME = "mock-agent-loop"


class MockLoop(AgentLoop):
    LOOP_TYPE: ClassVar[str] = MOCK_LOOP
    DEFAULT_MODEL: ClassVar[str] = "mock-model"
    DEFAULT_MAX_TURNS: ClassVar[Optional[int]] = 5
    DEFAULT_MAX_TOKENS: ClassVar[Optional[int]] = 1000
    outcome: ClassVar[Dict[str, Any]] = {"status": "succeeded", "summary": "mock run"}

    def __init__(self, settings: Any) -> None:
        super().__init__(settings)
        self._native: Optional[str] = None

    @property
    def native(self) -> Any:
        return self._native

    def init(self, agent_spec: TAgentSpec) -> None:
        self.spec = agent_spec
        self._system_prompt = agent_spec["system_prompt"]

    async def run(
        self,
        inputs: Optional[Dict[str, Any]] = None,
        run_args: Optional[Dict[str, Any]] = None,
        model: Optional[str] = None,
        limits: Optional[TAgentLimits] = None,
        instructions: Optional[str] = None,
    ) -> Dict[str, Any]:
        inputs = inputs or {}
        self.resolve_run(model, limits, instructions)
        self._native = f"mock-native:{self.model_id()}"
        system_prompt = self.render_system_prompt(inputs)
        # three pretend turns, counted through the base so the token limit applies here too
        for _ in range(3):
            self.count_tokens(40, 15)
        self._trace = self._base_trace(inputs)
        self._trace["turn_count"] = 3
        self._trace["total_tokens"] = self.tokens_used
        return {
            **self.outcome,
            "ran": {
                "agent": self.spec["name"],
                "model": self.model_id(),
                "profile": os.environ.get(WORKSPACE__PROFILE),
                "max_turns": self.settings["max_turns"],
                "verbosity": self.settings["verbosity"],
                "instructions": self.settings["instructions"],
                "user_turn": self.user_turn,
                "system_prompt": system_prompt,
                "run_args": run_args,
                "run_context": inputs.get("run_context"),
            },
        }


class MockLoopPlugin:
    @plugins.hookimpl(specname="plug_agent_loop")
    def plug_agent_loop(self, loop_type: str) -> Any:
        return MockLoop if loop_type == MOCK_LOOP else None


def register() -> None:
    """Adds the loop to the active plugin manager.

    Replaces an earlier registration: each workspace copy imports this module afresh, and a
    plugin left over from the previous copy would answer with that copy's class.
    """
    manager = Container()[PluginContext].manager
    existing = manager.get_plugin(PLUGIN_NAME)
    if existing is not None:
        manager.unregister(existing, name=PLUGIN_NAME)
    manager.register(MockLoopPlugin(), name=PLUGIN_NAME)


register()
