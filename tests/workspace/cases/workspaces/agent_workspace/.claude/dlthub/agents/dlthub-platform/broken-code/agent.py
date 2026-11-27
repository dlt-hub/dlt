"""Agent code that fails on import, so a test can prove nothing imports it too early."""

raise RuntimeError("agent.py of broken-code was imported")
