print("this module checks if certain modules are not imported by default")
import sys, dlt


# plugin system should not be initialized on dlt import, workspace should not be imported
excluded_imports = ["dlt._workspace._workspace_context", "pluggy", "git"]

for excluded  in excluded_imports:
    assert excluded not in sys.modules


# will instantiate pluggy
dlt.current.run_context()

assert "pluggy" in sys.modules
assert "dlt._workspace._workspace_context" not in sys.modules