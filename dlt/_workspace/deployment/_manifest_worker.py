"""Subprocess worker for isolated manifest generation.

Run via `python -m dlt._workspace.deployment._manifest_worker <module> [--no-use-all]
[--environment JSON]`. Prints JSON to stdout: `{"manifest": ..., "warnings": [...]}` on
success, `{"error": "...", "error_type": "..."}` on failure.
"""

import os
import sys


def main() -> int:
    import argparse

    from dlt._workspace._known_env import WORKSPACE__PROFILE
    from dlt._workspace.workspace_definition import default_profile

    parser = argparse.ArgumentParser()
    parser.add_argument("module")
    parser.add_argument("--no-use-all", action="store_true")
    parser.add_argument(
        "--environment",
        default=None,
        help="the environment the deployment goes to, as JSON (TEnvironmentDefinition)",
    )
    args = parser.parse_args()

    from dlt.common import json

    environment = json.loads(args.environment) if args.environment else None
    # the run context reads WORKSPACE__PROFILE when dlt first creates it, before any import below
    if environment:
        os.environ[WORKSPACE__PROFILE] = default_profile(environment, "write")

    try:
        from dlt._workspace.deployment.manifest import manifest_from_module

        manifest, warnings = manifest_from_module(
            args.module, environment, use_all=not args.no_use_all, isolated=False
        )
        sys.stdout.write(json.typed_dumps({"manifest": manifest, "warnings": warnings}))
    except Exception as ex:
        sys.stdout.write(json.typed_dumps({"error": str(ex), "error_type": type(ex).__qualname__}))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
