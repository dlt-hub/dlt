"""Subprocess worker for isolated manifest generation.

Run via ``python -m dlt._workspace.deployment._manifest_worker <module> [--no-use-all]``.
Prints JSON to stdout: ``{"manifest": ..., "warnings": [...]}`` on success,
``{"error": "...", "error_type": "..."}`` on failure.
"""

import sys


def main() -> int:
    import argparse

    from dlt.common import json

    parser = argparse.ArgumentParser()
    parser.add_argument("module")
    parser.add_argument("--no-use-all", action="store_true")
    args = parser.parse_args()

    try:
        from dlt._workspace.deployment.manifest import manifest_from_module

        manifest, warnings = manifest_from_module(
            args.module, use_all=not args.no_use_all, isolated=False
        )
        sys.stdout.write(json.typed_dumps({"manifest": manifest, "warnings": warnings}))
    except Exception as ex:
        sys.stdout.write(json.typed_dumps({"error": str(ex), "error_type": type(ex).__qualname__}))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
