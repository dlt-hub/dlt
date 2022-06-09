import argparse
from typing import Callable

from dlt.common.runners import TRunArgs, add_pool_cli_arguments


def main() -> None:
    parser = argparse.ArgumentParser(description="Runs various DLT modules")
    subparsers = parser.add_subparsers(dest="command")
    unpack = subparsers.add_parser("unpack", help="Runs unpacker")
    add_pool_cli_arguments(unpack)
    load = subparsers.add_parser("load", help="Runs loader")
    add_pool_cli_arguments(load)
    dbt = subparsers.add_parser("dbt", help="Executes dbt package")
    add_pool_cli_arguments(dbt)

    args = parser.parse_args()
    run_f: Callable[[TRunArgs], None] = None

    if args.command == "unpack":
        from dlt.unpacker.unpacker import run_main as unpacker_run
        run_f = unpacker_run
    elif args.command == "load":
        from dlt.loaders.loader import run_main as loader_run
        run_f = loader_run
    elif args.command == "dbt":
        from dlt.dbt_runner.runner import run_main as dbt_run
        run_f = dbt_run
    else:
        raise ValueError(args.command)

    run_args = TRunArgs(args.single_run, args.wait_runs)
    run_f(run_args)

if __name__ == "__main__":
    main()
