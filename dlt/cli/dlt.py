import yaml
import os
import argparse
from typing import Callable

from dlt.common import json
from dlt.cli import TRunnerArgs
from dlt.common.schema import Schema
from dlt.common.typing import DictStrAny
from dlt.common.utils import str2bool

from dlt.pipeline import Pipeline, PostgresPipelineCredentials


def add_pool_cli_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--is-single-run", action="store_true", help="exit when all pending items are processed")
    parser.add_argument("--wait-runs", type=int, nargs='?', const=True, default=1, help="maximum idle runs to wait for incoming data")


# def str2bool_a(v: str) -> bool:
#     try:
#         return str2bool(v)
#     except ValueError:
#         raise argparse.ArgumentTypeError('Boolean value expected.')


def main() -> None:
    parser = argparse.ArgumentParser(description="Runs various DLT modules", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(dest="command")
    normalize = subparsers.add_parser("normalize", help="Runs normalize")
    add_pool_cli_arguments(normalize)
    load = subparsers.add_parser("load", help="Runs loader")
    add_pool_cli_arguments(load)
    dbt = subparsers.add_parser("dbt", help="Executes dbt package")
    add_pool_cli_arguments(dbt)
    schema = subparsers.add_parser("schema", help="Shows, converts and upgrades schemas")
    schema.add_argument("file", help="Schema file name, in yaml or json format, will autodetect based on extension")
    schema.add_argument("--format", choices=["json", "yaml"], default="yaml", help="Display schema in this format")
    schema.add_argument("--remove-defaults", action="store_true", help="Does not show default hint values")
    pipeline = subparsers.add_parser("pipeline", help="Operations on the pipelines")
    pipeline.add_argument("name", help="Pipeline name")
    pipeline.add_argument("workdir", help="Pipeline working directory")
    pipeline.add_argument("operation", choices=["failed_loads"], default="failed_loads", help="Show failed loads for a pipeline")

    # TODO: consider using fire: https://github.com/google/python-fire
    # TODO: this also looks better https://click.palletsprojects.com/en/8.1.x/complex/#complex-guide
    args = parser.parse_args()
    run_f: Callable[[TRunnerArgs], None] = None

    if args.command == "normalize":
        from dlt.normalize.normalize import run_main as normalize_run
        run_f = normalize_run
    elif args.command == "load":
        from dlt.load.load import run_main as loader_run
        run_f = loader_run
    elif args.command == "dbt":
        from dlt.dbt_runner.runner import run_main as dbt_run
        run_f = dbt_run
    elif args.command == "schema":
        with open(args.file, "r", encoding="utf-8") as f:
            if os.path.splitext(args.file)[1][1:] == "json":
                schema_dict: DictStrAny = json.load(f)
            else:
                schema_dict = yaml.safe_load(f)
        s = Schema.from_dict(schema_dict)
        if args.format == "json":
            schema_str = json.dumps(s.to_dict(remove_defaults=args.remove_defaults), indent=2)
        else:
            schema_str = s.to_pretty_yaml(remove_defaults=args.remove_defaults)
        print(schema_str)
        exit(0)
    elif args.command == "pipeline":
        p = Pipeline(args.name)
        p.restore_pipeline(PostgresPipelineCredentials("dummy"), args.workdir)
        completed_loads = p.list_completed_loads()
        for load_id in completed_loads:
            print(f"Checking failed jobs in {load_id}")
            for job, failed_message in p.list_failed_jobs(load_id):
                print(f"JOB: {job}\nMSG: {failed_message}")
        exit(0)
    else:
        parser.print_help()
        exit(-1)

    run_args = TRunnerArgs(args.is_single_run, args.wait_runs)
    run_f(run_args)

if __name__ == "__main__":
    main()
